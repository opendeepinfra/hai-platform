#!/usr/bin/env bash
#
# create.sh —— 把 Hai Platform 部署到 103 单节点 K8s 集群（带 V100）。
#
# 跑在**你的开发机**上，通过 SSH 驱动 103；Terraform 本身运行在 103 上。
#
# 部署链路：镜像导入 → MetalLB(LAN VIP) → 生成 config.sh + hai-up up + 加固
#           → 验收 → π 任务测试 → **GPU 任务测试**
#
# 用法：
#   ./create.sh --preflight-only   # 只读前置检查（含与 VM 平台的隔离性检查）
#   ./create.sh --plan             # 只出 plan
#   ./create.sh                    # 完整部署（较慢：镜像导入 + hai-up + 两个任务测试）
#   ./create.sh -y                 # 跳过二次确认
#
# 环境变量：HOST（默认 fireflyer@192.168.100.103）、TF_DIR（默认 /opt/terraform/hai-platform-single-node）

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
TF_DIR="${TF_DIR:-/opt/terraform/hai-platform-single-node}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
SCP=(scp -o BatchMode=yes -o ConnectTimeout=10)
HERE="$(cd "$(dirname "$0")" && pwd)"
MODE="apply"
AUTO=0

for arg in "$@"; do
  case "$arg" in
    --preflight-only) MODE="preflight" ;;
    --plan)           MODE="plan" ;;
    -y|--auto-approve) AUTO=1 ;;
    -h|--help) sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数：${arg}（--preflight-only | --plan | -y）" >&2; exit 2 ;;
  esac
done

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

log "0) 检查 SSH：${HOST}"
"${SSH[@]}" 'hostname; uname -sr' || die "无法连接 ${HOST}"

log "1) 检查 103 上的 terraform 与单节点集群 kubeconfig"
"${SSH[@]}" 'terraform version | head -n1'
"${SSH[@]}" 'sudo test -f /root/.kube/hai-single.conf && echo "kubeconfig OK: /root/.kube/hai-single.conf" || echo "缺少 kubeconfig，请先部署 terraform-k8s-single-node"'

log "2) 同步模块到 ${TF_DIR}"
"${SSH[@]}" "sudo mkdir -p '${TF_DIR}' && sudo chown \$(id -u):\$(id -g) '${TF_DIR}'"
tar czf - -C "$HERE" main.tf files | "${SSH[@]}" "tar xzf - -C '${TF_DIR}'"
"${SSH[@]}" "chmod +x '${TF_DIR}'/files/*.sh"
if [ -f "$HERE/terraform.tfvars" ]; then
  "${SCP[@]}" "$HERE/terraform.tfvars" "${HOST}:${TF_DIR}/terraform.tfvars"
  echo "已同步 terraform.tfvars"
fi

log "3) terraform init"
"${SSH[@]}" "cd '${TF_DIR}' && terraform init -input=false"

case "$MODE" in
  preflight)
    log "4) 干跑：只执行只读前置检查"
    "${SSH[@]}" "cd '${TF_DIR}' && terraform apply -target=null_resource.preflight -auto-approve"
    log "干跑完成：未对系统做任何修改"
    ;;
  plan)
    log "4) terraform plan"
    "${SSH[@]}" "cd '${TF_DIR}' && terraform plan"
    ;;
  apply)
    log "4) terraform apply"
    cat <<'WARN'

    ⚠️  完整部署会：
        * 把平台镜像 docker save → 导入 k8s containerd（约 5GB，几分钟）；
        * 安装 MetalLB 并占用 192.168.100.150 作为平台 VIP；
        * 在 /nfs-shared/hai-single/hai-platform 下创建**独立**数据目录并执行 hai-up up；
        * 在集群里跑 π 任务与 GPU 任务（真实占用节点几分钟）。
        不影响 VM 平台（/nfs-shared/hai-platform 与 /root/.kube/config 都不动）。

WARN
    if [ "$AUTO" != "1" ] && [ -t 0 ]; then
      read -r -p "继续？(y/N) " reply
      [[ "$reply" =~ ^[Yy]$ ]] || die "已取消"
    fi
    "${SSH[@]}" "cd '${TF_DIR}' && terraform apply -auto-approve"
    ;;
esac

log "完成"
"${SSH[@]}" "cd '${TF_DIR}' && terraform output 2>/dev/null || true"
