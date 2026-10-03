#!/usr/bin/env bash
#
# create.sh —— 在 host 103 上把本机初始化成 **单机 K8s 全节点 + GPU**（路线 A）。
#
# 本脚本跑在 **你的开发机** 上，通过 SSH 驱动 103：
#   1. 检查 SSH / terraform；
#   2. 把 main.tf 与 files/ 同步到 103 的 /opt/terraform/k8s-single-node；
#   3. 在 103 上执行 terraform init + apply。
#
# Terraform 与所有 local-exec 都运行在 103 本机（沿用 terraform-k8s-ha / terraform-hai-platform 的约定）。
#
# 用法：
#   ./create.sh --preflight-only   # 干跑：只跑只读前置检查（不修改系统）
#   ./create.sh --plan             # 只 terraform plan，不落地
#   ./create.sh                    # 完整初始化（会自动 approve，见下方提示）
#   ./create.sh -y                 # 同上，显式表示无需二次确认
#
# 可用环境变量覆盖：
#   HOST    默认 fireflyer@192.168.100.103
#   TF_DIR  默认 /opt/terraform/k8s-single-node

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
TF_DIR="${TF_DIR:-/opt/terraform/k8s-single-node}"
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
    -h|--help)
      sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "未知参数：${arg}（--preflight-only | --plan | -y）" >&2; exit 2 ;;
  esac
done

log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

log "0) 检查 SSH：$HOST"
"${SSH[@]}" 'echo OK; hostname; uname -sr; nvidia-smi -L 2>/dev/null | head -1' \
  || die "无法通过 SSH 连接 $HOST"

log "1) 检查 103 上的 terraform"
if ! "${SSH[@]}" 'command -v terraform >/dev/null 2>&1'; then
  die "103 上没有 terraform（本环境通过 /opt/terraform/plugins 做离线 provider mirror，请先安装 terraform）"
fi
"${SSH[@]}" 'terraform version | head -n1; cat ~/.terraformrc 2>/dev/null | head -5'

log "2) 同步模块到 $TF_DIR"
"${SSH[@]}" "sudo mkdir -p '$TF_DIR' && sudo chown \$(id -u):\$(id -g) '$TF_DIR'"
tar czf - -C "$HERE" main.tf files | "${SSH[@]}" "tar xzf - -C '$TF_DIR'"
"${SSH[@]}" "chmod +x '$TF_DIR'/files/*.sh && ls -1 '$TF_DIR' '$TF_DIR/files'"
if [ -f "$HERE/terraform.tfvars" ]; then
  "${SCP[@]}" "$HERE/terraform.tfvars" "$HOST:$TF_DIR/terraform.tfvars"
  echo "已同步 terraform.tfvars"
fi

log "3) terraform init（离线 mirror：/opt/terraform/plugins）"
"${SSH[@]}" "cd '$TF_DIR' && terraform init -input=false"

case "$MODE" in
  preflight)
    log "4) 干跑：只执行只读前置检查（null_resource.preflight）"
    "${SSH[@]}" "cd '$TF_DIR' && terraform apply -target=null_resource.preflight -auto-approve"
    log "干跑完成：未对系统做任何修改。确认无误后执行 ./create.sh"
    ;;
  plan)
    log "4) terraform plan"
    "${SSH[@]}" "cd '$TF_DIR' && terraform plan"
    ;;
  apply)
    log "4) terraform apply"
    cat <<'WARN'

    ⚠️  完整 apply 会依次：
        * 停掉 103 上指向已失联 Sealos 控制面的陈旧 kubelet，并 kubeadm reset；
        * 清理旧 cilium CNI 残留与 KUBE-*/CILIUM_* iptables 链（**不动** Docker 链）；
        * 重写 containerd 的 GPU drop-in 并重启 containerd（Docker 与 k8s 共用它，
          运行中的 RustFS 容器不会被杀，但 docker CLI 可能需要重启 dockerd）；
        * kubeadm init 单节点集群 + 装 bridge CNI + NVIDIA device plugin。

        建议先跑：./create.sh --preflight-only

WARN
    if [ "$AUTO" != "1" ] && [ -t 0 ]; then
      read -r -p "继续？(y/N) " reply
      [[ "$reply" =~ ^[Yy]$ ]] || die "已取消"
    fi
    "${SSH[@]}" "cd '$TF_DIR' && terraform apply -auto-approve"
    ;;
esac

log "完成。验证：./verify.sh"
"${SSH[@]}" "cd '$TF_DIR' && terraform output -raw next_steps 2>/dev/null || true"
