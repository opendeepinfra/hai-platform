#!/usr/bin/env bash
#
# destroy.sh —— 拆掉本模块在 103 上创建的单节点集群。
#
# 会发生什么（按 Terraform 的逆序）：
#   * 删除 NVIDIA device plugin DaemonSet 与 GPU 冒烟 Pod；
#   * kubeadm reset + 停 kubelet + 清 /etc/kubernetes 运行时文件与 bridge CNI 配置；
#   * 把 containerd 的 GPU drop-in 还原为 03 步骤的备份并重启 containerd。
#
# 不会发生什么：
#   * 不卸载 NVIDIA 驱动 / nvidia-container-toolkit；
#   * 不删 Docker 容器（RustFS 保持运行），不动 DOCKER* iptables 链；
#   * 不动 /root/.kube/config（仍指向 VM 集群），也不动 4 台 Multipass VM 与现有平台。
#
# 用法：
#   ./destroy.sh        # 二次确认后 terraform destroy
#   ./destroy.sh -y     # 跳过确认

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
TF_DIR="${TF_DIR:-/opt/terraform/k8s-single-node}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
HERE="$(cd "$(dirname "$0")" && pwd)"
AUTO=0
for arg in "$@"; do
  case "$arg" in
    -y|--auto-approve) AUTO=1 ;;
    -h|--help) sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数：${arg}（-y）" >&2; exit 2 ;;
  esac
done

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

log "0) 检查 SSH：$HOST"
"${SSH[@]}" 'echo OK; hostname' || { echo "无法连接 $HOST" >&2; exit 1; }

log "1) 同步模块到 $TF_DIR"
"${SSH[@]}" "sudo mkdir -p '$TF_DIR' && sudo chown \$(id -u):\$(id -g) '$TF_DIR'"
tar czf - -C "$HERE" main.tf files | "${SSH[@]}" "tar xzf - -C '$TF_DIR'"
"${SSH[@]}" "chmod +x '$TF_DIR'/files/*.sh"
if [ -f "$HERE/terraform.tfvars" ]; then
  scp -o BatchMode=yes "$HERE/terraform.tfvars" "$HOST:$TF_DIR/terraform.tfvars"
fi
"${SSH[@]}" "cd '$TF_DIR' && terraform init -input=false >/dev/null"

log "2) terraform destroy"
cat <<'WARN'

    ⚠️  将执行 kubeadm reset 并停止 kubelet。Docker 容器与 NVIDIA 驱动不受影响。

WARN
if [ "$AUTO" != "1" ] && [ -t 0 ]; then
  read -r -p "继续？(y/N) " reply
  [[ "$reply" =~ ^[Yy]$ ]] || { echo "已取消"; exit 1; }
fi
"${SSH[@]}" "cd '$TF_DIR' && terraform destroy -auto-approve"

log "完成。"
