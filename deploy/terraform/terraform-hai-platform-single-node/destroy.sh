#!/usr/bin/env bash
#
# destroy.sh —— 卸载单节点集群上的 Hai Platform。
#
# 会做什么：hai-up down 删除本实例的 k8s 资源（namespace hai-platform）。
# 不会做什么：不动 VM 平台（另一个集群 + /nfs-shared/hai-platform）、
#             不动单节点集群本身（那是 terraform-k8s-single-node 的职责）、
#             默认保留共享盘数据（${SHARED_FS_ROOT}/hai-platform）。
#
# 用法：
#   ./destroy.sh            # 二次确认
#   ./destroy.sh -y         # 跳过确认
#   ./destroy.sh --purge    # 同时删除共享盘数据目录

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
TF_DIR="${TF_DIR:-/opt/terraform/hai-platform-single-node}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
HERE="$(cd "$(dirname "$0")" && pwd)"
AUTO=0
PURGE="false"

for arg in "$@"; do
  case "$arg" in
    -y|--auto-approve) AUTO=1 ;;
    --purge) PURGE="true" ;;
    -h|--help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数：${arg}（-y | --purge）" >&2; exit 2 ;;
  esac
done

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

log "同步模块到 ${TF_DIR}"
"${SSH[@]}" "sudo mkdir -p '${TF_DIR}' && sudo chown \$(id -u):\$(id -g) '${TF_DIR}'"
tar czf - -C "$HERE" main.tf files | "${SSH[@]}" "tar xzf - -C '${TF_DIR}'"
"${SSH[@]}" "chmod +x '${TF_DIR}'/files/*.sh"
[ -f "$HERE/terraform.tfvars" ] && scp -o BatchMode=yes "$HERE/terraform.tfvars" "${HOST}:${TF_DIR}/terraform.tfvars"
"${SSH[@]}" "cd '${TF_DIR}' && terraform init -input=false >/dev/null"

log "terraform destroy"
cat <<'WARN'

    ⚠️  将执行 hai-up down：删除 namespace hai-platform 内的平台资源与任务 Pod。
        共享盘数据默认保留（--purge 才删除）。VM 平台不受影响。

WARN
if [ "$AUTO" != "1" ] && [ -t 0 ]; then
  read -r -p "继续？(y/N) " reply
  [[ "$reply" =~ ^[Yy]$ ]] || { echo "已取消"; exit 1; }
fi
"${SSH[@]}" "cd '${TF_DIR}' && terraform destroy -auto-approve"

if [ "$PURGE" = "true" ]; then
  log "额外清理数据目录"
  "${SSH[@]}" "sudo env PURGE_DATA=true bash '${TF_DIR}/files/90-destroy.sh'"
fi

log "完成。若需彻底移除 MetalLB：kubectl-hai delete -f https://raw.githubusercontent.com/metallb/metallb/v0.14.9/config/manifests/metallb-native.yaml"
