#!/usr/bin/env bash
#
# verify.sh —— 只读验收：集群状态 + GPU 是否在 Pod 里真的可用。
#
# 默认行为：
#   1. 打印节点 / Pod / nvidia.com/gpu 容量；
#   2. 跑一个 GPU 冒烟 Pod（**不申请 nvidia.com/gpu**，只注入 NVIDIA_VISIBLE_DEVICES=0），
#      这正是 Hai Platform 任务 Pod 的形态 —— 验证 containerd 默认运行时 = nvidia 这条链路。
#
# 用法：
#   ./verify.sh              # 完整验收（含冒烟 Pod）
#   ./verify.sh --no-smoke   # 只看状态，不起 Pod
#   ./verify.sh --keep       # 保留冒烟 Pod 供取证
#
# 环境变量（与 main.tf 变量对应，默认值即 103 现状）：
#   HOST / TF_DIR / NODE_IP / KUBECONFIG_PATH / DEFAULT_RUNTIME / GPU_SMOKE_IMAGE

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
TF_DIR="${TF_DIR:-/opt/terraform/k8s-single-node}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
HERE="$(cd "$(dirname "$0")" && pwd)"

SKIP_SMOKE="false"
KEEP="false"
for arg in "$@"; do
  case "$arg" in
    --no-smoke) SKIP_SMOKE="true" ;;
    --keep)     KEEP="true" ;;
    -h|--help)  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数：${arg}（--no-smoke | --keep）" >&2; exit 2 ;;
  esac
done

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

log "同步验证脚本到 ${TF_DIR}（保持与仓库一致）"
"${SSH[@]}" "sudo mkdir -p '$TF_DIR' && sudo chown \$(id -u):\$(id -g) '$TF_DIR'"
tar czf - -C "$HERE" files | "${SSH[@]}" "tar xzf - -C '$TF_DIR'"
"${SSH[@]}" "chmod +x '$TF_DIR'/files/*.sh"

log "在 103 上执行只读验收"
ENVS="SKIP_SMOKE=${SKIP_SMOKE} KEEP_SMOKE_POD=${KEEP}"
for v in NODE_IP KUBECONFIG_PATH DEFAULT_RUNTIME GPU_SMOKE_IMAGE GPU_SMOKE_IMPORT_FROM_DOCKER IMAGE_REPOSITORY; do
  if [ -n "${!v:-}" ]; then ENVS="$ENVS $v=${!v}"; fi
done
"${SSH[@]}" "sudo env $ENVS bash '$TF_DIR/files/07-verify.sh'"
