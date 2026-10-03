#!/usr/bin/env bash
#
# verify.sh —— 只读验收平台部署情况（Pod / Service / LB / hai-cli / DB 里的 gpu_num）。
#
# 用法：
#   ./verify.sh              # 完整验收（不提交任务）
#   ./verify.sh --tasks      # 额外跑 π 任务 + GPU 任务端到端测试
#
# 环境变量：HOST、TF_DIR、NODE_NAME、KUBECONFIG_PATH...

set -euo pipefail

HOST="${HOST:-fireflyer@192.168.100.103}"
TF_DIR="${TF_DIR:-/opt/terraform/hai-platform-single-node}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST")
HERE="$(cd "$(dirname "$0")" && pwd)"
RUN_TASKS=0

for arg in "$@"; do
  case "$arg" in
    --tasks) RUN_TASKS=1 ;;
    -h|--help) sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数：${arg}（--tasks）" >&2; exit 2 ;;
  esac
done

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

log "同步脚本到 ${TF_DIR}"
"${SSH[@]}" "sudo mkdir -p '${TF_DIR}' && sudo chown \$(id -u):\$(id -g) '${TF_DIR}'"
tar czf - -C "$HERE" files | "${SSH[@]}" "tar xzf - -C '${TF_DIR}'"
"${SSH[@]}" "chmod +x '${TF_DIR}'/files/*.sh"

log "只读验收"
ENVS=""
for v in NODE_NAME KUBECONFIG_PATH TASK_NAMESPACE SHARED_FS_ROOT POSTGRES_USER NODE_GPUS TRAINING_GROUP; do
  if [ -n "${!v:-}" ]; then ENVS="${ENVS} ${v}=${!v}"; fi
done
"${SSH[@]}" "sudo env${ENVS} bash '${TF_DIR}/files/05-verify.sh'"

if [ "$RUN_TASKS" = "1" ]; then
  log "π 任务端到端测试"
  "${SSH[@]}" "sudo env${ENVS} bash '${TF_DIR}/files/06-pi-task.sh'"
  log "GPU 任务端到端测试"
  "${SSH[@]}" "sudo env${ENVS} bash '${TF_DIR}/files/07-gpu-task.sh'"
fi
