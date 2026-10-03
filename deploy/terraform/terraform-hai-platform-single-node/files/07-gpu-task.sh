#!/usr/bin/env bash
# 07-gpu-task.sh —— 关键验收：**平台任务 Pod 里能用上 V100**。
#
# 这条路和 k8s 常规 GPU 用法不同：平台不申请 nvidia.com/gpu，而是
#   调度器（按 host.gpu_num）→ assigned_gpus → init_manager 注入 NVIDIA_VISIBLE_DEVICES
#   → containerd 默认运行时 nvidia 挂载 /dev/nvidia*。
# 所以本脚本要在任务容器内同时看到：NVIDIA_VISIBLE_DEVICES、/dev/nvidia0、nvidia-smi 输出。

set -uo pipefail
source "$(dirname "$0")/task_lib.sh"

LB="${1:-$(lb_ip)}"
[ -n "$LB" ] || die "拿不到 LoadBalancer IP"

step "A. 准备隔离的 hai-cli 配置并登录"
hcli_login "$LB"

step "B. 放置 GPU 探针脚本"
SCRIPT="$(stage_task_script "$(dirname "$0")/gpu_task.py" gpu_probe_single)"
ok "脚本：$SCRIPT"

step "C. 提交任务"
TASK_ID="$(hcli_submit "$SCRIPT" gpu_probe_test)" || die "提交失败，无法解析 task id"
ok "task id = $TASK_ID"

step "D. 等待终态（最多约 15 分钟）"
read -r RESULT JOB <<<"$(hcli_wait "$TASK_ID" 90)"
echo "      chain=$RESULT job=$JOB"

step "E. 校验 GPU 可见性"
LOG="$(hcli logs "$TASK_ID" 2>/dev/null || true)"
echo "$LOG" | tail -25 | sed 's/^/      /'
if [ -z "$LOG" ] && sudo test -f "$(dirname "$SCRIPT")/gpu_output.txt"; then
  LOG="$(sudo cat "$(dirname "$SCRIPT")/gpu_output.txt" 2>/dev/null || true)"
fi
GPU_RESULT="$(echo "$LOG" | grep -m1 -oE 'GPU_RESULT .*' || true)"
CODE="$(echo "$LOG" | grep -m1 -oE 'TASK_RUNNER:EXIT_(OK|ERR)' || true)"
DEV_LINE="$(echo "$LOG" | grep -m1 -oE 'GPU_DEVS .*' || true)"
ENV_LINE="$(echo "$LOG" | grep -m1 -oE 'GPU_INFO .*' || true)"

echo "      ${ENV_LINE:-GPU_INFO <none>}"
echo "      ${DEV_LINE:-GPU_DEVS <none>}"
if [ "$JOB" = "succeeded" ] && [ "$CODE" = "TASK_RUNNER:EXIT_OK" ] && [ -n "$GPU_RESULT" ]; then
  ok "GPU 任务通过：${GPU_RESULT}（job=succeeded）"
else
  die "GPU 任务失败（chain=$RESULT job=$JOB code=${CODE:-<none>} result=${GPU_RESULT:-<none>}）"
fi
