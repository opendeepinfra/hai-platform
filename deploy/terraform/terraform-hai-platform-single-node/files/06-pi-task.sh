#!/usr/bin/env bash
# 06-pi-task.sh —— 端到端冒烟：提交 numpy 版 Monte-Carlo 求 π 任务，严格校验结果。
#
# 这是"平台真的能跑任务"的判据（不只是接口 200）：任务 succeeded + 日志里 PI 误差 < 5e-4。

set -uo pipefail
source "$(dirname "$0")/task_lib.sh"

LB="${1:-$(lb_ip)}"
[ -n "$LB" ] || die "拿不到 LoadBalancer IP"

step "A. 准备隔离的 hai-cli 配置并登录"
hcli_login "$LB"

step "B. 把 π 脚本放进平台共享工作区"
SCRIPT="$(stage_task_script "$(dirname "$0")/pi_task.py" pi_single)"
ok "脚本：$SCRIPT"

step "C. 提交任务（1 节点，分组 ${TRAINING_GROUP}）"
TASK_ID="$(hcli_submit "$SCRIPT" pi_single_test)" || die "提交失败，无法解析 task id"
ok "task id = $TASK_ID"

step "D. 等待终态（最多约 15 分钟）"
read -r RESULT JOB <<<"$(hcli_wait "$TASK_ID" 90)"
echo "      chain=$RESULT job=$JOB"

step "E. 取日志并校验 π"
LOG="$(hcli logs "$TASK_ID" 2>/dev/null || true)"
echo "$LOG" | tail -25 | sed 's/^/      /'
PI="$(echo "$LOG" | grep -m1 -oE 'PI_RESULT [0-9.]+' | awk '{print $2}')"
CODE="$(echo "$LOG" | grep -m1 -oE 'TASK_RUNNER:EXIT_(OK|ERR)' || true)"
if [ -z "$PI" ] && sudo test -f "$(dirname "$SCRIPT")/pi_output.txt"; then
  FILE_OUT="$(sudo cat "$(dirname "$SCRIPT")/pi_output.txt" 2>/dev/null || true)"
  PI="$(echo "$FILE_OUT" | grep -m1 -oE 'PI_RESULT [0-9.]+' | awk '{print $2}')"
  CODE="$(echo "$FILE_OUT" | grep -m1 -oE 'TASK_RUNNER:EXIT_(OK|ERR)' || true)"
fi
ERR=""
[ -n "$PI" ] && ERR="$(python3 -c "print(abs(float('$PI')-3.141592653589793))" 2>/dev/null || true)"

if [ "$JOB" = "succeeded" ] && [ -n "$PI" ] && [ "$CODE" = "TASK_RUNNER:EXIT_OK" ] \
   && python3 -c "import sys; sys.exit(0 if float('${ERR:-9}')<0.0005 else 1)"; then
  ok "π 任务通过：pi=$PI error=$ERR"
else
  die "π 任务失败（chain=$RESULT job=$JOB pi=${PI:-<none>} code=${CODE:-<none>} err=${ERR:-<none>}）"
fi
