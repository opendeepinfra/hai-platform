#!/bin/bash
# `hai-cli env`（haienv）一键验证：L1 单元 / 客户端单测 / L2 接口 / L3 端到端 / workspace 回归。
#
# 用法（host 103）：
#   bash verify_env.sh            # 全跑
#   SKIP_E2E=1 bash verify_env.sh # 跳过需要重建镜像的 E2E
#   SKIP_REG=1 bash verify_env.sh # 跳过 workspace 回归（较慢）
#
# 每步的完整输出落在 /tmp/verify_env_<step>.log。
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${BASE:-http://10.205.52.200}"
REPO="${REPO:-$HOME/hai-platform}"
NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"

PASSED=(); FAILED=()

step() { # step <名字> <命令...>
  local name="$1"; shift
  local log="/tmp/verify_env_${name}.log"
  echo "───────────────────────────────────────────────"
  echo ">>> ${name}: $*"
  if "$@" > "${log}" 2>&1; then
    echo "<<< ${name}: PASS (log: ${log})"
    PASSED+=("${name}")
  else
    echo "<<< ${name}: FAIL (log: ${log})"
    tail -n 15 "${log}"
    FAILED+=("${name}")
  fi
}

l1_unit() {
  sudo kubectl -n "${NS}" exec "${POD}" -- sh -c \
    "cd /high-flyer/code/multi_gpu_runner_server && MARSV2_MANAGER_CONFIG_DIR=/etc/hai_one_config \
     python3 -m pytest tests/env/test_env_registry.py -q --no-header -p no:cacheprovider"
}

l1_client() {
  # 客户端侧单元测试：env push 链路（E3/E7/E13/C-6）+ haienv create 的 CUDA 门禁（E10/C-8）
  cd "${REPO}" && HAIENV_PATH=$(mktemp -d) python3 -m pytest \
    tests/env/test_client_push.py tests/env/test_haienv_create_prereq.py -q --no-header -p no:cacheprovider
}

step l1_unit l1_unit
step l1_client l1_client
step l2_smoke bash "${SCRIPT_DIR}/smoke_env.sh" "${BASE}"
step compat_idempotent bash "${SCRIPT_DIR}/check_env_idempotent.sh" "${BASE}"

if [ "${SKIP_DRILL:-0}" != "1" ]; then
  # 一级回滚演练会改 override.toml 并重启 ugc_server 两次（约 10s），默认执行
  step rollback_drill bash "${SCRIPT_DIR}/env_rollback_drill.sh" "${BASE}"
fi

if [ "${SKIP_E2E:-0}" != "1" ]; then
  step l3_e2e bash "${SCRIPT_DIR}/e2e_env.sh" "${BASE}"
fi

if [ "${SKIP_REG:-0}" != "1" ]; then
  step reg_ugc bash "${SCRIPT_DIR}/smoke_ugc.sh" "${BASE}"
  step reg_workspace bash "${SCRIPT_DIR}/e2e_workspace.sh" all
fi

echo "═══════════════════════════════════════════════"
echo "验证汇总: PASS=${#PASSED[@]} FAIL=${#FAILED[@]}"
[ "${#PASSED[@]}" -gt 0 ] && echo "  PASS: ${PASSED[*]}"
[ "${#FAILED[@]}" -gt 0 ] && echo "  FAIL: ${FAILED[*]}"
[ "${#FAILED[@]}" -eq 0 ]
