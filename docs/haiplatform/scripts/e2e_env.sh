#!/bin/bash
# hai-cli env 端到端（L3）：fixture env → `hai-cli env push` → 集群注册 → 任务内 `source haienv` → 探针包 import
# 对应用例：E2E-01（AC-03/AC-04）、E2E-02（AC-05）、TC-T02/T03、TC-REG-04。
#
# 用法（host 103，以 fireflyer 身份；需 ~/.hfai/conf.yml 里有 token）：
#   bash e2e_env.sh              # 默认 provider=s3（103 上跑的是 RustFS）
#   PROVIDER=oss bash e2e_env.sh
#
# 前置：
#   1) ugc-server 已含 env push 实现（build_hai.sh + redeploy_local.sh，或部署期 deploy_pod_dev.sh）
#   2) 宿主机的 hai-cli/haienv 已含 env push（build_cli_local.sh）
#   3) 任务容器能挂到 env_root（mount_env_root.sh，否则任务内 HAIENV_PATH 不存在）
set -u

BASE="${1:-http://10.205.52.200}"
PROVIDER="${PROVIDER:-s3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURE="${SCRIPT_DIR}/env_fixture.py"

TOKEN=$(sudo grep -E '^token:' /home/fireflyer/.hfai/conf.yml | awk '{print $2}')
USER_NAME=$(sudo -u fireflyer env HOME=/home/fireflyer hai-cli whoami 2>/dev/null | head -1 | awk '{print $1}')
ENV_ROOT="${ENV_ROOT:-/nfs-shared/hai-platform/workspace/hfai_envs}"
USER_ENV="${ENV_ROOT}/${USER_NAME}"
NAME="${ENV_NAME:-e2eenv}"
TASK_NAME="${TASK_NAME:-env_e2e_probe}"
TASK_DIR="/nfs-shared/hai-platform/workspace/${USER_NAME}/env_e2e_probe"
PY="${PY:-3.8}"

PASS=0; FAIL=0
LOG=/tmp/e2e_env.log
: > "${LOG}"

log() { echo "$*" | tee -a "${LOG}"; }
ok()   { log "PASS | $1"; PASS=$((PASS+1)); }
bad()  { log "FAIL | $1"; FAIL=$((FAIL+1)); }

log "=== e2e_env against ${BASE}  $(date +%T)  user=${USER_NAME} env_root=${ENV_ROOT} provider=${PROVIDER}"

log "--- 0) 清场 + 确保任务容器能挂到 env_root"
sudo bash "${SCRIPT_DIR}/mount_env_root.sh" "${ENV_ROOT}" >> "${LOG}" 2>&1
sudo python3 "${FIXTURE}" --env-root "${ENV_ROOT}" --user "${USER_NAME}" --name "${NAME}" --clean >> "${LOG}" 2>&1
sudo rm -rf "${USER_ENV}/${NAME}_"* 2>/dev/null || true

log "--- 1) 造本地环境（真实 haienv 注册表 + 探针包 haienv_probe_unique）"
PREFIX=$(sudo python3 "${FIXTURE}" --env-root "${ENV_ROOT}" --user "${USER_NAME}" --name "${NAME}" --py "${PY}")
log "prefix=${PREFIX}"
if [ -n "${PREFIX}" ] && [ -f "${PREFIX}/lib/python${PY}/site-packages/haienv_probe_unique/__init__.py" ]; then
  ok "fixture 就绪"
else
  bad "fixture 创建失败"; log "=== 结果 PASS=${PASS} FAIL=${FAIL} ==="; exit 1
fi

log "--- 2) 第一次 push（E2E-01 / AC-03）"
PUSH1_LOG=/tmp/e2e_env_push1.log
sudo -u fireflyer env HOME=/home/fireflyer HAIENV_PATH="${USER_ENV}" \
  hai-cli env push "${NAME}" --provider "${PROVIDER}" > "${PUSH1_LOG}" 2>&1
RC=$?
log "exit=${RC}"; tail -n 6 "${PUSH1_LOG}" >> "${LOG}"
if [ "${RC}" = "0" ] && grep -q "上传并注册成功" "${PUSH1_LOG}"; then
  ok "第一次 push 退出码 0 且提示「上传并注册成功」"
else
  bad "第一次 push 失败（exit=${RC}）"
fi
if grep -q "haienv workspace push" "${PUSH1_LOG}"; then
  bad "E13 回归：命令行里出现了非法的 haienv workspace push"
else
  ok "E13 无回归（未出现 haienv workspace push）"
fi

log "--- 3) 集群侧落盘 + 注册表（AC-03/AC-04）"
if sudo test -f "${PREFIX}/lib/python${PY}/site-packages/haienv_probe_unique/__init__.py"; then
  ok "集群侧 prefix 与探针包存在（stage2 落盘）"
else
  bad "集群侧探针包缺失"
fi
DB_CHECK=$(python3 - "${USER_ENV}/venv.db" "${NAME}" "${PREFIX}" <<'PY'
import sqlite3, sys
db, name, prefix = sys.argv[1], sys.argv[2], sys.argv[3]
with sqlite3.connect(db) as conn:
    rows = conn.execute('SELECT COUNT(*) FROM "haienv" WHERE key=?', (name,)).fetchone()
print(rows[0])
PY
)
log "注册表记录数=${DB_CHECK}"
if [ "${DB_CHECK}" = "1" ]; then ok "注册表恰有 1 条记录"; else bad "注册表记录数异常（${DB_CHECK}）"; fi

log "--- 4) 第二次 push（E2E-02 / AC-05：幂等、数据已同步）"
PUSH2_LOG=/tmp/e2e_env_push2.log
sudo -u fireflyer env HOME=/home/fireflyer HAIENV_PATH="${USER_ENV}" \
  hai-cli env push "${NAME}" --provider "${PROVIDER}" > "${PUSH2_LOG}" 2>&1
RC2=$?
log "exit=${RC2}"; grep -E "数据已同步|上传并注册成功|上传失败|注册失败" "${PUSH2_LOG}" | tail -n 3 >> "${LOG}"
if [ "${RC2}" = "0" ] && grep -q "上传并注册成功" "${PUSH2_LOG}"; then
  ok "第二次 push 幂等成功"
else
  bad "第二次 push 失败（exit=${RC2}）"
fi
if grep -q "数据已同步" "${PUSH2_LOG}"; then
  ok "第二次 push 未重复上传（数据已同步，忽略本次操作）"
else
  bad "第二次 push 未命中「数据已同步」"
fi
DB_CHECK2=$(python3 - "${USER_ENV}/venv.db" "${NAME}" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1]) as conn:
    print(conn.execute('SELECT COUNT(*) FROM "haienv" WHERE key=?', (sys.argv[2],)).fetchone()[0])
PY
)
if [ "${DB_CHECK2}" = "1" ]; then ok "重复 push 未产生重复注册项"; else bad "注册项重复（${DB_CHECK2}）"; fi
if sudo -u fireflyer env HOME=/home/fireflyer HAIENV_PATH="${USER_ENV}" hai-cli env list 2>/dev/null | grep -q "${NAME}"; then
  ok "hai-cli env list 可见（AC-04）"
else
  bad "hai-cli env list 不可见"
fi

log "--- 5) 任务侧：HF_ENV_NAME=${NAME} 提交任务（TC-T02/T03 / AC-03）"
sudo mkdir -p "${TASK_DIR}" && sudo chmod 777 "${TASK_DIR}"
sudo tee "${TASK_DIR}/probe_check.py" > /dev/null <<'PYFILE'
import os
import sys

print('HAIENV_PATH=', os.environ.get('HAIENV_PATH'))
print('HF_ENV_NAME=', os.environ.get('HF_ENV_NAME'))
print('HF_ENV_OWNER=', os.environ.get('HF_ENV_OWNER'))
print('PYTHONPATH=', os.environ.get('PYTHONPATH'))
import haienv_probe_unique as m
print('PROBE_VALUE=', m.VALUE)
assert m.VALUE == 'env-push-ok', 'probe value mismatch: %s' % m.VALUE
print('PROBE_OK')
PYFILE
sudo chmod 644 "${TASK_DIR}/probe_check.py"

sudo -u fireflyer env HOME=/home/fireflyer HAIENV_PATH="${USER_ENV}" \
  HF_ENV_NAME="${NAME}" HF_ENV_OWNER="${USER_NAME}" \
  hai-cli python "${TASK_DIR}/probe_check.py" -- --nodes 1 -g training --name "${TASK_NAME}" \
  > /tmp/e2e_env_task_submit.log 2>&1
log "提交输出尾部:"; tail -n 5 /tmp/e2e_env_task_submit.log >> "${LOG}"

STATUS_JSON=/tmp/e2e_env_status.json
for i in $(seq 1 48); do
  sudo -u fireflyer env HOME=/home/fireflyer hai-cli status "${TASK_NAME}" -j > "${STATUS_JSON}" 2>/dev/null || true
  CHAIN=$(python3 -c "import json;print(json.load(open('${STATUS_JSON}')).get('chain_status',''))" 2>/dev/null || echo "")
  log "  [$i] chain_status=${CHAIN}"
  case "${CHAIN}" in
    finished|failed|stopped) break ;;
  esac
  sleep 10
done

POD_STATUS=$(python3 -c "
import json
d=json.load(open('${STATUS_JSON}'))
pods=d.get('_pods_') or []
print(pods[0]['status'] if pods else 'no-pod')
" 2>/dev/null || echo "unknown")
log "pod_status=${POD_STATUS}"
if [ "${POD_STATUS}" = "succeeded" ]; then ok "任务 pod 状态 succeeded"; else bad "任务 pod 状态 ${POD_STATUS}"; fi

sudo -u fireflyer env HOME=/home/fireflyer hai-cli logs "${TASK_NAME}" > /tmp/e2e_env_task.log 2>&1 || true
grep -E "HAIENV_PATH|HF_ENV_NAME|PROBE_VALUE|PROBE_OK|no valid env found" /tmp/e2e_env_task.log | tail -n 8 | tee -a "${LOG}"
if grep -q "PROBE_OK" /tmp/e2e_env_task.log; then
  ok "任务内 source haienv 生效且探针包 import 成功（AC-03）"
else
  bad "任务内探针包 import 失败"
fi
if grep -q "HAIENV_PATH= ${USER_ENV}\|HAIENV_PATH=${USER_ENV}" /tmp/e2e_env_task.log; then
  ok "任务内 HAIENV_PATH 与数据面同源（TC-T01）"
else
  bad "任务内 HAIENV_PATH 与期望不一致"
fi

log "=== E2E_ENV 结果: PASS=${PASS} FAIL=${FAIL} ==="
[ "${FAIL}" -eq 0 ]
