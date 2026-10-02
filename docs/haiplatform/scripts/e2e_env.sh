#!/bin/bash
# hai-cli env 端到端（L3）：本地（集群外）env → `hai-cli env push` → 对象存储 → 集群落盘 + 注册
#   → 任务内 `source haienv` → 探针包 import
# 对应用例：E2E-01（AC-03/AC-04）、E2E-02（AC-05）、TC-T02/T03、TC-REG-04。
#
# ⚠️ 关键：本地环境必须放在**集群共享盘之外**（默认 /tmp/hai-env-e2e）。
#    若把「本地」env 直接建在集群 env_root 下，`haiworkspace push` 会判定「数据已同步」
#    而完全跳过上传，从而漏掉对象 key / stage2 落盘这类缺陷（C-6 就是这样被漏掉一次）。
#
# 用法（host 103，以 fireflyer 身份；需 ~/.hfai/conf.yml 里有 token）：
#   bash e2e_env.sh              # 默认 provider=s3（103 上跑的是 RustFS）
#   PROVIDER=oss bash e2e_env.sh
set -u

BASE="${1:-http://10.205.52.200}"
PROVIDER="${PROVIDER:-s3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURE="${SCRIPT_DIR}/env_fixture.py"

TOKEN=$(sudo grep -E '^token:' /home/fireflyer/.hfai/conf.yml | awk '{print $2}')
USER_NAME=$(sudo -u fireflyer env HOME=/home/fireflyer hai-cli whoami 2>/dev/null | head -1 | awk '{print $1}')
ENV_ROOT="${ENV_ROOT:-/nfs-shared/hai-platform/workspace/hfai_envs}"
USER_ENV="${ENV_ROOT}/${USER_NAME}"
LOCAL_ENV_ROOT="${LOCAL_ENV_ROOT:-/tmp/hai-env-e2e}"
LOCAL_USER_ENV="${LOCAL_ENV_ROOT}/${USER_NAME}"
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

s3_key_exists() { # s3_key_exists <key> -> 打印 FOUND / MISSING
  sudo python3 - "$1" <<'PY'
import io, re, sys
import boto3
from botocore.config import Config

src = io.open('/nfs-shared/hai-platform/override.toml', encoding='utf-8').read()


def cfg(key):
    m = re.search(r"^%s\s*=\s*'([^']*)'" % key, src, re.M)
    return m.group(1) if m else ''


try:
    s3 = boto3.client('s3', endpoint_url=cfg('endpoint'), aws_access_key_id=cfg('access_key_id'),
                      aws_secret_access_key=cfg('access_key_secret'), region_name='us-east-1',
                      config=Config(s3={'addressing_style': 'path'}))
    s3.head_object(Bucket=cfg('private_bucket'), Key=sys.argv[1])
    print('FOUND')
except Exception as e:
    print('MISSING', e)
PY
}

log "=== e2e_env against ${BASE}  $(date +%T)"
log "user=${USER_NAME} env_root=${ENV_ROOT} local_env_root=${LOCAL_ENV_ROOT} provider=${PROVIDER}"

log "--- 0) 清场（两侧都清）+ 确保任务容器能挂到 env_root"
sudo bash "${SCRIPT_DIR}/mount_env_root.sh" "${ENV_ROOT}" >> "${LOG}" 2>&1
sudo python3 "${FIXTURE}" --env-root "${ENV_ROOT}" --user "${USER_NAME}" --name "${NAME}" --clean >> "${LOG}" 2>&1
sudo rm -rf "${USER_ENV}/${NAME}_"* "${LOCAL_ENV_ROOT}" 2>/dev/null || true
sudo mkdir -p "${LOCAL_USER_ENV}" && sudo chmod -R 777 "${LOCAL_ENV_ROOT}"

log "--- 1) 造**集群外**本地环境（真实 haienv 注册表 + 探针包 haienv_probe_unique）"
LOCAL_PREFIX=$(sudo python3 "${FIXTURE}" --env-root "${LOCAL_ENV_ROOT}" --user "${USER_NAME}" --name "${NAME}" --py "${PY}")
log "local_prefix=${LOCAL_PREFIX}"
if [ -n "${LOCAL_PREFIX}" ] && [ -f "${LOCAL_PREFIX}/lib/python${PY}/site-packages/haienv_probe_unique/__init__.py" ]; then
  ok "本地 fixture 就绪（不在共享盘上：${LOCAL_PREFIX}）"
else
  bad "本地 fixture 创建失败"; log "=== 结果 PASS=${PASS} FAIL=${FAIL} ==="; exit 1
fi

log "--- 2) 第一次 push（E2E-01 / AC-03）"
PUSH1_LOG=/tmp/e2e_env_push1.log
sudo -u fireflyer env HOME=/home/fireflyer HAIENV_PATH="${LOCAL_USER_ENV}" \
  hai-cli env push "${NAME}" --provider "${PROVIDER}" > "${PUSH1_LOG}" 2>&1
RC=$?
log "exit=${RC}"; tail -n 8 "${PUSH1_LOG}" >> "${LOG}"
if [ "${RC}" = "0" ] && grep -q "上传并注册成功" "${PUSH1_LOG}"; then
  ok "第一次 push 退出码 0 且提示「上传并注册成功」"
else
  bad "第一次 push 失败（exit=${RC}）"
fi
if grep -q "数据已同步" "${PUSH1_LOG}"; then
  bad "本地环境被判定为「数据已同步」——本地不在集群盘上时不应发生（用例有效性前提）"
else
  ok "确实执行了上传（未命中「数据已同步」）"
fi
if grep -q "haienv workspace push" "${PUSH1_LOG}"; then
  bad "E13 回归：命令行里出现了非法的 haienv workspace push"
else
  ok "E13 无回归（未出现 haienv workspace push）"
fi

log "--- 3) 对象存储 key 与集群落盘 + 注册表（AC-03/AC-04）"
PRE=$(curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=${PY}")
CLUSTER_PREFIX=$(python3 -c "import json,sys;print(json.loads(sys.argv[1]).get('path',''))" "${PRE}")
CLOUD_PATH=$(python3 -c "import json,sys;print(json.loads(sys.argv[1]).get('cloud_path',''))" "${PRE}")
log "api11 path=${CLUSTER_PREFIX}"
log "api11 cloud_path=${CLOUD_PATH}"
if [ -n "${CLOUD_PATH}" ] && [[ "${CLOUD_PATH}" != /* ]] && [ "$(basename "${CLOUD_PATH}")" = "$(basename "${CLUSTER_PREFIX}")" ]; then
  ok "API-11 返回对象存储前缀且 basename 与集群目录一致（C-6）"
else
  bad "API-11 的 cloud_path 不合规：${CLOUD_PATH}"
fi
OBJ=$(s3_key_exists "${CLOUD_PATH}/$(basename "${LOCAL_PREFIX}").zip")
log "s3 ${CLOUD_PATH}/$(basename "${LOCAL_PREFIX}").zip -> ${OBJ}"
if [ "${OBJ%% *}" = "FOUND" ]; then ok "对象已落到 RustFS 期望 key"; else bad "RustFS 上没有期望的对象（${OBJ}）"; fi
if sudo test -f "${CLUSTER_PREFIX}/lib/python${PY}/site-packages/haienv_probe_unique/__init__.py"; then
  ok "集群侧 prefix 与探针包存在（stage2 落盘）"
else
  bad "集群侧探针包缺失（${CLUSTER_PREFIX}）"
fi
DB_CHECK=$(python3 - "${USER_ENV}/venv.db" "${NAME}" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1]) as conn:
    print(conn.execute('SELECT COUNT(*) FROM "haienv" WHERE key=?', (sys.argv[2],)).fetchone()[0])
PY
)
log "集群侧注册表记录数=${DB_CHECK}"
if [ "${DB_CHECK}" = "1" ]; then ok "集群侧注册表恰有 1 条记录"; else bad "注册表记录数异常（${DB_CHECK}）"; fi

log "--- 4) 第二次 push（E2E-02 / AC-05：幂等、数据已同步）"
# cluster_files/list 服务端有 30s 缓存（FR-04）；不等待会拿到空的集群列表而重新上传，测不出幂等
log "  等待 35s 让 cluster_files/list 缓存过期..."
sleep 35
PUSH2_LOG=/tmp/e2e_env_push2.log
sudo -u fireflyer env HOME=/home/fireflyer HAIENV_PATH="${LOCAL_USER_ENV}" \
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
if HAIENV_PATH="${USER_ENV}" bash -c "source haienv ${NAME} >/dev/null 2>&1 && python3 -c 'import haienv_probe_unique as m; assert m.VALUE==\"env-push-ok\"'"; then
  ok "集群侧 source haienv + 探针包 import（AC-04）"
else
  bad "集群侧 source haienv 失败"
fi

log "--- 5) 任务侧：HF_ENV_NAME=${NAME} 提交任务（TC-T02/T03 / AC-03）"
sudo mkdir -p "${TASK_DIR}" && sudo chmod 777 "${TASK_DIR}"
sudo tee "${TASK_DIR}/probe_check.py" > /dev/null <<'PYFILE'
import os

print('HAIENV_PATH=', os.environ.get('HAIENV_PATH'))
print('HF_ENV_NAME=', os.environ.get('HF_ENV_NAME'))
print('HF_ENV_OWNER=', os.environ.get('HF_ENV_OWNER'))
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
