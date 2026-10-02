#!/bin/bash
# haienv（`hai-cli env push`）一级回滚演练 —— Checklist RB-01 / RB-03 / RB-04 的可复现版本。
#
# 断言链（关闭态）：
#   ① API-11 预检          → success=0 + code=FEATURE_DISABLED
#   ② API-13 注册          → success=0 + code=FEATURE_DISABLED
#   ③ 数据面 sync_to_cluster(file_type=env) → success=0 + code=FEATURE_DISABLED   ← N4 修复点
#   ④ venv.db 未被改动（md5 + key 列表一致），已注册环境仍可读 → RB-03 无脏数据
# 然后恢复开关，断言 ① 重新可用，并校验 override.toml 回到演练前的 md5。
#
# 用法（host 103，fireflyer 身份）：
#   bash env_rollback_drill.sh
#   BASE=http://10.205.52.200 bash env_rollback_drill.sh
#
# 关键实现细节：override.toml 是**以文件 bind mount** 进 pod 的
# （one/hai-up.sh: ${HAI_PLATFORM_PATH}/override.toml:/etc/hai_one_config/override.toml）。
# `sed -i` 会 rename 出新 inode，容器里仍看旧内容；因此本脚本一律**在 pod 内原地截断重写**
# （保持 inode），改完由 supervisorctl 重启 ugc_server 让 CONF 重新加载。
set -u

BASE="${1:-${BASE:-http://10.205.52.200}}"
NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
PORT="${PORT:-8083}"
OVERRIDE="${OVERRIDE:-/nfs-shared/hai-platform/override.toml}"
POD_OVERRIDE="${POD_OVERRIDE:-/etc/hai_one_config/override.toml}"
ENV_ROOT="${ENV_ROOT:-/nfs-shared/hai-platform/workspace/hfai_envs}"
NAME="${NAME:-rbdemo}"
KEEP_ENV="${KEEP_ENV:-1}"        # 演练后是否保留演练用环境（1=保留，便于复查）

TOKEN=$(sudo grep -E '^token:' /home/fireflyer/.hfai/conf.yml | awk '{print $2}')
USER_NAME=$(sudo -u fireflyer hai-cli whoami 2>/dev/null | head -1 | awk '{print $1}')
USER_ENV="${ENV_ROOT}/${USER_NAME}"
LOG="${LOG:-/tmp/env_rollback_drill.log}"

PASS=0; FAIL=0
: > "${LOG}"
log() { echo "$*" | tee -a "${LOG}"; }

chk() { # chk <描述> <python断言表达式> <json文件>
  local desc="$1" expr="$2" file="$3"
  if python3 -c "
import json
d = json.load(open('${file}'))
assert ${expr}, d
print('ok')
" >>"${LOG}" 2>&1; then
    log "PASS | ${desc}"; PASS=$((PASS + 1))
  else
    log "FAIL | ${desc}"; tail -n 2 "${LOG}"; FAIL=$((FAIL + 1))
  fi
}

pod_py() { sudo kubectl -n "${NS}" exec -i "${POD}" -- python3 -; }
pod_curl() { sudo kubectl -n "${NS}" exec "${POD}" -- curl -s -m 10 "$@"; }

# 安全网：脚本在任何位置异常退出时都必须把开关恢复成 true，否则 env push 会被留在关闭态。
SWITCH_OFF=0
L2_ACTIVE=0
restore_on_exit() {
  local rc=$?
  if [ "${L2_ACTIVE}" = "1" ]; then
    log "!!! 脚本提前退出（rc=${rc}），强制恢复二级回滚改动（api/register/implement.py）"
    pod_py >/dev/null 2>&1 <<'PYEOF' || true
import os
path = '/high-flyer/code/multi_gpu_runner_server/api/register/implement.py'
if os.path.exists(path + '.l2bak'):
    os.replace(path + '.l2bak', path)
    print('已恢复 api/register/implement.py')
PYEOF
    restart_ugc "异常恢复（二级）" || true
  fi
  if [ "${SWITCH_OFF}" = "1" ]; then
    log "!!! 脚本提前退出（rc=${rc}），强制恢复 env_push_enabled=true"
    set_switch true || true
    restart_ugc "异常恢复" || true
  fi
  exit "${rc}"
}
trap restore_on_exit EXIT

restart_ugc() { # restart_ugc <描述>；返回耗时秒数（写进 RESTART_SECONDS）
  local t0 t1
  t0=$(date +%s)
  sudo kubectl -n "${NS}" exec "${POD}" -- supervisorctl restart ugc_server >/dev/null 2>&1 || true
  for _ in $(seq 1 60); do
    if pod_curl -o /dev/null -w '%{http_code}' "http://127.0.0.1:${PORT}/metrics" 2>/dev/null | grep -q 200; then
      break
    fi
    sleep 1
  done
  t1=$(date +%s)
  RESTART_SECONDS=$((t1 - t0))
  log "     重启 ugc_server（${1}）耗时 ${RESTART_SECONDS}s"
}

set_switch() { # set_switch true|false —— 在 pod 内原地改写（保持 inode）
  local value="$1"
  pod_py <<PY >>"${LOG}" 2>&1
import io
import re
path = '${POD_OVERRIDE}'
src = io.open(path, encoding='utf-8').read()
new, n = re.subn(r'(?m)^env_push_enabled\s*=\s*(true|false)\s*\$', 'env_push_enabled = ${value}', src)
assert n == 1, '未找到唯一的 env_push_enabled 行: %d' % n
io.open(path, 'w', encoding='utf-8').write(new)   # 截断重写：保持 inode（bind mount 才可见）
print('env_push_enabled = ${value}')
PY
}

snapshot_registry() { # snapshot_registry <输出文件>
  pod_py > "$1" <<PY
import json
import os
import sqlite3
db = '${USER_ENV}/venv.db'
info = {'db': db, 'exists': os.path.exists(db), 'md5': None, 'keys': []}
if info['exists']:
    import hashlib
    with open(db, 'rb') as f:
        info['md5'] = hashlib.md5(f.read()).hexdigest()
    conn = sqlite3.connect('file:%s?mode=ro' % db, uri=True)
    info['keys'] = [r[0] for r in conn.execute('select key from "haienv" order by rowid')]
    conn.close()
print(json.dumps(info, ensure_ascii=False))
PY
}

log "=== env 一级回滚演练  开始 $(date '+%F %T')  base=${BASE}  user=${USER_NAME}"

# ------------------------------------------------------------------ 0) 准备：造一个已注册环境
log "--- 0) 准备已注册环境 ${NAME}（走 API-13，此时开关应为 on）"
sudo mkdir -p "${USER_ENV}" && sudo chmod 777 "${USER_ENV}"
sudo mkdir -p "${USER_ENV}/${NAME}_0"
sudo chmod -R 777 "${USER_ENV}/${NAME}_0"
sudo touch "${USER_ENV}/${NAME}_0/activate"
BODY=$(python3 - "$USER_ENV" "$NAME" <<'PY'
import json
import sys
user_env, name = sys.argv[1], sys.argv[2]
print(json.dumps({'venv_name': name, 'path': f'{user_env}/{name}_0', 'py': '3.8',
                  'extra_search_dir': [], 'extra_search_bin_dir': [], 'extra_environment': []}))
PY
)
curl -s -X POST "${BASE}/ugc/register_cluster_venv?token=${TOKEN}" \
  -H 'Content-Type: application/json' -d "${BODY}" -o /tmp/rb_reg.json
cat /tmp/rb_reg.json >> "${LOG}"; echo >> "${LOG}"
chk "准备：API-13 注册 ${NAME}（开关 on）" "d['success']==1" /tmp/rb_reg.json

snapshot_registry /tmp/rb_before.json
log "     演练前注册表：$(cat /tmp/rb_before.json)"
BEFORE_MD5=$(python3 -c "import json;print(json.load(open('/tmp/rb_before.json'))['md5'])")
BEFORE_KEYS=$(python3 -c "import json;print(','.join(json.load(open('/tmp/rb_before.json'))['keys']))")
OVERRIDE_MD5=$(md5sum "${OVERRIDE}" | awk '{print $1}')
log "     override.toml md5=${OVERRIDE_MD5}"

# ------------------------------------------------------------------ 1) 关开关 + 重启
log "--- 1) 一级回滚：env_push_enabled=false"
set_switch false
SWITCH_OFF=1
restart_ugc "关闭态"
ROLLBACK_SECONDS="${RESTART_SECONDS}"

# ------------------------------------------------------------------ 2) 关闭态断言
log "--- 2) 关闭态断言（三条写入路径都必须被拒）"
curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=3.8" -o /tmp/rb_off_pre.json
cat /tmp/rb_off_pre.json >> "${LOG}"; echo >> "${LOG}"
chk "RB-01 API-11 被拒（FEATURE_DISABLED）" \
    "d['success']==0 and d['code']=='FEATURE_DISABLED'" /tmp/rb_off_pre.json

curl -s -X POST "${BASE}/ugc/register_cluster_venv?token=${TOKEN}" \
  -H 'Content-Type: application/json' -d "${BODY}" -o /tmp/rb_off_reg.json
cat /tmp/rb_off_reg.json >> "${LOG}"; echo >> "${LOG}"
chk "RB-01 API-13 被拒（FEATURE_DISABLED）" \
    "d['success']==0 and d['code']=='FEATURE_DISABLED'" /tmp/rb_off_reg.json

curl -s -X POST "${BASE}/ugc/sync_to_cluster?token=${TOKEN}&name=${NAME}&file_type=env" \
  -H 'Content-Type: application/json' -d '{"file_list":[]}' -o /tmp/rb_off_sync.json
cat /tmp/rb_off_sync.json >> "${LOG}"; echo >> "${LOG}"
chk "N4 数据面 sync_to_cluster(file_type=env) 被拒（FEATURE_DISABLED）" \
    "d['success']==0 and d['code']=='FEATURE_DISABLED'" /tmp/rb_off_sync.json

snapshot_registry /tmp/rb_after.json
AFTER_MD5=$(python3 -c "import json;print(json.load(open('/tmp/rb_after.json'))['md5'])")
AFTER_KEYS=$(python3 -c "import json;print(','.join(json.load(open('/tmp/rb_after.json'))['keys']))")
if [ "${BEFORE_MD5}" = "${AFTER_MD5}" ] && [ "${BEFORE_KEYS}" = "${AFTER_KEYS}" ]; then
  log "PASS | RB-03 关闭态期间 venv.db 未被改动（md5 与 key 列表一致）"
  PASS=$((PASS + 1))
else
  log "FAIL | RB-03 venv.db 发生变化 before=${BEFORE_MD5}/${BEFORE_KEYS} after=${AFTER_MD5}/${AFTER_KEYS}"
  FAIL=$((FAIL + 1))
fi

pod_py > /tmp/rb_readable.json <<PY
import json
import os
os.environ.setdefault('HAIENV_PATH', '${USER_ENV}')
from haienv.client.model import Haienv
key = '${NAME}'
cfg = Haienv.select(outside_db_path='${USER_ENV}/venv.db', haienv_name=key)
print(json.dumps({'key': key, 'readable': cfg is not None,
                  'path': getattr(cfg, 'path', None)}, ensure_ascii=False))
PY
cat /tmp/rb_readable.json >> "${LOG}"; echo >> "${LOG}"
chk "RB-03 已注册环境在关闭态仍可读（haienv 能反序列化）" \
    "d['readable'] is True and d['path']" /tmp/rb_readable.json

# ------------------------------------------------------------------ 3) 恢复
log "--- 3) 恢复：env_push_enabled=true"
set_switch true
SWITCH_OFF=0
restart_ugc "恢复态"
RESTORE_SECONDS="${RESTART_SECONDS}"

curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=3.8" -o /tmp/rb_on_pre.json
cat /tmp/rb_on_pre.json >> "${LOG}"; echo >> "${LOG}"
chk "恢复后 API-11 重新可用（并复用已注册路径）" \
    "d['success']==1 and d['exists'] is True and d['path'].endswith('${NAME}_0')" /tmp/rb_on_pre.json

NEW_OVERRIDE_MD5=$(md5sum "${OVERRIDE}" | awk '{print $1}')
if [ "${NEW_OVERRIDE_MD5}" = "${OVERRIDE_MD5}" ]; then
  log "PASS | override.toml 已回到演练前内容（md5=${OVERRIDE_MD5}）"
  PASS=$((PASS + 1))
else
  log "FAIL | override.toml 未复原 before=${OVERRIDE_MD5} after=${NEW_OVERRIDE_MD5}"
  FAIL=$((FAIL + 1))
fi

# ------------------------------------------------------------------ 4) 二级回滚（可选：DRILL_L2=1）
if [ "${DRILL_L2:-0}" = "1" ]; then
  log "--- 4) 二级回滚：注释掉两条路由注册 → API-11/API-13 应 404（workspace 路由不受影响）"
  L2_OK=$(pod_py 2>&1 <<'PYEOF'
import io
import re
import shutil
path = '/high-flyer/code/multi_gpu_runner_server/api/register/implement.py'
shutil.copyfile(path, path + '.l2bak')
src = io.open(path, encoding='utf-8').read()
new, n = re.subn(r"(?m)^(\s*)(app\.post\('/ugc/(?:update|register)_cluster_venv'\).*)$",
                 r'\1# \2', src)
if n == 2:
    io.open(path, 'w', encoding='utf-8').write(new)
print('OK' if n == 2 else 'NG:%d' % n)
PYEOF
)
  log "     二级回滚改动结果：${L2_OK}"
  if [ "${L2_OK}" != "OK" ]; then
    log "FAIL | RB-02 无法完成二级回滚改动（路由注册行未匹配，见日志）"; FAIL=$((FAIL + 1))
  else
    L2_ACTIVE=1
    restart_ugc "二级回滚"

    for route in update_cluster_venv register_cluster_venv; do
      code=$(curl -s -o "/tmp/rb_l2_${route}.json" -w '%{http_code}' -X POST \
        "${BASE}/ugc/${route}?token=${TOKEN}&venv_name=${NAME}&py=3.8")
      log "     /ugc/${route} → HTTP ${code}"
      if [ "${code}" = "404" ]; then
        log "PASS | RB-02 ${route} 已下线（HTTP 404）"; PASS=$((PASS + 1))
      else
        log "FAIL | RB-02 ${route} 期望 404，实际 ${code}"; FAIL=$((FAIL + 1))
      fi
    done

    code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${BASE}/ugc/get_sync_status?token=${TOKEN}&index=none")
    if [ "${code}" != "404" ]; then
      log "PASS | RB-02 workspace 路由不受影响（/ugc/get_sync_status HTTP ${code}）"; PASS=$((PASS + 1))
    else
      log "FAIL | RB-02 workspace 路由被误伤（HTTP 404）"; FAIL=$((FAIL + 1))
    fi

    if [ -n "${CLIENT_ENV_PATH:-}" ] && [ -n "${CLIENT_ENV_NAME:-}" ]; then
      CLIENT_OUT=$(HAIENV_PATH="${CLIENT_ENV_PATH}" hai-cli env push "${CLIENT_ENV_NAME}" 2>&1 | tail -2)
      log "     客户端输出：${CLIENT_OUT}"
      if echo "${CLIENT_OUT}" | grep -qE "接口不存在|Not Found"; then
        log "PASS | RB-02 客户端在路由下线时给出明确错误"; PASS=$((PASS + 1))
      else
        log "FAIL | RB-02 客户端错误不明确：${CLIENT_OUT}"; FAIL=$((FAIL + 1))
      fi
    fi

    pod_py >/dev/null 2>&1 <<'PYEOF' || true
import os
path = '/high-flyer/code/multi_gpu_runner_server/api/register/implement.py'
if os.path.exists(path + '.l2bak'):
    os.replace(path + '.l2bak', path)
    print('已恢复 api/register/implement.py')
PYEOF
    L2_ACTIVE=0
    restart_ugc "二级回滚恢复"
    curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=3.8" -o /tmp/rb_l2_restore.json
    cat /tmp/rb_l2_restore.json >> "${LOG}"; echo >> "${LOG}"
    chk "RB-02 恢复路由后 API-11 重新可用" "d['success']==1" /tmp/rb_l2_restore.json
  fi
fi

if [ "${KEEP_ENV}" != "1" ]; then
  log "--- 清理：删除演练环境 ${NAME}_0 与注册项"
  pod_py >/dev/null 2>&1 <<PY || true
import sqlite3
conn = sqlite3.connect('${USER_ENV}/venv.db')
conn.execute('DELETE FROM "haienv" WHERE key=?', ('${NAME}',))
conn.commit()
PY
  sudo rm -rf "${USER_ENV}/${NAME}_0"
fi

log ""
log "=== 演练结果 $(date '+%F %T')  PASS=${PASS} FAIL=${FAIL}"
log "    一级回滚（关开关+重启 ugc_server）耗时：${ROLLBACK_SECONDS}s；恢复耗时：${RESTORE_SECONDS}s"
log "    日志：${LOG}"
[ "${FAIL}" = "0" ]
