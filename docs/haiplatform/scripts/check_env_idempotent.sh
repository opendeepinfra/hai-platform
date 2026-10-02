#!/bin/bash
# N3 幂等性 / 兼容性在线自检 —— 把「注册失败后重试不该重复上传」这件事做成可复现的断言。
#
# 背景：旧实现在注册表里查不到名字时，无条件取「第一个空闲后缀」。于是
#   ① 客户端上传成功 → ② API-13 注册失败（或客户端中断）→ ③ 用户重试 `env push`
# 会分配 `name_1`，把整份环境**再传一遍**（103 上实测复现 `xxx_0 → xxx_1`）。
# 修复后：预检发现磁盘上已有 `name_0` 就复用它（reused=true），重试落在同一批对象 key 上。
#
# 用法（host 103）：
#   bash check_env_idempotent.sh [base_url]
#
# 说明：目录一律在 **pod 内**创建 —— 真实链路里 `name_0` 也是数据面（pod 内）落盘的，
# 这样可以避免 NFS 属性缓存带来的等待，同时精确复现目标场景。
set -u

BASE="${1:-${BASE:-http://10.205.52.200}}"
NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
ENV_ROOT="${ENV_ROOT:-/nfs-shared/hai-platform/workspace/hfai_envs}"
NAME="${NAME:-idemcheck}"
LOG="${LOG:-/tmp/check_env_idempotent.log}"

TOKEN=$(sudo grep -E '^token:' /home/fireflyer/.hfai/conf.yml | awk '{print $2}')
USER_NAME=$(sudo -u fireflyer hai-cli whoami 2>/dev/null | head -1 | awk '{print $1}')
USER_ENV="${ENV_ROOT}/${USER_NAME}"

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

precheck() { # precheck <输出文件>
  curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=3.8" -o "$1"
  cat "$1" >> "${LOG}"; echo >> "${LOG}"
}

log "=== N3 幂等自检  开始 $(date '+%F %T')  base=${BASE}  user=${USER_NAME}  name=${NAME}"

log "--- 0) 清理上一次自检残留（注册项 + ${NAME}_* 目录）"
pod_py >/dev/null 2>&1 <<PY || true
import os
import sqlite3
db = '${USER_ENV}/venv.db'
if os.path.exists(db):
    conn = sqlite3.connect(db)
    conn.execute('DELETE FROM "haienv" WHERE key LIKE ?', ('${NAME}%',))
    conn.commit()
PY
sudo kubectl -n "${NS}" exec "${POD}" -- bash -lc "rm -rf '${USER_ENV}/${NAME}_'*" 2>/dev/null || true
sudo mkdir -p "${USER_ENV}" && sudo chmod 777 "${USER_ENV}"

log "--- 1) 首次预检（未注册、磁盘上无同名目录）→ 必须给 _0 且 reused=false"
precheck /tmp/idem1.json
chk "首次预检 = _0 / exists=false / reused=false" \
    "d['success']==1 and d['path'].endswith('${NAME}_0') and d['exists'] is False and d['reused'] is False" /tmp/idem1.json
P0=$(python3 -c "import json;print(json.load(open('/tmp/idem1.json'))['path'])")

log "--- 2) 模拟「上传成功但注册失败」：pod 内落盘 ${P0}（真实链路里由数据面创建）"
sudo kubectl -n "${NS}" exec "${POD}" -- bash -lc "mkdir -p '${P0}' && chmod 777 '${P0}' && touch '${P0}/activate'"

log "--- 3) 重试预检 → 必须复用同一个目录（N3 修复点；旧实现会给 _1）"
precheck /tmp/idem2.json
chk "重试复用同一路径（reused=true，不再分配 _1）" \
    "d['success']==1 and d['path']=='${P0}' and d['reused'] is True and d['exists'] is False" /tmp/idem2.json

log "--- 4) 补登记（API-13）→ 幂等写注册表"
BODY=$(python3 - "$P0" "$NAME" <<'PY'
import json
import sys
path, name = sys.argv[1], sys.argv[2]
print(json.dumps({'venv_name': name, 'path': path, 'py': '3.8',
                  'extra_search_dir': [], 'extra_search_bin_dir': [], 'extra_environment': []}))
PY
)
curl -s -X POST "${BASE}/ugc/register_cluster_venv?token=${TOKEN}" \
  -H 'Content-Type: application/json' -d "${BODY}" -o /tmp/idem3.json
cat /tmp/idem3.json >> "${LOG}"; echo >> "${LOG}"
chk "API-13 补登记成功" "d['success']==1" /tmp/idem3.json

log "--- 5) 注册后预检 → exists=true（可跳过上传的判据）且仍指向同一目录"
precheck /tmp/idem4.json
chk "注册后 exists=true / 同一路径 / reused=true / 返回 haienv_version" \
    "d['success']==1 and d['exists'] is True and d['path']=='${P0}' and d['reused'] is True and d.get('haienv_version')" /tmp/idem4.json

log "--- 6) 重复注册（同 body）→ 注册表记录数不增长"
curl -s -X POST "${BASE}/ugc/register_cluster_venv?token=${TOKEN}" \
  -H 'Content-Type: application/json' -d "${BODY}" -o /tmp/idem5.json
COUNT=$(pod_py <<PY
import sqlite3
conn = sqlite3.connect('${USER_ENV}/venv.db')
print(conn.execute('select count(*) from "haienv" where key=?', ('${NAME}',)).fetchone()[0])
PY
)
log "     注册表 ${NAME} 记录数=${COUNT}"
if [ "$(echo "${COUNT}" | tr -d '[:space:]')" = "1" ]; then
  log "PASS | 重复注册幂等（记录数=1）"; PASS=$((PASS + 1))
else
  log "FAIL | 重复注册后记录数=${COUNT}"; FAIL=$((FAIL + 1))
fi

log ""
log "=== 结果 $(date '+%F %T')  PASS=${PASS} FAIL=${FAIL}  （日志：${LOG}）"
[ "${FAIL}" = "0" ]
