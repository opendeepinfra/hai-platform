#!/bin/bash
# hai-cli env（haienv）接口冒烟：API-11 /ugc/update_cluster_venv 与 API-13 /ugc/register_cluster_venv
# 对应用例集 docs/haiplatform/env/env-server-test-cases.md §4.2（A 组）+ §4.6（S 组部分）。
#
# 用法（在 host 103 上，以 fireflyer 身份；token 取自 ~/.hfai/conf.yml）：
#   bash smoke_env.sh [base_url]
#
# 前置：ugc-server 已包含 env push 实现（build_hai.sh + redeploy_local.sh，
#       或联调期用 deploy_pod_dev.sh 快速覆盖）。
set -u

BASE="${1:-http://10.205.52.200}"
TOKEN=$(sudo grep -E '^token:' /home/fireflyer/.hfai/conf.yml | awk '{print $2}')
USER_NAME=$(sudo -u fireflyer hai-cli whoami 2>/dev/null | head -1 | awk '{print $1}')
ENV_ROOT="${ENV_ROOT:-/nfs-shared/hai-platform/workspace/hfai_envs}"
USER_ENV="${ENV_ROOT}/${USER_NAME}"
NAME="${NAME:-smokeenv}"
OTHER_USER="${OTHER_USER:-nobody_else}"

PASS=0; FAIL=0
LOG=/tmp/smoke_env.log
FIXTURE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env_fixture.py"
: > "${LOG}"

log() { echo "$*" | tee -a "${LOG}"; }
chk() { # chk <描述> <python断言表达式> <json文件>
  local desc="$1" expr="$2" file="$3"
  if python3 -c "
import json
d=json.load(open('${file}'))
assert ${expr}, d
print('ok')
" >>"${LOG}" 2>&1; then
    log "PASS | ${desc}"; PASS=$((PASS+1))
  else
    log "FAIL | ${desc}"; tail -n 1 "${LOG}"; FAIL=$((FAIL+1))
  fi
}

log "=== smoke_env against ${BASE}  $(date +%T)  user=${USER_NAME} env_root=${ENV_ROOT}"

log "--- 0) 环境准备：${USER_ENV}"
sudo mkdir -p "${USER_ENV}" && sudo chmod 777 "${USER_ENV}"
# 清掉上一次冒烟残留的注册与目录，保证 exists=false 分支可判
python3 - "$USER_ENV" "$NAME" <<'PY' 2>/dev/null || true
import os, sqlite3, sys
user_env, name = sys.argv[1], sys.argv[2]
db = os.path.join(user_env, 'venv.db')
if os.path.exists(db):
    with sqlite3.connect(db) as conn:
        conn.execute('DELETE FROM "haienv" WHERE key=?', (name,))
        conn.commit()
PY
sudo rm -rf "${USER_ENV}/${NAME}_"* 2>/dev/null || true

log "--- 1) API-11 预检（新环境，应 success=1 + exists=false + path 以 _0 结尾）"
curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=3.8&extend=False" -o /tmp/env1.json
cat /tmp/env1.json >> "${LOG}"; echo >> "${LOG}"
chk "API-11 新环境" "d['success']==1 and d['exists'] is False and d['path'].endswith('_0') and d['path'].startswith('${USER_ENV}/')" /tmp/env1.json

log "--- 2) API-11 旧形态（省略 extend，AC-09）"
curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=3.8" -o /tmp/env2.json
chk "API-11 旧形态等价" "d['success']==1 and d['path']==json.load(open('/tmp/env1.json'))['path']" /tmp/env2.json

log "--- 3) API-11 幂等（重复 3 次响应相同，TC-A08）"
for i in 1 2 3; do
  curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=3.8" -o "/tmp/env3_${i}.json"
done
chk "API-11 幂等" "d==json.load(open('/tmp/env1.json'))" /tmp/env3_3.json

log "--- 4) API-11 拒绝 extend=True（TC-A04）"
curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=3.8&extend=True" -o /tmp/env4.json
chk "API-11 extend 拒绝" "d['success']==0 and d['code']=='INVALID_PARAM'" /tmp/env4.json

log "--- 5) API-11 非法名（TC-A05：'', '../x', 'a/b', 'a b', 超长）"
python3 - "$TOKEN" "$BASE" <<'PY'
import json, subprocess, sys, urllib.parse
token, base = sys.argv[1], sys.argv[2]
bad = ['', '../x', 'a/b', 'a b', 'a'*65]
results = []
for name in bad:
    url = f'{base}/ugc/update_cluster_venv?token={token}&venv_name={urllib.parse.quote(name)}&py=3.8'
    out = subprocess.run(['curl', '-s', '-X', 'POST', url], capture_output=True, text=True).stdout
    results.append(json.loads(out))
open('/tmp/env5.json', 'w').write(json.dumps(results))
PY
chk "API-11 非法名全部 INVALID_PARAM" "all(r['success']==0 and r['code']=='INVALID_PARAM' for r in d)" /tmp/env5.json

log "--- 6) API-11 缺 token（TC-A07，应带 success=0）"
code=$(curl -s -o /tmp/env6.json -w '%{http_code}' -X POST "${BASE}/ugc/update_cluster_venv?venv_name=${NAME}&py=3.8")
log "http=${code}"
chk "API-11 缺 token" "'success' in d and d['success']==0" /tmp/env6.json

log "--- 7) 构造 fixture prefix（TC-A11 前置；真实 haienv 包写注册表 + 可用的 activate）"
P=$(sudo python3 "${FIXTURE}" --env-root "${ENV_ROOT}" --user "${USER_NAME}" --name "${NAME}" --py 3.8 \
      --extra-search-dir /opt/x --extra-search-dir /opt/y)
log "prefix=${P}"
sudo chmod -R 777 "${P}" "${USER_ENV}" 2>/dev/null || true

log "--- 8) API-13 注册（text/plain body，TC-A11/A12）"
REG_BODY="{\"venv_name\":\"${NAME}\",\"path\":\"${P}\",\"py\":\"3.8\",\"extra_search_dir\":[\"/opt/x\",\"/opt/y\"],\"extra_search_bin_dir\":[\"/opt/bin\"],\"extra_environment\":[\"TEMP=temp\"]}"
curl -s -X POST "${BASE}/ugc/register_cluster_venv?token=${TOKEN}" \
  -H 'Content-Type: text/plain; charset=utf-8' \
  -d "${REG_BODY}" \
  -o /tmp/env8.json
cat /tmp/env8.json >> "${LOG}"; echo >> "${LOG}"
chk "API-13 注册成功" "d['success']==1 and d['registered'] is True and d['path']=='${P}' and d['db']=='${USER_ENV}/venv.db'" /tmp/env8.json

log "--- 9) API-13 幂等（application/json 同 body，记录数不增长，TC-A17）"
curl -s -X POST "${BASE}/ugc/register_cluster_venv?token=${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d "${REG_BODY}" -o /tmp/env9.json
CNT=$(python3 - "$USER_ENV/venv.db" "$NAME" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1]) as conn:
    print(conn.execute('SELECT COUNT(*) FROM "haienv" WHERE key=?', (sys.argv[2],)).fetchone()[0])
PY
)
log "count=${CNT}"
chk "API-13 幂等" "d['success']==1 and ${CNT}==1" /tmp/env9.json

log "--- 10) API-11 已注册（exists=true 且复用同一 path，TC-A01）"
curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=3.8" -o /tmp/env10.json
chk "API-11 exists=true 复用" "d['success']==1 and d['exists'] is True and d['path']=='${P}'" /tmp/env10.json

log "--- 11) API-13 越界 path（TC-A15/SEC-04）"
for evil in '/tmp/evil' "${ENV_ROOT}/../etc" "${USER_ENV}/../../${OTHER_USER}/x"; do
  curl -s -X POST "${BASE}/ugc/register_cluster_venv?token=${TOKEN}" \
    -H 'Content-Type: text/plain; charset=utf-8' \
    -d "{\"venv_name\":\"${NAME}\",\"path\":\"${evil}\",\"py\":\"3.8\"}" -o /tmp/env11.json
  chk "API-13 越界拒绝 path=${evil}" "d['success']==0 and d['code'] in ('PATH_ESCAPE','FORBIDDEN')" /tmp/env11.json
done

log "--- 12) API-13 他人目录（TC-A14/SEC-02）"
curl -s -X POST "${BASE}/ugc/register_cluster_venv?token=${TOKEN}" \
  -H 'Content-Type: text/plain; charset=utf-8' \
  -d "{\"venv_name\":\"${NAME}\",\"path\":\"${ENV_ROOT}/${OTHER_USER}/${NAME}_0\",\"py\":\"3.8\"}" -o /tmp/env12.json
chk "API-13 他人目录拒绝" "d['success']==0 and d['code'] in ('PATH_ESCAPE','FORBIDDEN')" /tmp/env12.json

log "--- 13) API-13 非法名 / 缺 py（TC-A13）"
curl -s -X POST "${BASE}/ugc/register_cluster_venv?token=${TOKEN}" \
  -H 'Content-Type: text/plain; charset=utf-8' \
  -d "{\"venv_name\":\"../x\",\"path\":\"${P}\",\"py\":\"3.8\"}" -o /tmp/env13a.json
curl -s -X POST "${BASE}/ugc/register_cluster_venv?token=${TOKEN}" \
  -H 'Content-Type: text/plain; charset=utf-8' \
  -d "{\"venv_name\":\"${NAME}\",\"path\":\"${P}\"}" -o /tmp/env13b.json
chk "API-13 非法名" "d['success']==0 and d['code']=='INVALID_PARAM'" /tmp/env13a.json
chk "API-13 缺 py" "d['success']==0 and d['code']=='INVALID_PARAM'" /tmp/env13b.json

log "--- 14) 注册表反序列化（客户端 Haienv.select，TC-REG-02/03）"
HAIENV_PATH="${USER_ENV}" python3 - "$USER_ENV" "$NAME" "$P" <<'PY' >> "${LOG}" 2>&1
import sys
from haienv.client.model import Haienv
user_env, name, path = sys.argv[1], sys.argv[2], sys.argv[3]
got = Haienv.select(outside_db_path=f'{user_env}/venv.db', haienv_name=name)
assert got is not None, 'record missing'
assert got.path == path, (got.path, path)
assert got.extend == 'False'
assert list(got.extra_search_dir) == ['/opt/x', '/opt/y'], got.extra_search_dir
assert list(got.extra_environment) == ['TEMP=temp']
print('ok')
PY
if [ $? -eq 0 ]; then log "PASS | 客户端 Haienv.select 反序列化"; PASS=$((PASS+1)); else log "FAIL | 客户端 Haienv.select 反序列化"; FAIL=$((FAIL+1)); fi

log "--- 15) 客户端可见性 env list（TC-REG-04/AC-04）"
if sudo -u fireflyer env HOME=/home/fireflyer HAIENV_PATH="${USER_ENV}" hai-cli env list 2>/dev/null | grep -q "${NAME}"; then
  log "PASS | hai-cli env list 可见"; PASS=$((PASS+1))
else
  log "FAIL | hai-cli env list 不可见"; FAIL=$((FAIL+1))
fi

log "--- 16) source haienv 可用（TC-REG-04/T-02）"
if HAIENV_PATH="${USER_ENV}" bash -c "source haienv ${NAME} >/dev/null 2>&1 && python3 -c 'import haienv_probe_unique as m; assert m.VALUE==\"env-push-ok\"'"; then
  log "PASS | source haienv + 探针包 import"; PASS=$((PASS+1))
else
  log "FAIL | source haienv + 探针包 import"; FAIL=$((FAIL+1))
fi

log "--- 17) S 组：伪造 username/group 被忽略（TC-S01/SEC-01）"
curl -s -X POST "${BASE}/ugc/update_cluster_venv?token=${TOKEN}&venv_name=${NAME}&py=3.8&username=${OTHER_USER}&group=forged" -o /tmp/env17.json
chk "SEC-01 忽略伪造身份" "d['success']==1 and d['path'].startswith('${USER_ENV}/')" /tmp/env17.json

log "=== SMOKE_ENV 结果: PASS=${PASS} FAIL=${FAIL} ==="
[ "${FAIL}" -eq 0 ]
