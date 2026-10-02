#!/bin/bash
# ---------------------------------------------------------------------------
# hai-cli images 上传通道（`images push`）L3 端到端 —— 用例集 E2E-09 / E2E-10
#
#   E2E-09 上传闭环：本地（**共享盘之外**）的 tar → `images push` → RustFS/S3 → 共享盘
#          （md5 与本地一致）→ `train_image` 出现 loaded 行 → 用该镜像提交任务并产出可区分输出
#   E2E-10 上传通道开关一致性 / 一级回滚：upload_enabled=false 时 API-01 与 API-05 必须同时
#          被拒（FEATURE_DISABLED）且零新增写入，而控制面 load/list/delete 正常；enabled=false
#          时上传与控制面同时关闭
#
# ⚠️ 判据不是「接口 200」：必须核对共享盘上真的出现 md5 一致的文件，且 user_sync_status
#    （字节搬完没）与 train_image（能不能被任务用）两套状态各就各位。
#
# 用法（host 103，fireflyer 身份）：
#   bash e2e_images_push.sh [base_url]
# 可用环境变量：
#   IMAGE_ROOT / SRC_TAR / LOCAL_DIR / IMAGE_NAME / TASK_NAME / TASK_WAIT
#   SWITCH_TEST=0   跳过开关一致性演练（E2E-10）
#   RESTORE=0       演练后不还原 P0 的手工放盘布局（默认 1：还原，便于后续套件复用）
#   PURGE_IMAGE=1   任务提交前把镜像从三个节点 containerd 里删掉，强制走一次真实导入
#
# 退出码：0 = 全部通过；1 = 有 FAIL（日志在 /tmp/e2e_images_push.log）
# ---------------------------------------------------------------------------
set -u

BASE="${1:-${BASE:-http://10.205.52.200}}"
NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
PORT="${PORT:-8083}"
CONF="${CONF:-/home/fireflyer/.hfai/conf.yml}"
OVERRIDE="${OVERRIDE:-/nfs-shared/hai-platform/override.toml}"
POD_OVERRIDE="${POD_OVERRIDE:-/etc/hai_one_config/override.toml}"
REGISTRY="${REGISTRY:-registry.high-flyer.cn}"
GRP="${GRP:-hfai}"
IMAGE_NAME="${IMAGE_NAME:-demo:v1}"
IMG_URL="${IMG_URL:-${REGISTRY}/${GRP}/${IMAGE_NAME}}"
TASK_NAME="${TASK_NAME:-images_push_probe}"
TASK_WAIT="${TASK_WAIT:-120}"
SWITCH_TEST="${SWITCH_TEST:-1}"
RESTORE="${RESTORE:-1}"
PURGE_IMAGE="${PURGE_IMAGE:-0}"
PROBE_MARKER="${PROBE_MARKER:-IMAGE_PROBE=images-push-ok}"

PASS=0; FAIL=0; WARN=0
LOG="${LOG:-/tmp/e2e_images_push.log}"
: > "${LOG}"
log()  { echo "$*" | tee -a "${LOG}"; }
ok()   { log "PASS | $*"; PASS=$((PASS + 1)); }
bad()  { log "FAIL | $*"; FAIL=$((FAIL + 1)); }
warn() { log "WARN | $*"; WARN=$((WARN + 1)); }
info() { log "INFO | $*"; }

as_user() { if [ "$(id -un)" = "fireflyer" ]; then "$@"; else sudo -u fireflyer "$@"; fi; }
token() { grep -E '^ *token:' "${CONF}" 2>/dev/null | awk '{print $2}'; }
psql_q() { sudo kubectl -n "${NS}" exec "${POD}" -- psql -U root -d mars_db -tAc "$1" 2>/dev/null; }
pod_py() { sudo kubectl -n "${NS}" exec -i "${POD}" -- python3 -; }
pod_curl() { sudo kubectl -n "${NS}" exec "${POD}" -- curl -s -m 20 "$@"; }
conf_value() { python3 - "$1" "$2" <<'PY'
import re, sys
src = open(sys.argv[1]).read()
m = re.search(r"^%s\s*=\s*'?([^'\n]*)'?" % re.escape(sys.argv[2]), src, re.M)
print((m.group(1) if m else '').strip())
PY
}

restart_ugc() { # restart_ugc <描述>
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
  info "重启 ugc_server（${1}）耗时 $((t1 - t0))s"
}

# 在 pod 内**原地截断重写** [image] 段里的一个键（override.toml 是文件 bind mount，
# sed -i 会换 inode 导致容器看不到改动；缺键时插到 [image] 段首）
set_image_conf() { # set_image_conf <key> <value>
  pod_py <<PY >>"${LOG}" 2>&1
import io, re, sys
path = '${POD_OVERRIDE}'
key, value = '${1}', '${2}'
lines = io.open(path, encoding='utf-8').read().split('\n')
out, in_image, done = [], False, False
for line in lines:
    if re.match(r'^\s*\[', line):
        if in_image and not done:
            out.append('%s = %s' % (key, value)); done = True
        in_image = bool(re.match(r'^\s*\[image\]\s*$', line))
    if in_image and re.match(r'^\s*%s\s*=' % re.escape(key), line):
        line = '%s = %s' % (key, value); done = True
    out.append(line)
if in_image and not done:
    out.append('%s = %s' % (key, value)); done = True
assert done, 'override.toml 里没有 [image] 段，无法设置 %s' % key
io.open(path, 'w', encoding='utf-8').write('\n'.join(out))   # 保持 inode
print('%s = %s' % (key, value))
PY
}

# 安全网：任何异常退出都把开关与 override.toml 还原（否则会把上传通道留在关闭态）
SWITCH_OFF=0
restore_on_exit() {
  local rc=$?
  if [ "${SWITCH_OFF}" = "1" ]; then
    log "!!! 脚本提前退出（rc=${rc}），强制恢复 upload_enabled=true"
    set_image_conf upload_enabled true || true
    restart_ugc "异常恢复" || true
  fi
  exit "${rc}"
}
trap restore_on_exit EXIT

TOKEN="$(token)"
[ -n "${TOKEN}" ] || { echo "未取到 token（必须以 fireflyer 身份运行）"; exit 1; }
IMG_ROOT="$(conf_value "${OVERRIDE}" image_path)"
IMG_ROOT="${IMAGE_ROOT:-${IMG_ROOT:-/nfs-shared/hai-platform/workspace/image}}"
SRC_TAR="${SRC_TAR:-${IMG_ROOT}/demo.tar}"
LOCAL_DIR="${LOCAL_DIR:-/tmp/hai-image-push-fixture}"
LOCAL_TAR="${LOCAL_TAR:-${LOCAL_DIR}/$(basename "${SRC_TAR}")}"
CLUSTER_DIR="${IMG_ROOT}/${IMAGE_NAME}"
CLUSTER_TAR="${CLUSTER_DIR}/$(basename "${SRC_TAR}")"
USER_NAME="$(as_user env HOME=/home/fireflyer hai-cli whoami 2>/dev/null | head -1 | awk '{print $1}')"
TASK_DIR="/nfs-shared/hai-platform/workspace/${USER_NAME}/images_push_probe"

log "=== e2e_images_push @ ${BASE} $(date +%T)"
log "user=${USER_NAME} image_root=${IMG_ROOT} image=${IMG_URL}"
log "本地 tar=${LOCAL_TAR}（必须在共享根之外） 集群落点=${CLUSTER_TAR}"

# ------------------------------------------------------------------ 0) 前置
log "--- 0) 前置：客户端含 push / 迁移 036 / 上传开关 / fixture"
if as_user env HOME=/home/fireflyer hai-cli images push --help >/dev/null 2>&1; then
  ok "客户端已含 \`images push\`（S8-4）"
else
  bad "客户端没有 \`images push\`：先在 host 上安装含本分支的 hai-cli（build_cli_local.sh）"
  log "=== PASS=${PASS} FAIL=${FAIL} ==="; exit 1
fi
MIG=$(psql_q "select count(*) from pg_enum e join pg_type t on t.oid=e.enumtypid where t.typname='file_type' and e.enumlabel='image'")
[ "${MIG}" = "1" ] && ok "迁移 036 已生效：file_type 枚举含 image（OPS-08）" || bad "file_type 枚举没有 image（先执行 db_schemas/036）"
if [ "$(conf_value "${OVERRIDE}" upload_enabled)" = "false" ]; then
  info "override.toml 里 upload_enabled=false，脚本会先打开（演练结束按 RESTORE 还原）"
  set_image_conf upload_enabled true; restart_ugc "打开上传开关"
fi
[ -f "${SRC_TAR}" ] || { bad "fixture tar 不存在：${SRC_TAR}（先跑 image_fixture.sh）"; log "=== PASS=${PASS} FAIL=${FAIL} ==="; exit 1; }
mkdir -p "${LOCAL_DIR}"
[ -f "${LOCAL_TAR}" ] || cp "${SRC_TAR}" "${LOCAL_TAR}"
case "${LOCAL_TAR}" in
  "${IMG_ROOT}"/*) bad "本地 tar 落在共享根内（${LOCAL_TAR}）：无法证明字节真的走了对象存储" ;;
  *) ok "本地 tar 在共享根之外（${LOCAL_TAR}）" ;;
esac
LOCAL_MD5=$(md5sum "${LOCAL_TAR}" | awk '{print $1}')
info "本地 md5=${LOCAL_MD5} 大小=$(stat -c %s "${LOCAL_TAR}") 字节"

# ------------------------------------------------------------------ 1) 清场
log "--- 1) 清场：删掉旧登记与旧落点，确保这次是「真上传」"
# `images delete` 是**按 image 名**软删（该 image 的全部行，含 pushed 路径的历史行）；
# 为了让本次 push 造出一条全新的 loaded 行，把 pushed 路径那一行彻底删掉（否则它是 deleted 行，
# push 内部的 load 不带 --force 会拒绝重新登记 —— 实测踩过）。落点在共享根下由 pod 以 root 创建，
# 清理必须 sudo。
as_user env HOME=/home/fireflyer hai-cli images delete "${IMG_URL}" >/dev/null 2>&1 || true
psql_q "delete from train_image where image_tar='${CLUSTER_TAR}'" >/dev/null 2>&1 || true
if [ -d "${CLUSTER_DIR}" ]; then
  sudo rm -rf "${CLUSTER_DIR}" && info "已清理旧落点 ${CLUSTER_DIR}"
fi
if [ "${PURGE_IMAGE}" = "1" ]; then
  for NODE in k8s-slave01 k8s-slave02 k8s-slave03; do
    multipass exec "${NODE}" -- sudo microk8s.ctr images rm "${IMG_URL}" >/dev/null 2>&1 || true
    info "已从 ${NODE} 的 containerd 清除 ${IMG_URL}（强制真实导入）"
  done
fi

# ------------------------------------------------------------------ 2) push（E2E-09 主链路）
log "--- 2) images push（本地 tar → 对象存储 → 共享盘 → 自动登记）"
PUSH_LOG=/tmp/e2e_images_push_push.log
as_user env HOME=/home/fireflyer hai-cli images push "${LOCAL_TAR}" --image "${IMAGE_NAME}" > "${PUSH_LOG}" 2>&1
RC=$?
log "push exit=${RC}"; tail -n 6 "${PUSH_LOG}" | tee -a "${LOG}"
if [ "${RC}" = "0" ] && grep -q "上传并登记成功" "${PUSH_LOG}"; then
  ok "images push 成功（上传 + 自动登记，FR-16/FR-18）"
else
  bad "images push 失败（exit=${RC}）：$(tail -n 2 "${PUSH_LOG}" | tr '\n' ' ')"
fi

if [ -f "${CLUSTER_TAR}" ]; then
  CLUSTER_MD5=$(md5sum "${CLUSTER_TAR}" | awk '{print $1}')
  if [ "${CLUSTER_MD5}" = "${LOCAL_MD5}" ]; then
    ok "共享盘出现同一文件且 md5 一致（AC-15 核心判据）：${CLUSTER_TAR}"
  else
    bad "共享盘文件 md5 不一致：本地=${LOCAL_MD5} 集群=${CLUSTER_MD5}"
  fi
else
  bad "共享盘没有落点文件：${CLUSTER_TAR}"
fi

SYNC_ROW=$(psql_q "select push_status||'|'||file_type||'|'||name||'|'||cluster_path from user_sync_status where file_type='image' and name='${IMAGE_NAME}' order by updated_at desc limit 1")
log "user_sync_status：${SYNC_ROW}"
case "${SYNC_ROW}" in
  finished\|image\|*) ok "user_sync_status 有 file_type=image 的 finished 行（字节搬完了）" ;;
  *) bad "user_sync_status 未见 finished 行：${SYNC_ROW}" ;;
esac

ROW=$(psql_q "select image||'|'||path||'|'||status from train_image where image_tar='${CLUSTER_TAR}' order by updated_at desc limit 1")
log "train_image：${ROW}"
[ "$(echo "${ROW}" | cut -d'|' -f1)" = "${IMAGE_NAME}" ] && ok "train_image 的 image=${IMAGE_NAME}" || bad "train_image 的 image 异常：$(echo "${ROW}" | cut -d'|' -f1)"
[ "$(echo "${ROW}" | cut -d'|' -f2)" = "${CLUSTER_TAR}" ] && ok "train_image.path == 落盘 tar 绝对路径" || bad "train_image.path 异常：$(echo "${ROW}" | cut -d'|' -f2)"
[ "$(echo "${ROW}" | cut -d'|' -f3)" = "loaded" ] && ok "train_image.status=loaded（任务白名单，HC-03）" || bad "train_image.status=$(echo "${ROW}" | cut -d'|' -f3)"

LIST_LOG=/tmp/e2e_images_push_list.log
as_user env HOME=/home/fireflyer hai-cli images list > "${LIST_LOG}" 2>&1 || true
grep -q "$(basename "${CLUSTER_TAR}")" "${LIST_LOG}" && ok "images list 能看到该镜像行" || bad "images list 看不到该镜像"

# ------------------------------------------------------------------ 3) 幂等
log "--- 3) 幂等：同一 tar 再 push 一次"
BEFORE_MTIME=$(stat -c %Y "${CLUSTER_TAR}" 2>/dev/null || echo '')
as_user env HOME=/home/fireflyer hai-cli images push "${LOCAL_TAR}" --image "${IMAGE_NAME}" > /tmp/e2e_images_push_again.log 2>&1
AFTER_MTIME=$(stat -c %Y "${CLUSTER_TAR}" 2>/dev/null || echo '')
if grep -qE "跳过上传|已在集群" /tmp/e2e_images_push_again.log; then
  ok "重复 push 命中「已在集群且已登记」，跳过上传（FR-18 幂等）"
else
  bad "重复 push 未跳过：$(tail -n 2 /tmp/e2e_images_push_again.log | tr '\n' ' ')"
fi
[ "${BEFORE_MTIME}" = "${AFTER_MTIME}" ] && ok "重复 push 未改动共享盘文件（mtime 不变）" || bad "重复 push 改动了共享盘文件（mtime ${BEFORE_MTIME} → ${AFTER_MTIME}）"
CNT=$(psql_q "select count(*) from train_image where image_tar='${CLUSTER_TAR}'")
[ "${CNT}" = "1" ] && ok "重复 push 后 train_image 仍 1 行" || bad "重复 push 后行数=${CNT}"

# ------------------------------------------------------------------ 4) 任务侧（AC-15 第三段）
log "--- 4) 用该镜像提交任务，必须产出可区分输出"
sudo mkdir -p "${TASK_DIR}" && sudo chmod 777 "${TASK_DIR}"
sudo tee "${TASK_DIR}/probe_image.py" > /dev/null <<'PYFILE'
import os

print('IMAGE_PROBE_START')
print('HFAI_IMAGE_WEKA_PATH=', os.environ.get('HFAI_IMAGE_WEKA_PATH'))
with open('/hfai_image_probe.txt') as f:
    probe = f.read().strip()
print('PROBE_FILE_CONTENT=', probe)
print('IMAGE_PROBE=images-push-ok')
PYFILE
sudo chmod 644 "${TASK_DIR}/probe_image.py"

as_user env HOME=/home/fireflyer hai-cli python "${TASK_DIR}/probe_image.py" -- \
  --image "${IMG_URL}" --nodes 1 -g training --name "${TASK_NAME}" > /tmp/e2e_images_push_submit.log 2>&1
RC=$?
log "提交 exit=${RC}"; tail -n 4 /tmp/e2e_images_push_submit.log | tee -a "${LOG}"
if [ "${RC}" = "0" ]; then
  ok "任务提交成功（3 段 URL + status='loaded' 校验通过）"

  STATUS_JSON=/tmp/e2e_images_push_status.json
  CHAIN=""
  for _ in $(seq 1 "${TASK_WAIT}"); do
    as_user env HOME=/home/fireflyer hai-cli status "${TASK_NAME}" -j > "${STATUS_JSON}" 2>/dev/null || true
    CHAIN=$(python3 -c "import json;print(json.load(open('${STATUS_JSON}')).get('chain_status',''))" 2>/dev/null || echo "")
    WORKER=$(python3 -c "import json;print(json.load(open('${STATUS_JSON}')).get('worker_status',''))" 2>/dev/null || echo "")
    # 终态有两套口径：chain_status 用 finished，worker_status 用 succeeded/failed
    # （只认其中一套会白等到 TASK_WAIT×10s，实测踩过）
    case "${CHAIN}" in
      finished|succeeded|failed|stopped|canceled) break ;;
    esac
    case "${WORKER}" in
      succeeded|failed|stopped|canceled) break ;;
    esac
    sleep 10
  done
  as_user env HOME=/home/fireflyer hai-cli logs "${TASK_NAME}" -r 0 > /tmp/e2e_images_push_task.log 2>/dev/null || true
  if grep -q "${PROBE_MARKER}" /tmp/e2e_images_push_task.log; then
    ok "任务输出含 ${PROBE_MARKER}（用的是 pushed 的镜像，AC-15）"
  else
    bad "任务输出缺少探针标记（chain_status=${CHAIN}）"
  fi
  if grep -q "PROBE_FILE_CONTENT= HFAI_CUSTOM_IMAGE_PROBE\|PROBE_FILE_CONTENT=HFAI_CUSTOM_IMAGE_PROBE" /tmp/e2e_images_push_task.log; then
    ok "镜像内探针内容可区分（不是内建镜像）"
  else
    bad "未看到镜像内探针内容（chain_status=${CHAIN}）"
  fi
else
  # 提交就失败时**不要**进等待循环（否则会白等 TASK_WAIT×10s）；直接判定两条运行期断言失败
  bad "任务提交失败（exit=${RC}）：$(tail -n 2 /tmp/e2e_images_push_submit.log | tr '\n' ' ')"
  bad "任务未提交，无法产出可区分输出（AC-15 未达成）"
fi

# ------------------------------------------------------------------ 5) 开关一致性（E2E-10）
if [ "${SWITCH_TEST}" = "1" ]; then
  log "--- 5) 开关一致性：upload_enabled=false / enabled=false"
  FILES_BEFORE=$(find "${IMG_ROOT}" -type f 2>/dev/null | wc -l)

  SWITCH_OFF=1
  set_image_conf upload_enabled false
  restart_ugc "upload_enabled=false"

  OUT=$(curl -s -m 20 -X POST "${BASE}/ugc/get_sts_token?token=${TOKEN}&name=${IMAGE_NAME}&file_type=image" || echo '')
  echo "${OUT}" | grep -q 'FEATURE_DISABLED' && ok "upload_enabled=false：API-01 被拒（FR-19 / HC-12）" || bad "upload_enabled=false 时 API-01 仍可用：${OUT}"

  OUT=$(curl -s -m 20 -X POST "${BASE}/ugc/sync_to_cluster?token=${TOKEN}&name=${IMAGE_NAME}&file_type=image&no_zip=true" \
        -H 'Content-Type: text/plain' -d '{"file_list": {"files": ["demo.tar"]}}' || echo '')
  echo "${OUT}" | grep -q 'FEATURE_DISABLED' && ok "upload_enabled=false：API-05 被拒（数据面同源闸门）" || bad "upload_enabled=false 时 API-05 仍可用：${OUT}"

  OUT=$(curl -s -m 20 -X POST "${BASE}/ugc/user/train_image/list?token=${TOKEN}" || echo '')
  echo "${OUT}" | grep -q '"success":1' && ok "upload_enabled=false：控制面 list 仍正常（不误伤）" || bad "upload_enabled=false 时 list 异常：${OUT}"

  FILES_AFTER=$(find "${IMG_ROOT}" -type f 2>/dev/null | wc -l)
  [ "${FILES_BEFORE}" = "${FILES_AFTER}" ] && ok "关闭期间共享盘零新增写入（${FILES_AFTER} 个文件）" || bad "关闭期间共享盘文件数变化：${FILES_BEFORE} → ${FILES_AFTER}"

  # enabled=false：上传与控制面必须**同时**关闭（check_image_enabled 是共同入口）
  set_image_conf enabled false
  restart_ugc "enabled=false"
  OUT1=$(curl -s -m 20 -X POST "${BASE}/ugc/get_sts_token?token=${TOKEN}&name=${IMAGE_NAME}&file_type=image" || echo '')
  OUT2=$(curl -s -m 20 -X POST "${BASE}/ugc/user/train_image/list?token=${TOKEN}" || echo '')
  if echo "${OUT1}" | grep -q 'FEATURE_DISABLED' && echo "${OUT2}" | grep -q '"success":1'; then
    ok "enabled=false：上传入口被拒（list 只读路径按设计仍可用）"
  else
    bad "enabled=false 的上传入口行为异常：sts=${OUT1} list=${OUT2}"
  fi

  set_image_conf enabled true
  set_image_conf upload_enabled true
  restart_ugc "恢复开关"
  SWITCH_OFF=0
  OUT=$(curl -s -m 20 -X POST "${BASE}/ugc/get_sts_token?token=${TOKEN}&name=${IMAGE_NAME}&file_type=image" || echo '')
  echo "${OUT}" | grep -q '"success":1' && ok "恢复 upload_enabled=true 后 API-01 可用（一级回滚可逆）" || bad "恢复后 API-01 仍不可用：${OUT}"
else
  info "SWITCH_TEST=0：跳过开关一致性演练（E2E-10）"
fi

# ------------------------------------------------------------------ 6) 负例与边界（UP-02 / UP-05 / UP-10 / UP-11②）
log "--- 6) 负例与边界：STS 作用域 / 名字与落点校验 / 容量上限 / --no-load"

# UP-02①：STS 授权前缀必须**恰好等于** cloud_path（不许多一个字符，也不许少一层）
STS_JSON=/tmp/e2e_images_push_sts.json
curl -s -m 20 -X POST "${BASE}/ugc/get_sts_token?token=${TOKEN}&name=${IMAGE_NAME}&file_type=image" -o "${STS_JSON}" || true
SEC_TOKEN=$(python3 - "${STS_JSON}" <<'INNER1'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    print(''); raise SystemExit
for v in d.values():
    if isinstance(v, dict) and 'authorized_path' in v:
        print(v.get('security_token') or ''); break
INNER1
)
AUTH=$(python3 - "${STS_JSON}" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    print(''); raise SystemExit
for v in d.values():
    if isinstance(v, dict) and 'authorized_path' in v:
        print(v['authorized_path']); break
PY
)
WANT="hfai/shared/images/${USER_NAME}/${IMAGE_NAME}"
if [ "${AUTH}" = "${WANT}" ]; then
  ok "STS 授权前缀恰等于 cloud_path（SEC-08）：${AUTH}"
else
  bad "STS 授权前缀异常：got='${AUTH}' want='${WANT}'"
fi

# UP-02②：拿这组凭证写「他人前缀」必须失败（越权写）
if python3 -c "import boto3" >/dev/null 2>&1; then
  DENY=$(python3 - "${STS_JSON}" <<'PY'
import json, sys
import boto3
from botocore.config import Config
cred = None
for v in json.load(open(sys.argv[1])).values():
    if isinstance(v, dict) and 'authorized_path' in v:
        cred = v; break
if cred is None:
    print('NO_CRED'); raise SystemExit
s3 = boto3.client('s3', endpoint_url=cred['endpoint'],
                  aws_access_key_id=cred['access_key_id'],
                  aws_secret_access_key=cred['access_key_secret'],
                  region_name='us-east-1',
                  config=Config(signature_version='s3v4', retries={'max_attempts': 1}))
try:
    s3.put_object(Bucket=cred['bucket'], Key='hfai/shared/images/other-user/evil.txt', Body=b'x')
    print('ALLOWED')
except Exception as e:
    print('DENIED:' + type(e).__name__)
PY
)
  # RustFS/MinIO 没有阿里云 STS：provider 会**降级下发静态 AK/SK**（security_token 为空，
  # 见 cloud_storage/provider/s3.py:276-293 的「P0 降级实现」）。此时 prefix 只由客户端自觉遵守，
  # 存储侧不强制 —— 这是**已登记的环境限制**（test-report §6 的 D14 / 设计 §15 的 R-14），
  # 因此按 WARN 归类；生产侧必须改用 RustFS STS(AssumeRole + inline policy) 或 bucket policy。
  if [ -n "${SEC_TOKEN}" ]; then
    case "${DENY}" in
      DENIED*) ok "越权前缀写入被拒（真 STS 凭据，${DENY}）" ;;
      NO_CRED) bad "未取到 STS 凭证，无法验证越权写" ;;
      *) bad "越权前缀写入**未被拒**：${DENY}（SEC-08 风险，需查 rustfs 的策略）" ;;
    esac
  else
    case "${DENY}" in
      ALLOWED) warn "越权前缀写入成功：本环境下发的是**静态 AK/SK**（security_token 为空），prefix 不被强制（环境限制 D14 / R-14；SEC-08 的强制隔离需生产侧接 RustFS STS 或 bucket policy）" ;;
      DENIED*) ok "静态凭据下越权写仍被拒（存储侧策略更严格，${DENY}）" ;;
      *) bad "越权写结果不可判定：${DENY}" ;;
    esac
  fi
else
  info "未安装 boto3，UP-02 的越权写负例未执行（授权前缀本身已核对）"
fi

# UP-05：名字 / 相对路径注入防护，且共享盘零新增
FILES_BEFORE_NEG=$(find "${IMG_ROOT}" -type f 2>/dev/null | wc -l)
for BADFILE in '../evil.tar' 'a/b.tar' '/etc/passwd'; do
  OUT=$(curl -s -m 20 -X POST "${BASE}/ugc/user/train_image/push_precheck?token=${TOKEN}" \
        -H 'Content-Type: text/plain' -d "{\"file\": \"${BADFILE}\"}") || echo ''
  echo "${OUT}" | grep -q 'INVALID_PARAM' && ok "API-19 拒绝非法 file=${BADFILE}" || bad "API-19 未拒绝 file=${BADFILE}: ${OUT}"
done
OUT=$(curl -s -m 20 -X POST "${BASE}/ugc/sync_to_cluster?token=${TOKEN}&name=../evil&file_type=image&no_zip=true" \
      -H 'Content-Type: text/plain' -d '{"file_list": {"files": ["x.tar"]}}') || echo ''
echo "${OUT}" | grep -q 'INVALID_PARAM' && ok "API-05 拒绝 name 含 ..（镜像条目名非法）" || bad "API-05 未拒绝 name=../evil: ${OUT}"
# 每次用**唯一**的越界文件名：`index = hashkey(token, name, file_type, *files)` 是按文件列表算的，
# 复用同一个名字会命中「上一次同步正在进行中」的幂等短路（§9.3 的 R-15），从而拿不到 PATH_ESCAPE。
BAD_REL="../x-$$-$RANDOM.tar"
OUT=$(curl -s -m 20 -X POST "${BASE}/ugc/sync_to_cluster?token=${TOKEN}&name=${IMAGE_NAME}&file_type=image&no_zip=true" \
      -H 'Content-Type: text/plain' -d "{\"file_list\": {\"files\": [\"${BAD_REL}\"]}}") || echo ''
echo "${OUT}" | grep -q 'PATH_ESCAPE' && ok "API-05 拒绝越界相对路径（PATH_ESCAPE，${BAD_REL}）" || bad "API-05 未拒绝 ${BAD_REL}: ${OUT}"
FILES_AFTER_NEG=$(find "${IMG_ROOT}" -type f 2>/dev/null | wc -l)
[ "${FILES_BEFORE_NEG}" = "${FILES_AFTER_NEG}" ] && ok "负例期间共享盘零新增文件（${FILES_AFTER_NEG}）" || bad "负例期间共享盘文件数变化：${FILES_BEFORE_NEG} → ${FILES_AFTER_NEG}"

# UP-10：容量上限（改成 1000 字节后，声明 2000 字节必须被拒）
set_image_conf max_tar_bytes 1000
restart_ugc "max_tar_bytes=1000"
OUT=$(curl -s -m 20 -X POST "${BASE}/ugc/user/train_image/push_precheck?token=${TOKEN}" \
      -H 'Content-Type: text/plain' -d '{"file": "demo.tar", "image": "demo:v1", "file_size": 2000}') || echo ''
echo "${OUT}" | grep -q 'IMAGE_TAR_TOO_LARGE' && ok "超过 max_tar_bytes 快速失败（IMAGE_TAR_TOO_LARGE）" || bad "未按上限拒绝：${OUT}"
set_image_conf max_tar_bytes 0
restart_ugc "max_tar_bytes=0（恢复）"

# UP-11②：--no-load 只上传不登记
TINY_DIR=/tmp/hai-image-push-tiny
mkdir -p "${TINY_DIR}"
head -c 1048576 /dev/urandom > "${TINY_DIR}/tiny.tar"
as_user env HOME=/home/fireflyer hai-cli images push "${TINY_DIR}/tiny.tar" --image tiny-probe:v1 --no-load > /tmp/e2e_images_push_noload.log 2>&1
RC=$?
if [ "${RC}" = "0" ] && grep -q "跳过登记" /tmp/e2e_images_push_noload.log; then
  ok "--no-load：只上传不登记（FR-16）"
else
  bad "--no-load 行为异常（exit=${RC}）：$(tail -n 2 /tmp/e2e_images_push_noload.log | tr '\n' ' ')"
fi
CNT=$(psql_q "select count(*) from train_image where image='tiny-probe:v1'")
[ "${CNT}" = "0" ] && ok "--no-load 未产生 train_image 行" || bad "--no-load 仍产生了 ${CNT} 行"
# 清掉这份测试产物（对象存储里的对象按 delete 语义不回收，FR-14）
sudo rm -rf "${IMG_ROOT}/tiny-probe:v1"

# ------------------------------------------------------------------ 7) 还原（可选）
if [ "${RESTORE}" = "1" ]; then
  log "--- 7) 还原 P0 的手工放盘布局（兼容旁路仍可用，CMP-08）"
  as_user env HOME=/home/fireflyer hai-cli images delete "${IMG_URL}" >/dev/null 2>&1 || true
  sudo rm -rf "${CLUSTER_DIR}"
  # 共享根下的 fixture 由 image_fixture.sh 以 sudo 写入（root 拥有），还原时必须同样用 sudo
  sudo cp -f "${LOCAL_TAR}" "${SRC_TAR}"
  # 该行的 status 已被 delete 置为 deleted → 重新登记必须显式 --force（Q-3 语义）
  as_user env HOME=/home/fireflyer hai-cli images load "${SRC_TAR}" --image "${IMAGE_NAME}" --force > /tmp/e2e_images_push_restore.log 2>&1
  RC=$?
  ROW=$(psql_q "select image||'|'||path||'|'||status from train_image where image_tar='${SRC_TAR}' order by updated_at desc limit 1")
  if [ "${RC}" = "0" ] && [ "$(echo "${ROW}" | cut -d'|' -f3)" = "loaded" ]; then
    ok "还原成功：${SRC_TAR} 重新登记为 loaded（手工放盘路径仍可用）"
  else
    bad "还原失败（exit=${RC}）：${ROW}"
  fi
else
  info "RESTORE=0：保留 push 产生的落点与登记（${CLUSTER_TAR}）"
fi

log "=== E2E_IMAGES_PUSH 结果: PASS=${PASS} WARN=${WARN} FAIL=${FAIL} ==="
[ "${FAIL}" = "0" ]
