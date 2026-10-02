#!/usr/bin/env bash
#
# hai-cli images（用户自定义镜像）—— L3 端到端：控制面 → 提交面 → 运行面全链路
#
# 对应用例：docs/haiplatform/images/images-server-test-cases.md §5 E2E-01~E2E-04
#           docs/haiplatform/images/images-server-checklist.md §6（E2E-01~E2E-07）
#           docs/haiplatform/images/images-server-requirements.md §6（AC-01/AC-03/AC-04/AC-08/AC-09）
#
# ⚠️ 判据不是「接口 200」：必须用自定义镜像跑通一个真实任务，且**输出由镜像内容决定**
#    （夹具镜像里预置 /hfai_image_probe.txt，任务日志里能看到它的内容）。
#
# 用法（host 103）:
#   bash e2e_images.sh [base_url]
# 可用环境变量：IMAGE_ROOT / TAR / IMAGE_NAME / TASK_NAME / IMG_URL / NS
#
set -uo pipefail

BASE="${1:-http://10.205.52.200}"
NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
CONF="${CONF:-/home/fireflyer/.hfai/conf.yml}"
OVERRIDE="${OVERRIDE:-/nfs-shared/hai-platform/override.toml}"
REGISTRY="${REGISTRY:-registry.high-flyer.cn}"
GRP="${GRP:-hfai}"
IMAGE_NAME="${IMAGE_NAME:-demo:v1}"
IMG_URL="${IMG_URL:-${REGISTRY}/${GRP}/${IMAGE_NAME}}"
TASK_NAME="${TASK_NAME:-images_e2e_probe}"
TASK_WAIT="${TASK_WAIT:-120}"         # 轮询次数（每次 10s）；首次导入大 tar 可能要几分钟
# E2E_PURGE_IMAGE=1：提交前把镜像从三个节点的 containerd 里删掉，强制走一次真实导入
# （用于验证「长 init 不会被 unschedulable 看门狗打断」这一修复；默认 0 走幂等短路）
E2E_PURGE_IMAGE="${E2E_PURGE_IMAGE:-0}"
PROBE_MARKER="${PROBE_MARKER:-IMAGE_PROBE=images-load-ok}"

PASS=0; FAIL=0
LOG=/tmp/e2e_images.log
: > "${LOG}"
log() { echo "$*" | tee -a "${LOG}"; }
ok()  { log "PASS | $*"; PASS=$((PASS + 1)); }
bad() { log "FAIL | $*"; FAIL=$((FAIL + 1)); }
info(){ log "INFO | $*"; }

as_user() { if [ "$(id -un)" = "fireflyer" ]; then "$@"; else sudo -u fireflyer "$@"; fi; }
token() { grep -E '^ *token:' "${CONF}" 2>/dev/null | awk '{print $2}'; }
psql_q() { sudo kubectl -n "${NS}" exec "${POD}" -- psql -U root -d mars_db -tAc "$1" 2>/dev/null; }
conf_value() { python3 - "$1" "$2" <<'PY'
import re, sys
src = open(sys.argv[1]).read()
m = re.search(r"^%s\s*=\s*'([^']*)'" % re.escape(sys.argv[2]), src, re.M)
print(m.group(1) if m else '')
PY
}

TOKEN="$(token)"
[ -n "${TOKEN}" ] || { echo "未取到 token（必须以 fireflyer 身份运行）"; exit 1; }
IMG_ROOT="$(conf_value "${OVERRIDE}" image_path)"
IMG_ROOT="${IMAGE_ROOT:-${IMG_ROOT:-/nfs-shared/hai-platform/workspace/image}}"
TAR="${TAR:-${IMG_ROOT}/demo.tar}"
USER_NAME="$(as_user env HOME=/home/fireflyer hai-cli whoami 2>/dev/null | head -1 | awk '{print $1}')"
TASK_DIR="/nfs-shared/hai-platform/workspace/${USER_NAME}/images_e2e_probe"

log "=== e2e_images @ ${BASE} $(date +%T)"
log "user=${USER_NAME} image_root=${IMG_ROOT} tar=${TAR} image=${IMG_URL}"

# ------------------------------------------------------------------ 0) 前置
log "--- 0) 前置：无内网 registry + 共享根 + 节点前置 + 后端"
if [ "$(sudo kubectl get svc -A 2>/dev/null | grep -icE 'registry|:5000')" = "0" ]; then
  ok "集群内无 registry service（I11：register 后端是唯一可端到端路径）"
else
  info "集群内存在 registry service，本环境不再是「无 registry」前提"
fi
if getent hosts "${REGISTRY}" >/dev/null 2>&1; then
  info "getent ${REGISTRY} -> $(getent hosts "${REGISTRY}" | awk '{print $1}')（不可达即可）"
else
  ok "getent 解析不到 ${REGISTRY}"
fi
[ -f "${TAR}" ] && ok "测试 tar 存在：${TAR}" || { bad "测试 tar 不存在（先跑 image_fixture.sh）"; log "=== PASS=${PASS} FAIL=${FAIL} ==="; exit 1; }
[ "$(conf_value "${OVERRIDE}" loader_backend)" = "register" ] && ok "loader_backend=register（P0 主线）" || bad "loader_backend 不是 register"
for NODE in k8s-slave01 k8s-slave02 k8s-slave03; do
  multipass exec "${NODE}" -- test -d /data_local 2>/dev/null && ok "${NODE}:/data_local 就绪" || bad "${NODE}:/data_local 缺失（I17①）"
done
[ "$(conf_value "${OVERRIDE}" load_helper_image)" = "docker.io/library/busybox:latest" ] \
  && ok "load_helper_image 为节点已有的 busybox（I17②）" || bad "load_helper_image 未改为节点已有镜像"

# ------------------------------------------------------------------ 1) 清场 + load
log "--- 1) 清场并 load（显式 --image ${IMAGE_NAME}）"
psql_q "delete from train_image where image_tar='${TAR}'" >/dev/null 2>&1 || true
as_user env HOME=/home/fireflyer hai-cli images delete "${IMG_URL}" >/dev/null 2>&1 || true
if [ "${E2E_PURGE_IMAGE}" = "1" ]; then
  log "--- 1.0) 强制重新导入：从三个节点的 containerd 删除 ${IMG_URL}"
  for NODE in k8s-slave01 k8s-slave02 k8s-slave03; do
    # 注意：microk8s.ctr 包装器**自带** -n k8s.io，再传一次会报 "Cannot use two forms of the same flag"
    OUT=$(multipass exec "${NODE}" -- sudo microk8s.ctr images rm "${IMG_URL}" 2>&1)
    if multipass exec "${NODE}" -- sudo microk8s.ctr images ls -q 2>/dev/null | grep -qx "${IMG_URL}"; then
      bad "${NODE}: 镜像仍在（purge 失败：${OUT}）"
    else
      ok "${NODE}: 镜像已清除"
    fi
  done
fi

LOAD_LOG=/tmp/e2e_images_load.log
as_user env HOME=/home/fireflyer hai-cli images load "${TAR}" --image "${IMAGE_NAME}" > "${LOAD_LOG}" 2>&1
RC=$?
log "load exit=${RC}"; tail -n 3 "${LOAD_LOG}" | tee -a "${LOG}"
if [ "${RC}" = "0" ] && grep -q "已登记" "${LOAD_LOG}"; then ok "images load 成功"; else bad "images load 失败（exit=${RC}）"; fi
ROW=$(psql_q "select image||'|'||path||'|'||status||'|'||task_id from train_image where image_tar='${TAR}' order by updated_at desc limit 1")
log "DB 行：${ROW}"
[ "$(echo "${ROW}" | cut -d'|' -f1)" = "${IMAGE_NAME}" ] && ok "image 落库为 ${IMAGE_NAME}" || bad "image 落库异常：$(echo "${ROW}" | cut -d'|' -f1)"
[ "$(echo "${ROW}" | cut -d'|' -f2)" = "${TAR}" ] && ok "path == image_tar（register 后端契约）" || bad "path != image_tar"
[ "$(echo "${ROW}" | cut -d'|' -f3)" = "loaded" ] && ok "status=loaded（任务白名单，HC-03）" || bad "status=$(echo "${ROW}" | cut -d'|' -f3)"

# ------------------------------------------------------------------ 2) list 可见 + 幂等
log "--- 2) images list 可见 + 重复 load 幂等"
LIST_LOG=/tmp/e2e_images_list.log
as_user env HOME=/home/fireflyer hai-cli images list > "${LIST_LOG}" 2>&1 || true
# 客户端表格会截断长镜像名（实测显示为 registry.high-fly…），因此用 basename(image_tar) 判定
if grep -q "$(basename "${TAR}")" "${LIST_LOG}"; then
  ok "images list 能看到该镜像行（修 I2）"
else
  bad "images list 看不到该镜像（I2 未修）"; tail -n 5 "${LIST_LOG}" | tee -a "${LOG}"
fi
as_user env HOME=/home/fireflyer hai-cli images load "${TAR}" --image "${IMAGE_NAME}" >/dev/null 2>&1 || true
CNT=$(psql_q "select count(*) from train_image where image_tar='${TAR}'")
[ "${CNT}" = "1" ] && ok "重复 load 后仍 1 行（幂等，AC-05）" || bad "重复 load 后行数=${CNT}"

# ------------------------------------------------------------------ 3) 提交自定义镜像任务
log "--- 3) 用自定义镜像提交任务（探针读取镜像内文件）"
sudo mkdir -p "${TASK_DIR}" && sudo chmod 777 "${TASK_DIR}"
sudo tee "${TASK_DIR}/probe_image.py" > /dev/null <<'PYFILE'
import os

print('IMAGE_PROBE_START')
print('HFAI_IMAGE=', os.environ.get('HFAI_IMAGE'))
print('HFAI_IMAGE_WEKA_PATH=', os.environ.get('HFAI_IMAGE_WEKA_PATH'))
print('CONTAINER_HOSTNAME=', os.uname().nodename)
with open('/hfai_image_probe.txt') as f:
    probe = f.read().strip()
print('PROBE_FILE_CONTENT=', probe)
print('IMAGE_PROBE=images-load-ok')
PYFILE
sudo chmod 644 "${TASK_DIR}/probe_image.py"

as_user env HOME=/home/fireflyer hai-cli python "${TASK_DIR}/probe_image.py" -- \
  --image "${IMG_URL}" --nodes 1 -g training --name "${TASK_NAME}" > /tmp/e2e_images_submit.log 2>&1
RC=$?
log "提交 exit=${RC}"; tail -n 5 /tmp/e2e_images_submit.log | tee -a "${LOG}"
if [ "${RC}" = "0" ]; then ok "任务提交成功（3 段 URL + status='loaded' 校验通过，K1~K3）"; else bad "任务提交失败（exit=${RC}）"; fi

STATUS_JSON=/tmp/e2e_images_status.json
INIT_LOG=/tmp/e2e_images_init.log
: > "${INIT_LOG}"
rm -f /tmp/e2e_images_init_env.txt /tmp/e2e_images_init_image.txt
CHAIN=""
FIRST_TASK_ID=""
for i in $(seq 1 "${TASK_WAIT}"); do
  as_user env HOME=/home/fireflyer hai-cli status "${TASK_NAME}" -j > "${STATUS_JSON}" 2>/dev/null || true
  CHAIN=$(python3 -c "import json;print(json.load(open('${STATUS_JSON}')).get('chain_status',''))" 2>/dev/null || echo "")
  CUR_ID=$(python3 -c "import json;print(json.load(open('${STATUS_JSON}')).get('id',''))" 2>/dev/null || echo "")
  [ -z "${FIRST_TASK_ID}" ] && FIRST_TASK_ID="${CUR_ID}"
  # 任务 pod 生命周期很短（结束即删除），必须在运行期抓 initContainer 的证据
  POD=$(sudo kubectl -n "${NS}" get pods --no-headers 2>/dev/null | grep -E "^${USER_NAME}-[0-9]+-0 " | awk '{print $1}' | head -1)
  if [ -n "${POD}" ]; then
    INIT_NAME=$(sudo kubectl -n "${NS}" get pod "${POD}" -o jsonpath='{.spec.initContainers[0].name}' 2>/dev/null)
    sudo kubectl -n "${NS}" logs "${POD}" -c "${INIT_NAME}" >> "${INIT_LOG}" 2>/dev/null || true
    sudo kubectl -n "${NS}" get pod "${POD}" -o jsonpath='{.spec.initContainers[0].env}' > /tmp/e2e_images_init_env.txt 2>/dev/null || true
    sudo kubectl -n "${NS}" get pod "${POD}" -o jsonpath='{.spec.initContainers[0].image}' > /tmp/e2e_images_init_image.txt 2>/dev/null || true
  fi
  log "  [$i] chain_status=${CHAIN} task_id=${CUR_ID} pod=${POD:-none}"
  case "${CHAIN}" in finished|failed|stopped) break ;; esac
  sleep 10
done
TASK_ID=$(python3 -c "import json;print(json.load(open('${STATUS_JSON}')).get('id',''))" 2>/dev/null || echo "")
POD_STATUS=$(python3 -c "
import json
d=json.load(open('${STATUS_JSON}'))
pods=d.get('_pods_') or []
print(pods[0]['status'] if pods else 'no-pod')
" 2>/dev/null || echo unknown)
log "task_id=${TASK_ID} chain_status=${CHAIN} pod_status=${POD_STATUS}"
[ "${POD_STATUS}" = "succeeded" ] && ok "任务 pod succeeded（AC-01 前提）" || bad "任务 pod 状态 ${POD_STATUS}"

# ------------------------------------------------------------------ 4) 日志：可区分输出
log "--- 4) 任务日志必须出现镜像内的探针内容（AC-01 的唯一判据）"
as_user env HOME=/home/fireflyer hai-cli logs "${TASK_NAME}" > /tmp/e2e_images_task.log 2>&1 || true
grep -E "IMAGE_PROBE|HFAI_IMAGE|PROBE_FILE_CONTENT" /tmp/e2e_images_task.log | tail -n 8 | tee -a "${LOG}"
if grep -q "${PROBE_MARKER}" /tmp/e2e_images_task.log; then ok "任务输出含 ${PROBE_MARKER}（自定义镜像真的跑起来了）"; else bad "任务输出缺少探针标记（可能仍用了内建镜像）"; fi
if grep -q "PROBE_FILE_CONTENT= HFAI_CUSTOM_IMAGE_PROBE\|PROBE_FILE_CONTENT=HFAI_CUSTOM_IMAGE_PROBE" /tmp/e2e_images_task.log; then
  ok "输出内容由镜像内容决定（/hfai_image_probe.txt）"
else
  bad "未读到镜像内探针文件内容"
fi
# 说明：HFAI_IMAGE / HFAI_IMAGE_WEKA_PATH 注入的是 **manager（进而 initContainer）**，
# 不是计算容器本身；计算容器里的证据是「镜像内容」（上一断言），env 证据在第 5 步校验。
if grep -q "HFAI_IMAGE_WEKA_PATH= None" /tmp/e2e_images_task.log; then
  info "计算容器内看不到 HFAI_IMAGE*（属预期：它们只注入 manager/initContainer）"
fi

# ------------------------------------------------------------------ 5) initContainer（运行面证据）
log "--- 5) 运行面证据：load-image initContainer（I16/HC-08/E2E-07）"
INIT_IMG=$(cat /tmp/e2e_images_init_image.txt 2>/dev/null)
info "initContainer image=${INIT_IMG:-未知}"
case "${INIT_IMG}" in
  *busybox*) ok "initContainer 使用节点已有 busybox（不再 ImagePullBackOff）" ;;
  *registry.high-flyer.cn*) bad "initContainer 仍指向不可达的内网镜像：${INIT_IMG}" ;;
  *) [ -n "${INIT_IMG}" ] && info "initContainer 使用 ${INIT_IMG}" ;;
esac
if grep -qE "OK: ${IMG_URL}|已存在，跳过: ${IMG_URL}" "${INIT_LOG}" 2>/dev/null; then
  ok "initContainer 成功完成 link（可见「导入」或「已存在，跳过」，AC-08）"
else
  bad "未抓到 initContainer 成功日志（见 ${INIT_LOG}）"
  tail -n 5 "${INIT_LOG}" | tee -a "${LOG}"
fi
if grep -q "${TAR}" /tmp/e2e_images_init_env.txt 2>/dev/null; then
  ok "initContainer env 的 HFAI_IMAGE_WEKA_PATH == train_image.path（E2E-07）"
else
  bad "initContainer env 与表中 path 不一致"
fi
if [ -n "${FIRST_TASK_ID}" ] && [ "${FIRST_TASK_ID}" != "${TASK_ID}" ]; then
  info "本次任务链发生过重启（首个 id=${FIRST_TASK_ID}，最终 id=${TASK_ID}）——"
  info "  若 IMG 是首次导入，请确认 [image] 与 manager.unschedulable_timeout_Ms 的配合（见 I19）"
fi

# ------------------------------------------------------------------ 6) 删除闭环 + K5
log "--- 6) 删除后：任务被拒 + images list 能解释原因（K5 闭环）"
as_user env HOME=/home/fireflyer hai-cli images delete "${IMG_URL}" > /tmp/e2e_images_del.log 2>&1 || true
tail -n 2 /tmp/e2e_images_del.log | tee -a "${LOG}"
DEL_LOG=/tmp/e2e_images_del.log
grep -q "已删除" "${DEL_LOG}" && ok "images delete 成功（AC-04）" || bad "images delete 未见成功提示"
as_user env HOME=/home/fireflyer hai-cli images list -a > /tmp/e2e_images_list_all.log 2>&1 || true
if grep -q "deleted" /tmp/e2e_images_list_all.log; then ok "images list -a 可见 deleted 行（HC-04）"; else bad "list -a 未显示 deleted 状态"; fi
as_user env HOME=/home/fireflyer hai-cli python "${TASK_DIR}/probe_image.py" -- \
  --image "${IMG_URL}" --nodes 1 -g training --name "${TASK_NAME}_rejected" > /tmp/e2e_images_reject.log 2>&1 || true
tail -n 4 /tmp/e2e_images_reject.log | tee -a "${LOG}"
if grep -q "不存在镜像\|镜像仍在加载" /tmp/e2e_images_reject.log; then
  ok "删除后提交被拒且提示可自查（K5：原话保留）"
else
  bad "删除后提交未按预期被拒"
fi

log "=== E2E_IMAGES 结果: PASS=${PASS} FAIL=${FAIL} ==="
[ "${FAIL}" -eq 0 ]
