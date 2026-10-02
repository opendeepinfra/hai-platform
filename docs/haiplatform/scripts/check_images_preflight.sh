#!/usr/bin/env bash
#
# hai-cli images（用户自定义镜像）—— **部署前置自检**（OPS-04 / ENV-02 / ENV-06 / DEV-19 / AC-08）
#
# 检查「自定义镜像能不能真的跑起来」所依赖的环境前置。任何一项 FAIL 都意味着
# E2E（AC-01）必然失败，而且失败点会在任务 pod 深处才暴露 —— 所以要在部署阶段就查出来。
#
# 用法（host 103）:
#   bash check_images_preflight.sh
#
# 可用环境变量：NS / POD / OVERRIDE / REPO / NODES
#
# ⚠️ 脚本开了 pipefail：判定一律先取输出再匹配（禁止 `cmd | grep -q`，见 scripts/README §6 的陷阱说明）。
#
set -uo pipefail

NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
OVERRIDE="${OVERRIDE:-/nfs-shared/hai-platform/override.toml}"
REPO="${REPO:-/home/fireflyer/hai-platform}"
NODES="${NODES:-k8s-slave01 k8s-slave02 k8s-slave03}"

PASS=0; FAIL=0; WARN=0
ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
bad()  { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
warn() { echo "  [WARN] $*"; WARN=$((WARN + 1)); }

# conf_value <section> <key> —— 段内取键（避免 image.enabled 与 cloud.storage.service.enabled 撞名）
conf_value() {
  python3 - "$OVERRIDE" "$1" "$2" <<'PY'
import re, sys
try:
    src = open(sys.argv[1]).read()
except Exception:
    print(''); raise SystemExit
section, key = sys.argv[2], sys.argv[3]
m = re.search(r'^\[' + re.escape(section) + r'\]\s*$', src, re.M)
if not m:
    print(''); raise SystemExit
rest = src[m.end():]
n = re.search(r'^\[', rest, re.M)
body = rest[:n.start()] if n else rest
mm = re.search(r'^%s\s*=\s*(.+)$' % re.escape(key), body, re.M)
val = mm.group(1).strip() if mm else ''
if len(val) >= 2 and val[0] == "'" and val[-1] == "'":
    val = val[1:-1]
print(val)
PY
}

img() { conf_value image "$1"; }

echo "== hai-cli images 部署前置自检  $(date +%T)"

echo
echo "-- 1) 运行时配置（[image] 与 image_path）"
IMAGE_PATH="$(conf_value cloud.storage.service image_path)"
[ -n "${IMAGE_PATH}" ] && ok "image_path = ${IMAGE_PATH}" || bad "override.toml 缺 image_path"

ENABLED="$(img enabled)"
[ "${ENABLED}" = "true" ] && ok "[image].enabled=true" || bad "[image].enabled=${ENABLED}（写入接口会返回 FEATURE_DISABLED）"
BACKEND="$(img loader_backend)"
[ "${BACKEND}" = "register" ] && ok "[image].loader_backend=register" || warn "[image].loader_backend=${BACKEND}（P0 只实现 register）"
HELPER="$(img load_helper_image)"
[ -n "${HELPER}" ] && ok "[image].load_helper_image=${HELPER}" || bad "[image].load_helper_image 为空（initContainer 会 ImagePullBackOff）"
for key in data_local_path containerd_socket runtime_bin_dir runtime_lib_dir runtime_loader_file image_mount_root; do
  val="$(img "${key}")"
  if [ -n "${val}" ]; then ok "[image].${key}=${val}"; else warn "[image].${key} 为空（link 脚本可能拿不到运行时/共享根）"; fi
done

echo
echo "-- 2) 镜像共享根（平台 pod 必须能读到 tar，FR-03）"
if [ -n "${IMAGE_PATH}" ]; then
  OUT="$(sudo kubectl -n "${NS}" exec "${POD}" -- test -d "${IMAGE_PATH}" 2>&1; echo "rc=$?")"
  if [ "${OUT##*rc=}" = "0" ]; then
    ok "平台 pod 可见共享根 ${IMAGE_PATH}"
    OUT2="$(sudo kubectl -n "${NS}" exec "${POD}" -- sh -c "touch '${IMAGE_PATH}/.preflight_write_test' && rm -f '${IMAGE_PATH}/.preflight_write_test'" 2>&1; echo "rc=$?")"
    [ "${OUT2##*rc=}" = "0" ] && ok "共享根可写" || warn "共享根不可写（已存在的 tar 仍可登记；新 tar 无法就位）"
  else
    bad "平台 pod 看不到 ${IMAGE_PATH} —— load 必然 IMAGE_TAR_NOT_FOUND（见决策记录 §4.2）"
  fi
fi

echo
echo "-- 3) 运行面交付物（I16 / HC-08）"
[ -f "${REPO}/marsv2/scripts/link_hfai_image.sh" ] \
  && ok "源码树存在 marsv2/scripts/link_hfai_image.sh" || bad "源码树缺 link_hfai_image.sh"
OUT="$(sudo kubectl -n "${NS}" exec "${POD}" -- test -f /marsv2/scripts/link_hfai_image.sh 2>&1; echo "rc=$?")"
[ "${OUT##*rc=}" = "0" ] && ok "平台镜像内含 /marsv2/scripts/link_hfai_image.sh" \
  || warn "平台镜像内未见该脚本（任务侧由 configmap 挂载，但仍建议随镜像交付）"
grep -q "link_hfai_image.sh" "${REPO}/one/hai-up.sh" 2>/dev/null \
  && ok "one/hai-up.sh 已登记 storage 挂载种子" || bad "one/hai-up.sh 未登记挂载种子（pod 内会 not found）"
SEED="$(sudo kubectl -n "${NS}" exec "${POD}" -- psql -U root -d mars_db -tAc \
  "select count(*) from storage where mount_path='/marsv2/scripts/link_hfai_image.sh' and active" 2>/dev/null | tr -d '[:space:]')"
[ "${SEED}" = "1" ] && ok "storage 表已有该种子行" || bad "storage 表没有该种子行（拿到 ${SEED}）"

echo
echo "-- 4) 节点前置（I17 / OPS-04）"
SOCK="$(img containerd_socket)"
BIN="$(img runtime_bin_dir)"
LIB="$(img runtime_lib_dir)"
LDR="$(img runtime_loader_file)"
for NODE in ${NODES}; do
  multipass exec "${NODE}" -- test -d /data_local 2>/dev/null && ok "${NODE}:/data_local 存在" \
    || bad "${NODE}:/data_local 缺失（hostPath 未声明 type 时 kubelet 不创建）"
  [ -n "${SOCK}" ] && { multipass exec "${NODE}" -- test -S "${SOCK}" 2>/dev/null && ok "${NODE}: containerd socket 存在" || bad "${NODE}: containerd socket 缺失 ${SOCK}"; }
  [ -n "${BIN}" ]  && { multipass exec "${NODE}" -- test -x "${BIN}/ctr" 2>/dev/null && ok "${NODE}: ${BIN}/ctr 可执行" || bad "${NODE}: ${BIN}/ctr 不存在"; }
  [ -n "${LIB}" ]  && { multipass exec "${NODE}" -- test -e "${LIB}/libdl.so.2" 2>/dev/null && ok "${NODE}: ${LIB}/libdl.so.2 存在" || bad "${NODE}: 缺 libdl.so.2（宿主 ctr 是动态链接的）"; }
  [ -n "${LDR}" ]  && { multipass exec "${NODE}" -- test -e "${LDR}" 2>/dev/null && ok "${NODE}: loader 存在" || bad "${NODE}: loader 缺失（只挂 lib 目录会 SIGFPE）"; }
  if [ -n "${HELPER}" ]; then
    IMGS="$(multipass exec "${NODE}" -- sudo microk8s.ctr images ls -q 2>/dev/null)"
    if grep -qx "${HELPER}" <<<"${IMGS}"; then
      ok "${NODE}: helper 镜像已在节点 containerd 中"
    else
      bad "${NODE}: 节点没有 helper 镜像 ${HELPER}（会 ImagePullBackOff）"
    fi
  fi
done

echo
echo "== 结果 PASS=${PASS} FAIL=${FAIL} WARN=${WARN} =="
[ "${FAIL}" -eq 0 ]
