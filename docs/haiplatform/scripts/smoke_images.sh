#!/usr/bin/env bash
#
# hai-cli images（用户自定义镜像）—— L2 接口契约冒烟（API-15/16/17/18）
#
# 对应文档：docs/haiplatform/images/images-server-test-cases.md §4.2 A 组 / Checklist 附录 B
#           docs/haiplatform/images/images-server-checklist.md §5（API-01~API-12）
#
# 设计原则：**只验 HTTP 200 不算通过** —— 每个用例同时校验响应体与 DB 副作用（行数 / 状态 / path）。
#
# 用法（host 103）:
#   bash smoke_images.sh [base_url]           # 默认 http://10.205.52.200
#
# 可用环境变量：IMG_ROOT（默认从 override.toml 的 image_path 读取）/ TAR / GRP / NS / POD
#
set -uo pipefail

BASE_URL="${1:-http://10.205.52.200}"
NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
CONF="${CONF:-/home/fireflyer/.hfai/conf.yml}"
OVERRIDE="${OVERRIDE:-/nfs-shared/hai-platform/override.toml}"
GRP="${GRP:-hfai}"
REGISTRY="${REGISTRY:-registry.high-flyer.cn}"

PASS=0
FAIL=0
ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
bad()  { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
info() { echo "  [INFO] $*"; }

TMPD="$(mktemp -d)"
trap 'rm -rf "${TMPD}"' EXIT

# ---------------------------------------------------------------- 基础工具

as_user() {
  if [ "$(id -un)" = "fireflyer" ]; then "$@"; else sudo -u fireflyer "$@"; fi
}

token() { grep -E '^ *token:' "${CONF}" 2>/dev/null | awk '{print $2}'; }

psql_q() { sudo kubectl -n "${NS}" exec "${POD}" -- psql -U root -d mars_db -tAc "$1" 2>/dev/null; }

# pyget <file> <python 表达式（d 为解析后的 JSON）>
pyget() {
  python3 - "$1" "$2" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
print(eval(sys.argv[2]))
PY
}

# 调用一个接口，把响应体写进 ${TMPD}/<name>.json，回显 HTTP 码
call() { # call <name> <path> [额外 curl 参数...]
  local name="$1"; local path="$2"; shift 2
  local code
  code=$(curl -s -m 20 -o "${TMPD}/${name}.json" -w '%{http_code}' -X POST \
         "${BASE_URL}${path}" "$@")
  echo "${code}"
}

TOKEN="$(token)"
IMG_ROOT="${IMG_ROOT:-$(python3 - "${OVERRIDE}" <<'PY'
import re, sys
try:
    src = open(sys.argv[1]).read()
except Exception:
    print('/nfs-shared/hai-platform/workspace/image'); raise SystemExit
m = re.search(r"^image_path\s*=\s*'([^']*)'", src, re.M)
print(m.group(1) if m else '/nfs-shared/hai-platform/workspace/image')
PY
)}"
TAR="${TAR:-${IMG_ROOT}/demo.tar}"
# 第二个 tar：用于「显式 --image」与「同一 image 多行」的用例
# （同一 image_tar 重复 load 是幂等的，不会因为再传 --image 而改名 —— 见设计 §4.1）
TAR2="${TAR2:-${IMG_ROOT}/demo_v2.tar}"
if [ ! -f "${TAR2}" ] && [ -f "${TAR}" ]; then
  cp "${TAR}" "${TAR2}" 2>/dev/null || sudo cp "${TAR}" "${TAR2}" 2>/dev/null || true
  sudo chmod 644 "${TAR2}" 2>/dev/null || true
fi

echo "=== smoke_images against ${BASE_URL} @ $(date +%T)"
echo "=== image_root=${IMG_ROOT} tar=${TAR} group=${GRP}"

echo
echo "== 0) 环境自检 =="
[ -r "${CONF}" ] && ok "客户端配置可读: ${CONF}" || bad "读不到 ${CONF}（必须以 fireflyer 身份运行）"
[ -n "${TOKEN}" ] && ok "已取到 token（不打印）" || { bad "未取到 token"; echo "=== PASS=${PASS} FAIL=${FAIL} ==="; exit 1; }
[ -d "${IMG_ROOT}" ] && ok "镜像共享根存在: ${IMG_ROOT}" || bad "镜像共享根不存在: ${IMG_ROOT}"
[ -f "${TAR}" ] && ok "测试 tar 存在: ${TAR}" || bad "测试 tar 不存在: ${TAR}（先跑 image_fixture.sh）"

MISSING_TAR="${IMG_ROOT}/__not_exists__.tar"

echo
echo "== 0.5) 清场（删除本用例两个 tar 的历史行，保证脚本可重复运行）=="
psql_q "delete from train_image where image_tar in ('${TAR}','${TAR2}')" >/dev/null 2>&1 || true
ok "已清理 ${TAR} 与 ${TAR2} 的历史行"

echo
echo "== 1) API-17 list（修订版：user_images 真实查询）=="
CODE=$(call list "/ugc/user/train_image/list?token=${TOKEN}")
[ "${CODE}" = "200" ] && ok "HTTP 200" || bad "HTTP ${CODE}"
if [ "$(pyget "${TMPD}/list.json" "d['success']")" = "1" ]; then ok "success=1"; else bad "success!=1: $(cat "${TMPD}/list.json")"; fi
if [ "$(pyget "${TMPD}/list.json" "isinstance(d.get('result',{}).get('user_images'), list)")" = "True" ]; then
  ok "result.user_images 是列表（行数 $(pyget "${TMPD}/list.json" "len(d['result']['user_images'])")）"
else
  bad "result.user_images 缺失或非列表"
fi
if [ "$(pyget "${TMPD}/list.json" "isinstance(d.get('result',{}).get('mars_images'), list)")" = "True" ]; then
  ok "result.mars_images 保持原样（CMP-03）"
else
  bad "内建镜像字段被破坏"
fi
if [ "$(pyget "${TMPD}/list.json" "all(k in r for r in d['result']['user_images'] for k in ('registry','shared_group','image','status','image_tar','updated_at'))")" = "True" ]; then
  ok "每行 6 字段齐备（FR-11）"
else
  bad "user_images 行字段缺失"
fi

echo
echo "== 2) API-15 load（旧形态：只发 image_tar，服务端派生镜像名，CMP-01）=="
CODE=$(call load_old "/ugc/user/train_image/load?token=${TOKEN}&image_tar=${TAR}")
[ "${CODE}" = "200" ] && ok "HTTP 200" || bad "HTTP ${CODE}"
if [ "$(pyget "${TMPD}/load_old.json" "d['success']")" = "1" ]; then
  DERIVED="$(pyget "${TMPD}/load_old.json" "d['image']")"
  ok "success=1 image=${DERIVED} status=$(pyget "${TMPD}/load_old.json" "d['status']")"
  case "$(pyget "${TMPD}/load_old.json" "d['status']")" in
    loaded|processing|loading) ok "状态在合法集合内" ;;
    *) bad "状态非法" ;;
  esac
  [ "$(pyget "${TMPD}/load_old.json" "'msg' in d and bool(d['msg'])")" = "True" ] && ok "响应含 msg（旧客户端唯一消费字段）" || bad "响应缺 msg"
else
  bad "load 失败: $(cat "${TMPD}/load_old.json")"
fi

echo
echo "== 3) API-15 幂等（同 tar 再来一次，行数/状态不变）=="
ROWS_BEFORE=$(psql_q "select count(*) from train_image where shared_group='${GRP}' and image_tar='${TAR}'")
CODE=$(call load_again "/ugc/user/train_image/load?token=${TOKEN}&image_tar=${TAR}")
ROWS_AFTER=$(psql_q "select count(*) from train_image where shared_group='${GRP}' and image_tar='${TAR}'")
[ "${ROWS_AFTER}" = "1" ] && ok "同 tar 仍只有 1 行（before=${ROWS_BEFORE} after=${ROWS_AFTER}）" || bad "行数异常: ${ROWS_AFTER}"
[ "$(pyget "${TMPD}/load_again.json" "d['status']")" = "$(pyget "${TMPD}/load_old.json" "d['status']")" ] \
  && ok "状态未倒退/未重置" || bad "状态被重置: $(pyget "${TMPD}/load_again.json" "d['status']")"

echo
echo "== 4) DB 副作用核对（register 后端：path == image_tar、task_id=0、status=loaded）=="
ROW=$(psql_q "select image||'|'||path||'|'||status||'|'||task_id from train_image where shared_group='${GRP}' and image_tar='${TAR}' order by updated_at desc limit 1")
info "row = ${ROW}"
if [ -n "${ROW}" ]; then
  ok "DB 有该行"
  [ "$(echo "${ROW}" | cut -d'|' -f2)" = "${TAR}" ] && ok "path == image_tar（register 后端契约）" || bad "path 与 image_tar 不一致（I18 概念混淆风险）"
  [ "$(echo "${ROW}" | cut -d'|' -f3)" = "loaded" ] && ok "status=loaded（任务侧白名单，HC-03）" || bad "status 不是 loaded: $(echo "${ROW}" | cut -d'|' -f3)"
  [ "$(echo "${ROW}" | cut -d'|' -f4)" = "0" ] && ok "task_id=0" || info "task_id=$(echo "${ROW}" | cut -d'|' -f4)（非 register 后端可忽略）"
else
  bad "DB 无该行（写路径没生效）"
fi

echo
echo "== 5) API-15 负例（越界 / 不存在 / 非法名），且不得产生新行 =="
ROWS0=$(psql_q "select count(*) from train_image")
CODE=$(call neg_escape "/ugc/user/train_image/load?token=${TOKEN}" -H 'Content-Type: text/plain' -d '{"image_tar":"/etc/passwd"}')
[ "$(pyget "${TMPD}/neg_escape.json" "d.get('code')")" = "PATH_ESCAPE" ] && ok "越界 -> PATH_ESCAPE" || bad "越界未拒绝: $(cat "${TMPD}/neg_escape.json")"
CODE=$(call neg_missing "/ugc/user/train_image/load?token=${TOKEN}&image_tar=${MISSING_TAR}")
[ "$(pyget "${TMPD}/neg_missing.json" "d.get('code')")" = "IMAGE_TAR_NOT_FOUND" ] && ok "不存在 -> IMAGE_TAR_NOT_FOUND" || bad "不存在未拒绝: $(cat "${TMPD}/neg_missing.json")"
CODE=$(call neg_name "/ugc/user/train_image/load?token=${TOKEN}&image_tar=${TAR}&image=a/b:v1")
[ "$(pyget "${TMPD}/neg_name.json" "d.get('code')")" = "INVALID_PARAM" ] && ok "镜像名含 / -> INVALID_PARAM（HC-05）" || bad "非法名未拒绝: $(cat "${TMPD}/neg_name.json")"
ROWS1=$(psql_q "select count(*) from train_image")
[ "${ROWS0}" = "${ROWS1}" ] && ok "负例未产生新行（${ROWS0} -> ${ROWS1}）" || bad "负例产生了脏数据（${ROWS0} -> ${ROWS1}）"

echo
echo "== 6) API-15 入参承载兼容（query / text/plain / application/json）与显式 --image =="
[ -f "${TAR2}" ] || bad "第二个 tar 不存在（${TAR2}），显式 --image 用例将失败"
CODE=$(call load_body "/ugc/user/train_image/load?token=${TOKEN}" -H 'Content-Type: text/plain' \
        -d "{\"image_tar\":\"${TAR2}\",\"image\":\"demo:v1\"}")
[ "$(pyget "${TMPD}/load_body.json" "d['success']")" = "1" ] && ok "text/plain JSON 可用" || bad "text/plain JSON 失败: $(cat "${TMPD}/load_body.json")"
[ "$(pyget "${TMPD}/load_body.json" "d['image']")" = "${REGISTRY}/${GRP}/demo:v1" ] && ok "三段 URL 正确: $(pyget "${TMPD}/load_body.json" "d['image']")" || bad "三段 URL 不符: $(pyget "${TMPD}/load_body.json" "d['image']")"
CODE=$(call load_json "/ugc/user/train_image/load?token=${TOKEN}" -H 'Content-Type: application/json' \
        -d "{\"image_tar\":\"${TAR2}\",\"image\":\"demo:v1\"}")
[ "$(pyget "${TMPD}/load_json.json" "d['success']")" = "1" ] && ok "application/json 可用" || bad "application/json 失败: $(cat "${TMPD}/load_json.json")"
IMG2_ROW=$(psql_q "select image||'|'||path||'|'||status from train_image where shared_group='${GRP}' and image_tar='${TAR2}' order by updated_at desc limit 1")
info "tar2 row = ${IMG2_ROW}"
[ "$(echo "${IMG2_ROW}" | cut -d'|' -f1)" = "demo:v1" ] && ok "显式 image 落库为 demo:v1" || bad "显式 image 未落库: ${IMG2_ROW}"

echo
echo "== 7) API-16 update_status（防伪造 + 非法迁移）=="
CODE=$(call st_forge "/ugc/user/train_image/update_status?token=${TOKEN}" -H 'Content-Type: text/plain' \
        -d "{\"image_tar\":\"${TAR}\",\"status\":\"loaded\",\"path\":\"${TAR}\",\"task_id\":999999}")
[ "$(pyget "${TMPD}/st_forge.json" "d.get('code')")" = "FORBIDDEN" ] && ok "伪造 task_id -> FORBIDDEN（SEC-04）" || bad "伪造未拒绝: $(cat "${TMPD}/st_forge.json")"
CODE=$(call st_del "/ugc/user/train_image/update_status?token=${TOKEN}" -H 'Content-Type: text/plain' \
        -d "{\"image_tar\":\"${TAR}\",\"status\":\"deleted\",\"task_id\":0}")
CODE=$(call st_illegal "/ugc/user/train_image/update_status?token=${TOKEN}" -H 'Content-Type: text/plain' \
        -d "{\"image_tar\":\"${TAR}\",\"status\":\"processing\",\"task_id\":0}")
[ "$(pyget "${TMPD}/st_illegal.json" "d.get('code')")" = "ILLEGAL_TRANSITION" ] && ok "loaded -> processing 非法（ILLEGAL_TRANSITION）" || bad "非法迁移未拒绝: $(cat "${TMPD}/st_illegal.json")"

echo
echo "== 8) API-18 delete（幂等 / 跨组 / 非 3 段）=="
# 8.1 非 3 段
CODE=$(call del_short "/ugc/user/train_image/delete?token=${TOKEN}&image=demo:v1")
[ "$(pyget "${TMPD}/del_short.json" "d.get('code')")" = "INVALID_PARAM" ] && ok "非 3 段 -> INVALID_PARAM" || bad "非 3 段未拒绝: $(cat "${TMPD}/del_short.json")"
# 8.2 跨组
CODE=$(call del_cross "/ugc/user/train_image/delete?token=${TOKEN}&image=${REGISTRY}/othergroup/demo:v1")
[ "$(pyget "${TMPD}/del_cross.json" "d.get('code')")" = "FORBIDDEN" ] && ok "跨组 -> FORBIDDEN（SEC-02/05）" || bad "跨组未拒绝: $(cat "${TMPD}/del_cross.json")"
# 8.3 正常删除 + 幂等
CODE=$(call del1 "/ugc/user/train_image/delete?token=${TOKEN}&image=${REGISTRY}/${GRP}/demo:v1")
[ "$(pyget "${TMPD}/del1.json" "d['success']")" = "1" ] && ok "删除成功 deleted=$(pyget "${TMPD}/del1.json" "d['deleted']")" || bad "删除失败: $(cat "${TMPD}/del1.json")"
CODE=$(call del2 "/ugc/user/train_image/delete?token=${TOKEN}&image=${REGISTRY}/${GRP}/demo:v1")
[ "$(pyget "${TMPD}/del2.json" "d['deleted']")" = "0" ] && ok "重复删除 deleted:0（幂等）" || bad "重复删除非幂等: $(cat "${TMPD}/del2.json")"
# 8.4 删除后该镜像退出任务白名单（status != loaded）
LEFT=$(psql_q "select count(*) from train_image where shared_group='${GRP}' and image='demo:v1' and status='loaded'")
[ "${LEFT}" = "0" ] && ok "删除后无 loaded 行（自动退出任务白名单）" || bad "仍有 ${LEFT} 行 loaded"
# 8.5 list 仍能看到 deleted 行（服务端不隐藏，CMP-05 / HC-04）
CODE=$(call list_deleted "/ugc/user/train_image/list?token=${TOKEN}")
if [ "$(pyget "${TMPD}/list_deleted.json" "any('deleted' in r['status'] for r in d['result']['user_images'])")" = "True" ]; then
  ok "list 返回含 deleted 行（由客户端 -a 决定隐藏）"
else
  bad "list 未返回 deleted 行"
fi

echo
echo "== 9) 鉴权（缺 token 必须 403 且带 success）=="
CODE=$(curl -s -m 10 -o "${TMPD}/no_token.json" -w '%{http_code}' -X POST "${BASE_URL}/ugc/user/train_image/list")
[ "${CODE}" = "403" ] && ok "缺 token -> 403" || bad "缺 token HTTP ${CODE}"
python3 - "${TMPD}/no_token.json" <<'PY' && ok "403 响应体含 success=0" || bad "403 响应体不含 success"
import json, sys
d = json.load(open(sys.argv[1]))
body = d.get('detail', d)
assert body.get('success') == 0, d
PY

echo
echo "== 10) 运行面前置（I16/I17：脚本 / 挂载种子 / 节点 /data_local / helper 镜像）=="
[ -f "${REPO:-/home/fireflyer/hai-platform}/marsv2/scripts/link_hfai_image.sh" ] \
  && ok "link_hfai_image.sh 存在于源码树" || info "（跳过源码树检查：REPO 未指向仓库）"
if sudo kubectl -n "${NS}" exec "${POD}" -- test -f /high-flyer/code/multi_gpu_runner_server/marsv2/scripts/link_hfai_image.sh 2>/dev/null; then
  ok "平台镜像内含 marsv2/scripts/link_hfai_image.sh"
else
  bad "平台镜像内缺 marsv2/scripts/link_hfai_image.sh（HC-08）"
fi
SEED=$(psql_q "select count(*) from storage where mount_path='/marsv2/scripts/link_hfai_image.sh' and active")
[ "${SEED}" = "1" ] && ok "storage 表已登记 link 脚本挂载种子（HC-08）" || bad "storage 表缺 link 脚本种子行"
for NODE in k8s-slave01 k8s-slave02 k8s-slave03; do
  if multipass exec "${NODE}" -- test -d /data_local 2>/dev/null; then ok "${NODE}:/data_local 存在"; else bad "${NODE}:/data_local 缺失（I17①）"; fi
done

echo
echo "=== 结果 PASS=${PASS} FAIL=${FAIL} ==="
[ "${FAIL}" = "0" ]
