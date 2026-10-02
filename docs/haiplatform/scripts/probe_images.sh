#!/usr/bin/env bash
#
# hai-cli images —— 现状基线探测 / 实施后验收探测（只读、幂等）
#
# 用途：
#   1) 实施前：固化 `hfai images` 的**现状证据**（分析报告 §9 的可复现版本）；
#   2) 实施后：作为最小回归——同样的命令应当给出「全绿」的不同结果。
#
# 设计文档：docs/haiplatform/images/images-server-design.md
# 分析报告：docs/haiplatform/images/hai-cli-images-analysis.md §9
# 用例集  ：docs/haiplatform/images/images-server-test-cases.md（本脚本对应其 SMOKE 集合）
#
# 用法：
#   bash probe_images.sh [base_url]          # 默认 http://10.205.52.200
#
# 约定：
#   · **不修改任何平台状态**（只在 /tmp 下建一个空文件用于触发客户端路径检查）；
#   · 不含任何真实凭据，token 一律复用 ~/.hfai/conf.yml；
#   · hai-cli 必须以 fireflyer 身份运行（root 下没有 ~/.hfai/conf.yml，必然报「缺少 token」）。
#
set -uo pipefail

BASE_URL="${1:-http://10.205.52.200}"
NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
CONF="${CONF:-/home/fireflyer/.hfai/conf.yml}"

PASS=0
FAIL=0
ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
bad()  { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
info() { echo "  [INFO] $*"; }

# 以 fireflyer 身份跑 hai-cli（已是该用户则直接跑）
as_user() {
  if [ "$(id -un)" = "fireflyer" ]; then
    "$@"
  else
    sudo -u fireflyer "$@"
  fi
}

token() { grep -E '^ *token:' "$CONF" 2>/dev/null | awk '{print $2}'; }

# 探测一个接口，回显 "<http_code> <body 前 200 字符>"
probe() {
  local path="$1"
  curl -s -m 10 -X POST "${BASE_URL}${path}?token=$(token)" \
       -w ' [http=%{http_code}]' | head -c 220
  echo
}

echo "== 0) 环境自检 =="
if [ ! -r "$CONF" ]; then
  bad "读不到 $CONF（必须以 fireflyer 身份运行）"
else
  ok "客户端配置存在：$CONF（url=$(grep -E '^ *url:' "$CONF" | awk '{print $2}')）"
fi
as_user hai-cli whoami >/dev/null 2>&1 && ok "hai-cli whoami 可用" || bad "hai-cli whoami 失败"
T="$(token)"; [ -n "$T" ] && ok "已取到 token（不打印）" || bad "未取到 token"

echo
echo "== 1) 命令面（期望：list / load / delete 三个子命令都在）=="
# ⚠️ 不要写 `cmd | grep -q`：本脚本开了 `set -o pipefail`，`grep -q` 命中后立刻退出会让上游
# 收到 SIGPIPE(141)，管道整体被判为**失败** → 出现「其实匹配到了却走 else」的**假 PASS**。
# 统一做法：先把输出取到变量，再做匹配（不产生管道）。
HELP_OUT="$(as_user hai-cli images --help 2>&1)"
if grep -qE '^[[:space:]]+(list|load|delete)' <<<"$HELP_OUT"; then
  ok "images 组注册了 list / load / delete"
else
  bad "images --help 未见预期子命令"
fi

echo
echo "== 2) images list（现状期望：内建镜像有、用户自定义镜像**空**）=="
as_user hai-cli images list 2>&1 | tail -8

echo
echo "== 3) 服务端路由探测（现状期望：list 200；load/delete/update_status **404**）=="
for p in /ugc/user/train_image/list /ugc/user/train_image/load \
         /ugc/user/train_image/update_status /ugc/user/train_image/delete; do
  printf '%-42s ' "$p"
  probe "$p"
done

echo
echo "== 4) 数据库现状（现状期望：train_image **0 行**；train_environment 1 行 hai_base）=="
if sudo kubectl -n "$NS" exec "$POD" -- \
     psql -U root -d mars_db -c "select count(*) as train_image_rows from train_image;" 2>/dev/null | grep -qE '[0-9]'; then
  sudo kubectl -n "$NS" exec "$POD" -- \
    psql -U root -d mars_db -c "select count(*) as train_image_rows from train_image;" 2>/dev/null | head -4
  sudo kubectl -n "$NS" exec "$POD" -- \
    psql -U root -d mars_db -c "select env_name, image from train_environment;" 2>/dev/null | head -5
else
  info "无法直连 DB（跳过；不影响本脚本其余结论）"
fi

echo
echo "== 5) 运行面前置（本特性最大的风险面）=="
if [ -f marsv2/scripts/link_hfai_image.sh ] || [ -f "$(dirname "$0")/../../../marsv2/scripts/link_hfai_image.sh" ]; then
  ok "marsv2/scripts/link_hfai_image.sh 存在"
else
  bad "marsv2/scripts/link_hfai_image.sh **不存在**（I16：initContainer 会报 not found，pod 卡 Init）"
fi
# 挂载种子（one/hai-up.sh 的 storage 列表）
if grep -q 'link_hfai_image' "$(dirname "$0")/../../../one/hai-up.sh" 2>/dev/null; then
  ok "one/hai-up.sh 的 storage 种子里已登记 link 脚本"
else
  bad "one/hai-up.sh 的 storage 种子**未登记** link 脚本（任务 pod 里拿不到该文件）"
fi
# 节点前置：/data_local 与 busybox 引用
if command -v multipass >/dev/null 2>&1; then
  NODE="${NODE:-k8s-slave01}"
  if multipass exec "$NODE" -- ls -ld /data_local >/dev/null 2>&1; then
    ok "$NODE 上 /data_local 存在"
  else
    bad "$NODE 上 /data_local **不存在**（I17①：hostPath 未指定 type → kubelet 不创建 → 挂载失败）"
  fi
  if multipass exec "$NODE" -- sudo microk8s ctr images ls 2>/dev/null \
       | grep -q 'registry.high-flyer.cn/google_containers/busybox'; then
    ok "$NODE 上已有 initContainer 指定的 busybox 引用"
  else
    bad "$NODE 上**没有** registry.high-flyer.cn/google_containers/busybox:latest（I17②：会触发拉取）"
  fi
else
  info "无 multipass（跳过节点前置探测）"
fi
info "registry 探测：$(getent hosts registry.high-flyer.cn 2>/dev/null || echo '无法解析')"
info "  注意：解析到 198.18.0.0/15（代理/保留段）≠ 真实可达；无内网 registry 时请用 loader_backend=register"

echo
echo "== 6) 客户端硬缺陷复现（现状期望：两个 AttributeError）=="
if as_user hai-cli images load /tmp/probe-images-nonexistent.tar >/dev/null 2>&1; then
  info "load 对不存在 tar 打印「不存在这个镜像包」（预期行为，未走到 API）"
fi
: > /tmp/probe-images-fake.tar
LOAD_OUT="$(as_user hai-cli images load /tmp/probe-images-fake.tar 2>&1)"
if grep -q 'AttributeError' <<<"$LOAD_OUT"; then
  bad "images load 抛 AttributeError（审计 C-3 / I1：实施后应消失）"
else
  ok "images load 未抛 AttributeError"
fi
DEL_OUT="$(as_user hai-cli images delete registry.high-flyer.cn/hfai/probe:v1 2>&1)"
if grep -q 'AttributeError' <<<"$DEL_OUT"; then
  bad "images delete 抛 AttributeError（审计 C-3 / I1：实施后应消失）"
else
  ok "images delete 未抛 AttributeError"
fi
rm -f /tmp/probe-images-fake.tar

echo
echo "── 汇总：PASS=$PASS FAIL=$FAIL ──"
echo "说明：**实施前** FAIL 偏多是预期的（本脚本固化的是「现状基线」）；"
echo "     实施后应重跑本脚本，期望 load/delete/update_status 变为 200、"
echo "     link 脚本存在、挂载种子已登记、节点前置满足。"
exit 0
