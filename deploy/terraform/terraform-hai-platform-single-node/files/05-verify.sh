#!/usr/bin/env bash
# 05-verify.sh —— 部署后验收：Pod/Service/LB、hai-cli 连通、节点与 GPU 注册情况。
#
# 除 D2 的登录链路探测会创建一个 access token 外，其余全部只读；
# 且 hai-cli 使用独立配置（HFAI_CLIENT_CONFIG），不覆盖 VM 平台的 ~/.hfai/conf.yml。

set -uo pipefail
source "$(dirname "$0")/task_lib.sh"

FAILED=0
fail() { err "$*"; FAILED=$((FAILED + 1)); }

step "A. 平台 Pod 与 Service"
kube -n "$TASK_NAMESPACE" get pods -o wide 2>&1 | sed 's/^/      /'
echo
kube -n "$TASK_NAMESPACE" get svc,ingress 2>&1 | sed 's/^/      /'

step "B. 等 hai-platform-0 Running 且 Ready"
# 必须等到 Ready（而不只是 phase=Running）：平台 Pod 刚重建时，旧 Pod 可能仍在
# 终止过程中，读到的会是旧配置（例如修补前的 bffURL）。
PHASE="$(wait_pod_ready "$TASK_NAMESPACE" hai-platform-0 40 || true)"
if [ "$PHASE" = "Running" ]; then ok "hai-platform-0 Running 且 Ready"; else fail "hai-platform-0 ${PHASE}"; fi

step "C. LoadBalancer 地址"
LB="$(lb_ip)"
if [ -n "$LB" ]; then
  ok "EXTERNAL-IP = $LB"
  [ "$LB" = "$HAI_SERVER_ADDR" ] || warn "与 HAI_SERVER_ADDR=$HAI_SERVER_ADDR 不一致"
else
  fail "没有 EXTERNAL-IP（MetalLB 未就绪？）"
fi

step "D. HTTP 可达性（宿主机视角）"
if [ -n "$LB" ]; then
  # 注意：镜像内 one/thirdparty_conf/haproxy.cfg 只路由 /query/ /operating/ /ugc/ /monitor_v2/，
  # **没有 default_backend**，所以 http://$LB/ 必然是 503 —— 那不代表平台没起来。
  # 平台 UI（studio）直接监听 8080，API 走 :80 的 haproxy 前缀路由。
  UI="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "http://$LB:8080/" || true)"
  echo "      GET http://$LB:8080/ -> ${UI:-<no response>}（studio，平台 UI）"
  case "$UI" in
    200|301|302|401|403) ok "平台 UI（studio:8080）有响应" ;;
    *) fail "平台 UI 无响应（code=${UI:-none}）" ;;
  esac
  API="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "http://$LB/query/user/whoami" || true)"
  echo "      GET http://$LB/query/user/whoami -> ${API:-<no response>}（haproxy :80 → query-server）"
  case "$API" in
    000) fail "haproxy(:80) 无响应" ;;
    503) fail "haproxy(:80) 返回 503 —— 后端服务未起来？" ;;
    *)   ok "haproxy(:80) 已路由到 API（code=$API）" ;;
  esac
fi

step "D2. 浏览器登录链路（页面 haiConfig.bffURL + studio /proxy/s）"
# 这是最容易在“单节点 + 同一台机器还跑着 VM 平台”时踩的坑：
# 平台页面的 bffURL 来自 studio 的 BFF_URL ← 容器 env BFF_ADDR（one/hai-up.sh:531）。
# 若 BFF_ADDR 是集群内部服务名，浏览器就会去请求
#   http://hai-platform-svc.hai-platform.svc.cluster.local/proxy/s?endPoint=…
# 而部署机的 /etc/hosts 通常把这个内部名指向宿主机 103（宿主 nginx → VM 平台），
# 于是登录代理打到另一个平台 → 403（实测过）。这里同时校验：
#   1) 页面里的 bffURL 必须是本实例、浏览器可达的地址；
#   2) 真的走一遍 /proxy/s 的 access_token 创建（浏览器登录的第一步）—— 会新建一个
#      access token 记录，是本脚本唯一的写操作。
if [ -n "$LB" ]; then
  PAGE="$(curl -s --max-time 10 "http://$LB:8080/" || true)"
  BFF="$(echo "$PAGE" | grep -o '"bffURL":"[^"]*"' | head -1 | sed -E 's/.*:"([^"]*)".*/\1/')"
  echo "      window.haiConfig.bffURL = ${BFF:-<none>}"
  case "$BFF" in
    "http://$LB:8080") ok "bffURL 指向本实例的 studio（浏览器与页面同源）" ;;
    "")                fail "页面里读不到 bffURL（studio 没起来？）" ;;
    *".cluster.local"*) fail "bffURL 是集群内部名（$BFF）：浏览器多半会打到宿主 nginx/VM 平台 → 登录 403/503" ;;
    *)                 fail "bffURL=$BFF，期望 http://$LB:8080（BFF_ADDR 修补未生效？）" ;;
  esac
  if [ -n "$BFF" ]; then
    TOKEN="${USER_INFO##*:}"
    RESP="$(curl -s --max-time 15 -X POST \
      "$BFF/proxy/s?endPoint=/operating/user/access_token/create" \
      -H 'Content-Type: application/json' -H "token: ${TOKEN}" \
      -d "{\"url\":\"http://$LB/operating/user/access_token/create\",\"config\":{\"method\":\"POST\",\"data\":{\"user_name\":\"${ROOT_USER}\",\"token\":\"${TOKEN}\"},\"headers\":{\"Content-Type\":\"application/json\"}}}" \
      || true)"
    case "$RESP" in
      *'"success":1'*) ok "浏览器登录链路通（studio /proxy/s → access_token 创建成功）" ;;
      *) fail "浏览器登录链路失败：$(echo "$RESP" | head -c 160)" ;;
    esac
  fi
fi

step "E. hai-cli 连通性（隔离配置）"
if [ -n "$LB" ]; then
  hcli_login "$LB"
  echo "      whoami:"; hcli whoami 2>&1 | sed 's/^/        /' | head -5
  echo "      节点列表:"; hcli nodes 2>&1 | sed 's/^/        /' | head -10
fi

step "F. 平台 DB 里的节点/GPU 注册（单一事实来源）"
kube -n "$TASK_NAMESPACE" exec hai-platform-0 -- \
  psql -U "$POSTGRES_USER" -d mars_db -tAc \
  "select node, gpu_num, type, use, origin_group from host order by node" 2>&1 | sed 's/^/      /'
GPU_ROW="$(kube -n "$TASK_NAMESPACE" exec hai-platform-0 -- \
  psql -U "$POSTGRES_USER" -d mars_db -tAc \
  "select gpu_num from host where node='${NODE_NAME}'" 2>/dev/null | tr -d ' ' || true)"
if [ "${GPU_ROW:-0}" = "$NODE_GPUS" ]; then
  ok "host.gpu_num($NODE_NAME) = ${GPU_ROW}（= NODE_GPUS，调度器会把这块 V100 分给任务）"
else
  fail "host.gpu_num($NODE_NAME) = ${GPU_ROW:-<none>}，期望 $NODE_GPUS"
fi

step "G. k8s 侧 GPU 容量"
kube get node "$NODE_NAME" -o custom-columns=\
'NAME:.metadata.name,GPU_CAP:.status.capacity.nvidia\.com/gpu,GPU_ALLOC:.status.allocatable.nvidia\.com/gpu' \
  2>&1 | sed 's/^/      /'

echo
if [ "$FAILED" -eq 0 ]; then
  ok "验收通过：平台已部署在单节点集群上，地址 http://${LB:-<none>}"
else
  die "验收有 ${FAILED} 项失败"
fi
