#!/usr/bin/env bash
# 05-verify.sh —— 部署后验收：Pod/Service/LB、hai-cli 连通、节点与 GPU 注册情况。
#
# 全程只读，且 hai-cli 使用独立配置（HFAI_CLIENT_CONFIG），不覆盖 VM 平台的 ~/.hfai/conf.yml。

set -uo pipefail
source "$(dirname "$0")/task_lib.sh"

FAILED=0
fail() { err "$*"; FAILED=$((FAILED + 1)); }

step "A. 平台 Pod 与 Service"
kube -n "$TASK_NAMESPACE" get pods -o wide 2>&1 | sed 's/^/      /'
echo
kube -n "$TASK_NAMESPACE" get svc,ingress 2>&1 | sed 's/^/      /'

step "B. 等 hai-platform-0 Running"
PHASE="$(wait_pod_running "$TASK_NAMESPACE" hai-platform-0 30 || true)"
if [ "$PHASE" = "Running" ]; then ok "hai-platform-0 Running"; else fail "hai-platform-0 phase=$PHASE"; fi

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
