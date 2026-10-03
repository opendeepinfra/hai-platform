#!/usr/bin/env bash
# lib.sh —— terraform-hai-platform-single-node 各步骤脚本的公共函数。
#
# 所有脚本都由 Terraform 的 local-exec 在 **host 103 本机** 执行（SSH 用户 fireflyer + 免密 sudo）。
#
# ⚠️ 隔离原则（很重要）：同一台 103 上还跑着「VM 集群 + 老平台」，
#    它的数据在 /nfs-shared/hai-platform（含 postgres 数据目录）。
#    本模块必须使用**不同的共享根目录**（默认 /nfs-shared/hai-single），
#    且只允许清理自己目录下的 db/redis。

set -uo pipefail

C_RESET=$'\033[0m'; C_BOLD=$'\033[1m'; C_RED=$'\033[1;31m'
C_GREEN=$'\033[1;32m'; C_YELLOW=$'\033[1;33m'; C_CYAN=$'\033[1;36m'

log()  { printf '%s==>%s %s\n' "$C_CYAN" "$C_RESET" "$*"; }
step() { printf '\n%s==> %s%s\n' "$C_BOLD$C_CYAN" "$*" "$C_RESET"; }
ok()   { printf '%s[ OK ]%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn() { printf '%s[WARN]%s %s\n' "$C_YELLOW" "$C_RESET" "$*"; }
err()  { printf '%s[FAIL]%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; }
die()  { err "$*"; exit 1; }

# ---- 默认值（Terraform 会用 environment 覆盖；单独手工执行时也可用）----
: "${KUBECONFIG_PATH:=/root/.kube/hai-single.conf}"
: "${NODE_NAME:=fireflyer-0003}"
: "${TASK_NAMESPACE:=hai-platform}"
: "${PLATFORM_NAMESPACE:=${TASK_NAMESPACE}}"
: "${SHARED_FS_ROOT:=/nfs-shared/hai-single}"
: "${MARS_PREFIX:=hai}"
: "${TRAINING_GROUP:=training}"
: "${JUPYTER_GROUP:=jupyter_cpu}"
: "${TRAINING_NODES:=${NODE_NAME}}"
: "${JUPYTER_NODES:=}"
: "${MANAGER_NODES:=${NODE_NAME}}"
: "${NODE_GPUS:=1}"
: "${HAS_RDMA_HCA_RESOURCE:=0}"
: "${INGRESS_CLASS:=nginx}"
: "${INGRESS_HOST:=192.168.100.150}"
: "${HAI_SERVER_ADDR:=192.168.100.150}"
: "${METALLB_VERSION:=v0.14.9}"
: "${METALLB_IP_RANGE:=192.168.100.150/32}"
: "${PLATFORM_IMAGE:=registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform:f2cb559}"
: "${BASE_IMAGE:=${PLATFORM_IMAGE}}"
: "${TRAIN_IMAGE:=${PLATFORM_IMAGE}}"
: "${POSTGRES_USER:=root}"
: "${POSTGRES_PASSWORD:=root}"
: "${REDIS_PASSWORD:=root}"
: "${USER_INFO:=haiadmin:10020:123456}"
: "${ROOT_USER:=haiadmin}"
: "${BFF_ADMIN_UID:=10000}"
: "${MIN_FREE_DISK_GB:=30}"
: "${HAI_CONF_DIR:=/tmp/hai-single-hfai}"   # 冒烟测试用的独立 hai-cli 配置目录，避免覆盖 VM 平台的 ~/.hfai/conf.yml

HAI_DIR="${SHARED_FS_ROOT}/hai-platform"
CONFIG_PATH="${HAI_DIR}/config.sh"
OVERRIDE="${HAI_DIR}/override.toml"

# 平台自带 kubectl（系统 /usr/local/bin/kubectl v1.36 可用；kubectl-hai 是单节点集群的包装脚本）
kube() {
  local bin=""
  for c in /usr/local/bin/kubectl-hai "$(command -v kubectl 2>/dev/null || true)"; do
    [ -n "$c" ] && [ -x "$c" ] && { bin="$c"; break; }
  done
  [ -n "$bin" ] || die "找不到 kubectl"
  sudo "$bin" --kubeconfig "$KUBECONFIG_PATH" "$@"
}

# 等待某个 Pod 进入 Running
wait_pod_running() {
  local ns="$1" pod="$2" tries="${3:-30}"
  local i phase
  for i in $(seq 1 "$tries"); do
    phase="$(kube -n "$ns" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [ "$phase" = "Running" ] && { echo "$phase"; return 0; }
    sleep 10
  done
  echo "${phase:-<none>}"
  return 1
}

# 从 Service 读取 LoadBalancer IP
lb_ip() {
  kube -n "$TASK_NAMESPACE" get svc hai-platform-svc \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true
}
