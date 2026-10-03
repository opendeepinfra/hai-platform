#!/usr/bin/env bash
# 01-preflight.sh —— 只读前置检查。不做任何修改。
#
# 重点检查两件事：
#   1) 单节点集群确实可用且 GPU 在位（否则后面 hai-up 起来也没意义）；
#   2) **隔离性** —— 本实例的共享根目录不能与同机 VM 平台（/nfs-shared/hai-platform）重合，
#      否则会共用 postgres 数据目录 / redis / workspace，直接互相破坏。

set -uo pipefail
source "$(dirname "$0")/lib.sh"

FAILED=0
fail() { err "$*"; FAILED=$((FAILED + 1)); }

step "1/7 集群可达性（kubeconfig: ${KUBECONFIG_PATH}）"
if sudo test -f "$KUBECONFIG_PATH"; then
  ok "kubeconfig 存在"
else
  fail "kubeconfig 不存在：${KUBECONFIG_PATH}（先部署 terraform-k8s-single-node）"
fi
if kube get nodes >/dev/null 2>&1; then
  ok "apiserver 可达：$(kube get nodes -o jsonpath='{.items[*].metadata.name}')"
else
  fail "无法访问集群，请检查 $KUBECONFIG_PATH"
fi

step "2/7 节点与 GPU"
NODE_INFO="$(kube get node "$NODE_NAME" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
[ "$NODE_INFO" = "True" ] && ok "节点 $NODE_NAME Ready" || fail "节点 $NODE_NAME 不 Ready（实际：${NODE_INFO:-未知}）"
CAP="$(kube get node "$NODE_NAME" -o jsonpath='{.status.capacity.nvidia\.com/gpu}' 2>/dev/null || true)"
if [ -n "$CAP" ] && [ "${CAP:-0}" -ge "${NODE_GPUS:-1}" ] 2>/dev/null; then
  ok "nvidia.com/gpu = ${CAP}（计划给平台 NODE_GPUS=${NODE_GPUS}）"
else
  fail "nvidia.com/gpu = ${CAP:-<none>}，少于 NODE_GPUS=${NODE_GPUS}"
fi
RUNTIME="$(sudo crictl info -o json 2>/dev/null \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["config"]["containerd"]["defaultRuntimeName"])' 2>/dev/null || true)"
[ "$RUNTIME" = "nvidia" ] && ok "containerd 默认运行时 = nvidia（任务 Pod 才看得到 GPU）" \
  || fail "containerd 默认运行时 = ${RUNTIME:-未知}，应为 nvidia"

step "3/7 隔离性检查（绝不能碰 VM 平台的数据）"
case "$SHARED_FS_ROOT" in
  /nfs-shared|/nfs-shared/) fail "shared_fs_root 不能是 /nfs-shared（会与 VM 平台共用 /nfs-shared/hai-platform）" ;;
esac
if [ "$HAI_DIR" = "/nfs-shared/hai-platform" ]; then
  fail "HAI_DIR 与 VM 平台重合：$HAI_DIR"
else
  ok "本实例数据目录：${HAI_DIR}（与 VM 平台的 /nfs-shared/hai-platform 不同）"
fi
if sudo test -d "$HAI_DIR"; then
  warn "目录已存在（重复 apply）：$HAI_DIR"
fi

step "4/7 平台镜像"
if sudo docker image inspect "$PLATFORM_IMAGE" >/dev/null 2>&1; then
  ok "宿主 Docker 已有 $PLATFORM_IMAGE"
else
  fail "宿主 Docker 没有镜像 ${PLATFORM_IMAGE}（先构建或 docker pull/pull 到本机）"
fi
if sudo ctr -n k8s.io images ls -q 2>/dev/null | grep -Fxq "$PLATFORM_IMAGE"; then
  ok "containerd(k8s.io) 已有该镜像，可跳过导入"
else
  warn "containerd(k8s.io) 尚无该镜像，02 步骤会从 Docker 导入（约 5GB，需几分钟）"
fi

step "5/7 MetalLB 地址池"
POOL_IP="${METALLB_IP_RANGE%%/*}"
if ping -c1 -W1 "$POOL_IP" >/dev/null 2>&1; then
  if kube -n metallb-system get ipaddresspool >/dev/null 2>&1; then
    warn "$POOL_IP 有响应，但 MetalLB 已安装（可能是本集群自己的 VIP，属正常）"
  else
    fail "$POOL_IP 已被占用，请换 metallb_ip_range"
  fi
else
  ok "$POOL_IP 当前空闲"
fi

step "6/7 磁盘与运行时依赖"
FREE_GB="$(df -BG --output=avail / | tail -1 | tr -dc '0-9')"
[ "${FREE_GB:-0}" -ge "$MIN_FREE_DISK_GB" ] \
  && ok "根分区可用 ${FREE_GB}G ≥ ${MIN_FREE_DISK_GB}G" \
  || fail "根分区可用 ${FREE_GB}G < ${MIN_FREE_DISK_GB}G"
command -v hai-up >/dev/null 2>&1 && ok "hai-up: $(command -v hai-up)" || fail "找不到 hai-up"
command -v hai-cli >/dev/null 2>&1 && ok "hai-cli: $(command -v hai-cli)" || fail "找不到 hai-cli"

step "7/7 与既有 VM 平台的关系（只报告）"
if sudo test -d /nfs-shared/hai-platform/db; then
  warn "检测到 VM 平台数据 /nfs-shared/hai-platform/db —— 本模块**不会**读写它"
fi
if sudo test -f /root/.hfai/conf.yml; then
  warn "hai-cli 默认配置指向：$(sudo grep -h '^url:' /root/.hfai/conf.yml 2>/dev/null)（冒烟测试用 HFAI_CLIENT_CONFIG=$HAI_CONF_DIR 隔离，不改它）"
fi

echo
if [ "$FAILED" -eq 0 ]; then
  ok "前置检查全部通过"
else
  die "前置检查有 ${FAILED} 项失败"
fi
