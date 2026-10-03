#!/usr/bin/env bash
# 90-destroy.sh —— 卸载平台：`hai-up down` 删除本实例的 k8s 资源。
#
# 安全边界：
#   * 只删本实例的 namespace 资源（hai-up 依据 ${HAI_DIR}/k8s_configs 里的清单删除）；
#   * **不碰** VM 平台（/nfs-shared/hai-platform）与其 namespace（在另一个集群里）；
#   * 默认**保留**共享盘数据（${HAI_DIR}）；要一并删除需 PURGE_DATA=true。

set -uo pipefail
source "$(dirname "$0")/lib.sh"

step "A. 卸载平台（hai-up down）"
if sudo test -f "$CONFIG_PATH"; then
  printf 'y\n' | sudo hai-up down -c "$CONFIG_PATH" 2>&1 | sed 's/^/      /' || \
    warn "hai-up down 返回非 0（可能已删除干净）"
else
  warn "找不到 ${CONFIG_PATH}，改为直接删 namespace"
  kube delete ns "$TASK_NAMESPACE" --ignore-not-found 2>&1 | sed 's/^/      /' || true
fi

step "B. 复核 namespace"
if kube get ns "$TASK_NAMESPACE" >/dev/null 2>&1; then
  PHASE="$(kube get ns "$TASK_NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  warn "namespace $TASK_NAMESPACE 仍存在（phase=${PHASE}）；Terminating 卡住时可检查是否有 finalizer"
  kube -n "$TASK_NAMESPACE" get pods 2>&1 | sed 's/^/      /' || true
else
  ok "namespace $TASK_NAMESPACE 已删除"
fi

step "C. 数据目录"
if [ "${PURGE_DATA:-false}" = "true" ]; then
  case "$HAI_DIR" in
    /nfs-shared/hai-platform) die "拒绝执行：HAI_DIR 指向 VM 平台目录" ;;
  esac
  sudo rm -rf "$HAI_DIR"
  ok "已删除 $HAI_DIR"
else
  warn "保留数据目录 ${HAI_DIR}（如需删除：PURGE_DATA=true 重新执行本脚本）"
fi

ok "平台卸载流程完成（MetalLB 与 namespace 由本模块创建，保留以便重复部署；如需彻底清理见 README）"
