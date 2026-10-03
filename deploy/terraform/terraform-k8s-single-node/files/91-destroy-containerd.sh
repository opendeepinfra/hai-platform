#!/usr/bin/env bash
# 91-destroy-containerd.sh —— terraform destroy 时回滚 containerd 的 GPU 运行时配置。
#
# 默认把 03 步骤备份的 drop-in 还原回去（RESTORE_CONTAINERD_DROPIN=true）；
# 若还原后的旧配置指向已失联的 sealos.hub 沙箱镜像，属"恢复原状"，符合预期。

set -uo pipefail
source "$(dirname "$0")/lib.sh"

if [ "${RESTORE_CONTAINERD_DROPIN:-true}" != "true" ]; then
  warn "RESTORE_CONTAINERD_DROPIN=false —— 保留当前（单节点优化后的）containerd 配置"
  exit 0
fi

step "A. 查找备份"
LAST_BAK="$(sudo ls -1t "${DROPIN_PATH}".bak.* 2>/dev/null | head -1 || true)"
if [ -z "$LAST_BAK" ]; then
  warn "没有找到 ${DROPIN_PATH}.bak.* 备份，保留现有配置"
  exit 0
fi
ok "使用备份 $LAST_BAK"

step "B. 还原并重启 containerd"
sudo cp -a "$LAST_BAK" "$DROPIN_PATH"
if ! sudo containerd config dump >/dev/null 2>&1; then
  die "还原后的配置无法解析，请手工检查 ${DROPIN_PATH}（备份仍在）"
fi
sudo systemctl restart containerd
for i in $(seq 1 20); do
  sudo crictl info >/dev/null 2>&1 && break
  sleep 1
done
ok "containerd 已按备份还原并重启"

step "C. 复核 Docker 负载"
docker_snapshot | sed 's/^/      /'
ok "containerd 配置回滚完成"
