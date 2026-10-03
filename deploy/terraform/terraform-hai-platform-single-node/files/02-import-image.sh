#!/usr/bin/env bash
# 02-import-image.sh —— 把平台镜像从宿主 Docker 导入到 k8s 的 containerd。
#
# 为什么需要：103 上没有内网 registry 凭据（老环境也是走 docker save → ctr import 这条旁路）。
# 平台镜像有两个用途：
#   * 平台自身 Pod（hai-platform-0）；
#   * 任务 worker 镜像（BASE_IMAGE / TRAIN_IMAGE）。
# 两者是同一个 all-in-one 镜像，导入一次即可。

set -euo pipefail
source "$(dirname "$0")/lib.sh"

step "A. 检查 containerd(k8s.io) 是否已有镜像"
if sudo ctr -n k8s.io images ls -q 2>/dev/null | grep -Fxq "$PLATFORM_IMAGE"; then
  ok "已存在：${PLATFORM_IMAGE}（跳过导入）"
else
  step "B. docker save → ctr -n k8s.io images import"
  sudo docker image inspect "$PLATFORM_IMAGE" >/dev/null 2>&1 \
    || die "宿主 Docker 没有 $PLATFORM_IMAGE"
  TMP_TAR="/tmp/hai-single-image-$(date +%s).tar"
  log "导出镜像到 ${TMP_TAR}（约 5GB，请稍候）"
  sudo docker save "$PLATFORM_IMAGE" -o "$TMP_TAR"
  sudo ctr -n k8s.io images import "$TMP_TAR" | tail -3
  sudo rm -f "$TMP_TAR"
  ok "导入完成"
fi

step "C. 复核"
sudo ctr -n k8s.io images ls -q 2>/dev/null | grep -F "$PLATFORM_IMAGE" | sed 's/^/      /'
ok "镜像就绪"
