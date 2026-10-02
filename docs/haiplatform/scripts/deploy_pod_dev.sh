#!/bin/bash
# 把 $REPO 的源码同步进运行中的 hai-platform-0 pod 并重启 ugc_server（103 上的快速联调通道）。
#
# 用途：env（haienv）特性 P0/P1 联调时，可以免去每次构建 ~1GB 镜像：
#   kubectl cp/tar 覆盖 /high-flyer/code/multi_gpu_runner_server 后 restart ugc_server 即可生效。
#   （正式发布仍必须走 build_hai.sh + redeploy_local.sh，见 docs/haiplatform/scripts/README.md）
#
# 用法（在 host 103 上，以 fireflyer 身份）：
#   bash deploy_pod_dev.sh              # 同步全部源码 + 重启 ugc_server
#   PATHS="cloud_storage conf tests" bash deploy_pod_dev.sh   # 只同步指定路径
#   SKIP_RESTART=1 bash deploy_pod_dev.sh                     # 只同步不重启
set -e

REPO="${REPO:-$HOME/hai-platform}"
NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
DEST="${DEST:-/high-flyer/code/multi_gpu_runner_server}"
LOG="${LOG:-/high-flyer/log/ugc_0.log}"
PATHS="${PATHS:-api base_model client cloud_storage conf db db_schemas deploy docs exporter fetion k8s k8s_watcher logm marsv2 monitor one plugins roman_parliament scheduler server_model tests utils uvicorn_server.py launcher.py scheduler.py k8s_watcher.py requirements.txt idempotentize.py}"

cd "${REPO}"
echo "=== repo=${REPO} HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo '-')"
echo "=== 同步源码到 ${POD}:${DEST}  $(date +%T)"
# shellcheck disable=SC2086
tar --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' -cf - ${PATHS} \
  | sudo kubectl -n "${NS}" exec -i "${POD}" -- tar -xf - -C "${DEST}"
echo "=== 同步完成  $(date +%T)"

if [ "${SKIP_RESTART:-0}" = "1" ]; then
  echo "=== SKIP_RESTART=1，不重启"
  exit 0
fi

echo "=== 重启 ugc_server  $(date +%T)"
sudo kubectl -n "${NS}" exec "${POD}" -- supervisorctl restart ugc_server || true
for i in $(seq 1 30); do
  if sudo kubectl -n "${NS}" exec "${POD}" -- supervisorctl status ugc_server 2>/dev/null | grep -q RUNNING; then
    echo "ugc_server RUNNING"
    break
  fi
  sleep 2
done
sudo kubectl -n "${NS}" exec "${POD}" -- supervisorctl status ugc_server || true
echo "=== ${LOG} 尾部 ==="
sudo kubectl -n "${NS}" exec "${POD}" -- tail -n 25 "${LOG}" || true
echo "=== DEPLOY POD DEV DONE $(date +%T)"
