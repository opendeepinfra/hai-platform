#!/bin/bash
# 用户自定义镜像（hai-cli images）测试夹具：在平台基础镜像上加一个**内容可区分**的探针文件，
# 然后用 docker save 打成 tar 放到镜像共享根，供 `hai-cli images load` 使用。
#
# 为什么必须可区分（用例 E2E-02 / AC-01）：本特性的判据不是「接口 200」，而是
# 「自定义镜像真的跑起了任务，且输出能被镜像内容区分」。
#
# 用法（host 103）:
#   bash image_fixture.sh [镜像名] [输出 tar 路径]
#   默认镜像名 registry.high-flyer.cn/hfai/demo:v1
#   默认输出   {IMAGE_ROOT}/demo.tar
#
# 可用环境变量：BASE_IMAGE（默认取当前部署的 StatefulSet 镜像）/ IMAGE_ROOT / PROBE_TEXT / NS
set -eu

NS="${NS:-hai-platform}"
TAG="${1:-registry.high-flyer.cn/hfai/demo:v1}"
IMAGE_ROOT="${IMAGE_ROOT:-/nfs-shared/hai-platform/workspace/image}"
OUT="${2:-${IMAGE_ROOT}/demo.tar}"

BASE_IMAGE="${BASE_IMAGE:-$(sudo kubectl -n "${NS}" get statefulset hai-platform \
  -o jsonpath='{.spec.template.spec.containers[0].image}')}"
PROBE_TEXT="${PROBE_TEXT:-HFAI_CUSTOM_IMAGE_PROBE image=${TAG} built=$(date -u +%FT%TZ)}"

echo "=== fixture: tag=${TAG}"
echo "=== base  : ${BASE_IMAGE}"
echo "=== output: ${OUT}"

sudo mkdir -p "${IMAGE_ROOT}"
sudo chmod 777 "${IMAGE_ROOT}"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
cat > "${WORK}/Dockerfile" <<EOF
FROM ${BASE_IMAGE}
ARG PROBE_TEXT
RUN printf '%s\n' "\$PROBE_TEXT" > /hfai_image_probe.txt && cat /hfai_image_probe.txt
EOF

sudo docker build --build-arg PROBE_TEXT="${PROBE_TEXT}" -t "${TAG}" "${WORK}"
sudo docker save "${TAG}" -o "${OUT}.tmp"
sudo mv "${OUT}.tmp" "${OUT}"
sudo chmod 644 "${OUT}"
ls -l "${OUT}"
echo "PROBE_TEXT=${PROBE_TEXT}"
echo "FIXTURE_OK"
