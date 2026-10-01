#!/bin/bash
# 构建 hai-platform 镜像 + hai-cli wheels（适用于 103 这类「外网受限、需本地 assets」的环境）
#
# 用法:
#   bash build_hai.sh <tag> [--no-cache]
#
# 可用环境变量覆盖（默认值对应当前 103 测试环境）:
#   REPO       源码仓库路径                默认 $HOME/hai-platform
#   BUILD_ROOT 构建目录的父目录            默认 $HOME
#   ASSETS     buildx 命名上下文 assets    默认 $HOME/build-assets
#              （需含 kubectl / decode-protobuf-camel / ambient.tar.gz /
#                fountain.tar.gz / hai-studio-*.tar.gz）
#   REGISTRY   镜像仓库前缀                默认 registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform
#   PATCHER    Dockerfile 补丁脚本         默认 <本脚本同目录>/patch_dockerfile.py
#
# 产物: 本地 docker 镜像 hai-platform:<tag> 与 <REGISTRY>:<tag>，以及 hai-cli wheels
#      （one/build_cli.sh 输出到 build/ 与 /tmp）
set -e

TAG="${1:?usage: build_hai.sh <tag> [--no-cache]}"
NOCACHE="${2:-}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

REPO="${REPO:-$HOME/hai-platform}"
BUILD_ROOT="${BUILD_ROOT:-$HOME}"
BUILD_DIR="${BUILD_ROOT}/hai-build-${TAG}"
ASSETS="${ASSETS:-$HOME/build-assets}"
REGISTRY="${REGISTRY:-registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform}"
PATCHER="${PATCHER:-$HERE/patch_dockerfile.py}"

echo "=== tag         : ${TAG}"
echo "=== repo        : ${REPO}"
echo "=== build dir   : ${BUILD_DIR}"
echo "=== asset dir   : ${ASSETS}"
echo "=== registry    : ${REGISTRY}"
cd "${REPO}"
echo "=== HEAD        : $(git rev-parse --short HEAD)"

echo "STEP: prepare build directory"
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
# 排除 .git / 缓存，避免把无关文件带进构建上下文
tar --exclude=.git --exclude=__pycache__ --exclude="*.pyc" -cf - . | (cd "${BUILD_DIR}" && tar xf -)

echo "STEP: patch Dockerfile"
python3 "${PATCHER}" "${BUILD_DIR}/Dockerfile"

echo "STEP: verify boto3 in requirements"
grep -n "boto3" "${BUILD_DIR}/requirements.txt" || { echo "boto3 missing in requirements.txt"; exit 1; }

echo "STEP: docker buildx build  $(date +%T)"
cd "${BUILD_DIR}"
sudo docker buildx build \
  --build-context assets="${ASSETS}" \
  --build-arg HAI_VERSION="${TAG}" \
  --progress plain \
  ${NOCACHE} \
  -t "hai-platform:${TAG}" \
  -t "${REGISTRY}:${TAG}" \
  --load \
  . 2>&1 | tail -40

echo "STEP: build hai-cli wheels"
export HAI_VERSION="${TAG}"
bash one/build_cli.sh 2>&1 | tail -10 || echo "WARN: build_cli.sh failed (客户端 wheel 非必需)"

echo "STEP: image info"
sudo docker images | grep -E "hai-platform\s+${TAG}|opendeepinfra/hai-platform\s+${TAG}" || true
echo "ALL DONE: ${TAG}"
