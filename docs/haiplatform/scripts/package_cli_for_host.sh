#!/bin/bash
# 把当前提交打包成 hai-cli 全家桶 wheel，并安装到目标机（默认 fireflyer@192.168.100.103）。
#
# 为什么不用目标机上那份工作树直接构建：目标机的 git HEAD 往往落后于工作树内容
# （内容靠 rsync 同步），wheel 版本号会错；这里用 `git archive HEAD` 的**干净源码**构建，
# 并显式传 HAI_VERSION=<short>，保证「安装的包 == 某个提交」。
#
# 用法（在 hai-platform 仓库根执行；需要能 ssh/scp 到目标机）：
#   bash package_cli_for_host.sh                       # 默认 fireflyer@192.168.100.103
#   bash package_cli_for_host.sh fireflyer@192.168.100.104
#
# 产物：目标机 ~/hai-cli-wheels/<short>/ 下的三个 wheel + SHA256SUMS + MANIFEST.txt
set -euo pipefail

TARGET="${1:-fireflyer@192.168.100.103}"
REPO="${REPO:-$(pwd)}"
SSH_OPTS="${SSH_OPTS:--o BatchMode=yes -o StrictHostKeyChecking=no}"

cd "${REPO}"
if [ ! -d .git ]; then
  echo "错误：${REPO} 不是 git 仓库根目录（需要 git archive 打干净源码）"; exit 1
fi
SHORT="$(git rev-parse --short HEAD)"
DIRTY="$(git status --porcelain | wc -l | tr -d ' ')"
TARBALL="/tmp/hai-cli-${SHORT}.tar.gz"
REMOTE_DIR="/tmp/hai-cli-${SHORT}"

echo "=== 打包提交：${SHORT}（工作树改动 ${DIRTY} 处，打包内容为提交内容）"
git archive --format=tar.gz -o "${TARBALL}" HEAD
ls -la "${TARBALL}"
sha256sum "${TARBALL}" 2>/dev/null || shasum -a 256 "${TARBALL}"

echo "=== 传输到 ${TARGET}"
scp -q ${SSH_OPTS} "${TARBALL}" "${TARGET}:/tmp/"

echo "=== 在目标机构建并安装"
# shellcheck disable=SC2086
ssh ${SSH_OPTS} "${TARGET}" bash -s <<EOF
set -e
rm -rf "${REMOTE_DIR}" && mkdir -p "${REMOTE_DIR}"
tar xzf "${TARBALL}" -C "${REMOTE_DIR}"
REPO="${REMOTE_DIR}" HAI_VERSION="${SHORT}" bash "${REMOTE_DIR}/docs/haiplatform/scripts/build_cli_local.sh"

WHEEL_DIR="\$HOME/hai-cli-wheels/${SHORT}"
mkdir -p "\${WHEEL_DIR}"
cp -f /tmp/hai-*.whl /tmp/haienv-*.whl /tmp/haiworkspace-*.whl "\${WHEEL_DIR}/"
rm -rf "${REMOTE_DIR}" "${TARBALL}"

cd "\${WHEEL_DIR}"
sha256sum *.whl > SHA256SUMS
{
  echo "commit: ${SHORT}"
  echo "built_at: \$(date -Iseconds)"
  echo "host: \$(hostname)"
  echo "python: \$(python3 -V 2>&1)"
} > MANIFEST.txt

echo "=== 安装结果 ==="
hai-cli --version
python3 -m pip show hai haienv haiworkspace 2>/dev/null | grep -E '^(Name|Version|Location)'
echo "=== 归档 ==="
ls -la "\${WHEEL_DIR}"
EOF

rm -f "${TARBALL}"
echo "=== 完成：${TARGET} 已安装 ${SHORT}；wheel 归档在目标机 ~/hai-cli-wheels/${SHORT}/"
