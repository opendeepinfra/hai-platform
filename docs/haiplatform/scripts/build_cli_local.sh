#!/bin/bash
# 在 host 103 上从 $REPO 直接构建 hai-cli + hai* 插件 wheel 并安装（免 docker 的客户端联调通道）。
#
# 与 one/build_cli.sh（镜像内构建）等价，区别只在于安装目标是宿主机系统 python3，
# 便于在 103 上直接验证 `hai-cli env push` 的客户端行为（E3 / E13 / 分级结果）。
#
# 用法（host 103，以 fireflyer 身份，需免密 sudo pip3）：
#   bash build_cli_local.sh
#   BUILD_ONLY=1 bash build_cli_local.sh     # 只构建 wheel，不安装
set -e

REPO="${REPO:-$HOME/hai-platform}"
cd "${REPO}"

HAI_VERSION="${HAI_VERSION:-$(git rev-parse --short HEAD)}"
export HAI_VERSION
echo "=== repo=${REPO} HAI_VERSION=${HAI_VERSION}"

# 构建期依赖：`client/patch_client.py` 需要 astunparse；缺失时 install.sh 会在
# `cp conf/utils.py` 之前中断（set -e），产出「看起来成功但 hfai.conf.utils 缺失」的坏 wheel。
if ! python3 -c "import astunparse" >/dev/null 2>&1; then
  echo "=== 安装构建依赖 astunparse"
  sudo pip3 install astunparse
fi

rm -f /tmp/hai-*.whl /tmp/haienv-*.whl /tmp/haiworkspace-*.whl
bash one/build_cli.sh

echo "=== 构建产物 ==="
ls -l /tmp/hai*.whl

echo "=== 产物自检（关键模块必须在 wheel 内）==="
python3 - <<'PY'
import glob
import zipfile

required = {
    'hai': ['hfai/conf/utils.py', 'hfai/conf/flags/custom.py', 'hfai/client/api/venv_api.py'],
    'haienv': ['haienv/client/command.py', 'haienv/client/model.py'],
}
missing = []
for prefix, names in required.items():
    wheels = glob.glob(f'/tmp/{prefix}-*.whl')
    if not wheels:
        missing.append(f'{prefix}: wheel 缺失')
        continue
    with zipfile.ZipFile(wheels[0]) as zf:
        contents = set(zf.namelist())
    for name in names:
        if name not in contents:
            missing.append(f'{prefix}: {name}')
if missing:
    raise SystemExit('wheel 自检失败，缺少: ' + ', '.join(missing))
print('wheel 自检通过:', {p: glob.glob(f'/tmp/{p}-*.whl')[0] for p in required})
PY

if [ "${BUILD_ONLY:-0}" = "1" ]; then
  echo "=== BUILD_ONLY=1，跳过安装"
  exit 0
fi

echo "=== 安装到系统 python3 ==="
sudo pip3 install --force-reinstall --no-deps /tmp/hai-*.whl /tmp/haienv-*.whl /tmp/haiworkspace-*.whl
echo "=== 版本 ==="
hai-cli --version || true
python3 -c "import haienv, haienv.client.command as c; print('haienv push 存在:', hasattr(c, 'push'))"

echo "=== 清理构建中间产物 (one/hfai, one/build, one/dist, one/hai-cli, one/hai-up) ==="
rm -rf one/hfai one/build one/dist one/*.egg-info one/hai-cli one/hai-up
echo "=== BUILD CLI LOCAL DONE"
