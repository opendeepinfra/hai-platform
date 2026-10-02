
if [ "x${1}" != "x" ]; then
  HFAI_PATH=$1
  BIN_PATH=$1/client/hfai
else
  HFAI_PATH=$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')/hfai/
  BIN_PATH=$(python -c 'import sysconfig; print(sysconfig.get_paths()["scripts"])')/hfai
fi

set -e
echo "install hfai client in ${HFAI_PATH}..."
mkdir -p ${HFAI_PATH}
pushd ${HFAI_PATH}
  rm -rf base_model/ conf/ client/
popd

cp -r base_model/ client/ ${HFAI_PATH}

mkdir -p ${HFAI_PATH}/conf
touch ${HFAI_PATH}/conf/__init__.py

cp -r conf/flags ${HFAI_PATH}/conf/flags
# 保证 `import hfai.conf.flags` 可用：base_model 的 CustomFinder 只接管
#   <…>/hfai/client/** 与 <…>/hfai/base_model/** 下的 custom 模块，
#   hfai/conf/flags/custom.py 必须真实存在（`conf/flags/implement.py:3` 会 `from .custom import *`）。
# 原实现只在 external=true 时创建，导致构建出的 hai-cli 一执行就 ModuleNotFoundError。
echo "" > ${HFAI_PATH}/conf/flags/custom.py

python client/patch_client.py --hfai_path ${HFAI_PATH}

rm ${HFAI_PATH}/client/patch_client.py
rm ${HFAI_PATH}/client/create_develop_env.sh
rm -rf ${HFAI_PATH}/client/README.md

cp conf/utils.py ${HFAI_PATH}/conf

if [[ ! -f ${BIN_PATH} ]]; then
  cp client/hfai ${BIN_PATH}
fi
chmod +x ${BIN_PATH}

echo "install hfai client success"
