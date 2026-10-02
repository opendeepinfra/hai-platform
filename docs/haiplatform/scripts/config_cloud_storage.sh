#!/bin/bash
# 幂等地把 [cloud.storage] / [cloud.storage.service] 段写进平台的 override.toml。
#
# 背景：conf/proj_conf/default.py 的合并顺序是 core → scheduler → extension → override，
#      override 优先级最高；而 k8s 部署把 /nfs-shared/hai-platform/override.toml
#      挂进 pod 的 /etc/hai_one_config/override.toml。因此改这里 + 重启 pod 即可生效，
#      **不需要重建镜像**。
#
# 用法（密钥只从环境变量取，仓库不落真实密钥）:
#   RUSTFS_AK=xxx RUSTFS_SK=yyy bash config_cloud_storage.sh [override.toml 路径] [endpoint] [workspace_path]
#
# 默认值对应当前 103 测试环境:
#   override.toml  /nfs-shared/hai-platform/override.toml
#   endpoint       http://192.168.100.103:19000
#   workspace_path /nfs-shared/hai-platform/workspace
set -e

: "${RUSTFS_AK:?请先 export RUSTFS_AK=...}"
: "${RUSTFS_SK:?请先 export RUSTFS_SK=...}"

OVERRIDE="${1:-/nfs-shared/hai-platform/override.toml}"
ENDPOINT="${2:-http://192.168.100.103:19000}"
WORKSPACE_PATH="${3:-/nfs-shared/hai-platform/workspace}"
PROVIDER="${PROVIDER:-s3}"

# 允许覆盖 bucket 名（默认与 core.toml 模板一致）
PRIVATE_BUCKET="${PRIVATE_BUCKET:-hai-platform-private}"
PUBLIC_BUCKET="${PUBLIC_BUCKET:-hai-platform-public}"

echo "=== override.toml : ${OVERRIDE}"
echo "=== provider      : ${PROVIDER}"
echo "=== endpoint      : ${ENDPOINT}"
echo "=== workspace_path: ${WORKSPACE_PATH}"

sudo test -f "${OVERRIDE}" || { echo "找不到 ${OVERRIDE}"; exit 1; }

sudo python3 - "${OVERRIDE}" "${PROVIDER}" "${ENDPOINT}" "${WORKSPACE_PATH}" \
                "${PRIVATE_BUCKET}" "${PUBLIC_BUCKET}" <<'PYEOF'
import io, re, sys
path, provider, endpoint, ws_path, priv, pub = sys.argv[1:7]
src = io.open(path, encoding='utf-8').read()

# 先删掉旧的 cloud.storage 段（含 service 子表），保证幂等
src = re.split(r"\n\[cloud\.storage\]", src)[0].rstrip() + "\n"

# 密钥从环境变量读，避免出现在命令行/进程列表里
import os
ak, sk = os.environ['RUSTFS_AK'], os.environ['RUSTFS_SK']

src += """
[cloud.storage]
provider = '%s'
endpoint = '%s'
access_key_id = '%s'
access_key_secret = '%s'
uid = ''
role_arn = 'hai-platform'
private_bucket = '%s'
public_bucket = '%s'
doc_bucket = '%s'
pypi_bucket = '%s'
official_website_bucket = '%s'
[cloud.storage.service]
workspace_path = '%s'
env_path = '%s/hfai_envs'
public_dataset_path = '%s/public_dataset'
private_dataset_path = '%s/private_dataset'
doc_path = '%s/doc'
pypi_path = '%s/pypi'
official_website_path = '%s/website'
breakpoint_info_path = '%s/.hfai/breakpoints'
proxy_endpoint = ''
public_bucket_allowed_users = ''
password = ''
enabled = true
enabled_users = []
enabled_groups = []
legacy_param_compat = true
status_ttl_finished = 1800
max_files_per_request = 10000
max_bytes_per_request = 1099511627776
max_page_size = 1000
recover_on_startup = true
recover_stale_seconds = 600
workers = 4
""" % (provider, endpoint, ak, sk, priv, pub, priv, priv, priv,
       ws_path, ws_path, ws_path, ws_path, ws_path, ws_path, ws_path, ws_path)

io.open(path, 'w', encoding='utf-8').write(src)
print('override.toml 已更新（[cloud.storage] provider=%s endpoint=%s）' % (provider, endpoint))
PYEOF

sudo chmod 644 "${OVERRIDE}"
sudo mkdir -p "${WORKSPACE_PATH}/.hfai/breakpoints" 2>/dev/null || true
echo "DONE. 记得重启平台 Pod 让配置生效： sudo kubectl -n hai-platform delete pod hai-platform-0"
