#!/bin/bash
# 在宿主机上用 docker 拉起 RustFS（S3 兼容对象存储），并建好平台需要的 bucket。
#
# 背景：hai-platform 的 cloud_storage 需要 S3 兼容存储。本测试环境选 RustFS。
# 注意：镜像内rustfs 以 uid/gid 10001 运行，数据卷属主必须匹配，否则容器起不来。
#
# 用法（凭证只从环境变量取，仓库不落真实密钥）:
#   RUSTFS_AK=xxx RUSTFS_SK=yyy bash deploy_rustfs.sh
#
# 可用环境变量:
#   RUSTFS_AK / RUSTFS_SK   必填，对象存储访问凭证
#   RUSTFS_API_PORT         S3 API 端口，默认 19000
#   RUSTFS_CONSOLE_PORT     控制台端口，默认 19001
#   RUSTFS_DATA_DIR         数据目录，默认 /opt/rustfs/data
#   RUSTFS_IMAGE            镜像，默认 rustfs/rustfs:latest
#   RUSTFS_BUCKETS          要建的 bucket，默认 hai-platform-private hai-platform-public
#
# 提示：若宿主机 9000/9001 已被占用（例如已有一套原生 rustfs），本脚本默认用 19000/19001；
#      平台侧 [cloud.storage].endpoint 必须与 RUSTFS_API_PORT 保持一致。
set -e

: "${RUSTFS_AK:?请先 export RUSTFS_AK=...（对象存储访问凭证）}"
: "${RUSTFS_SK:?请先 export RUSTFS_SK=...（对象存储访问凭证）}"

API_PORT="${RUSTFS_API_PORT:-19000}"
CONSOLE_PORT="${RUSTFS_CONSOLE_PORT:-19001}"
DATA_DIR="${RUSTFS_DATA_DIR:-/opt/rustfs/data}"
IMAGE="${RUSTFS_IMAGE:-rustfs/rustfs:latest}"
BUCKETS="${RUSTFS_BUCKETS:-hai-platform-private hai-platform-public}"

echo "=== data dir : ${DATA_DIR}"
echo "=== ports    : ${API_PORT}(api) ${CONSOLE_PORT}(console)"
echo "=== buckets  : ${BUCKETS}"

echo "STEP: pull image"
sudo docker pull "${IMAGE}"

echo "STEP: prepare data dir"
sudo mkdir -p "${DATA_DIR}"
# 镜像内 rustfs 用户 uid/gid = 10001
sudo chown -R 10001:10001 "${DATA_DIR}"

echo "STEP: (re)start container"
sudo docker rm -f rustfs >/dev/null 2>&1 || true
sudo docker run -d --name rustfs --restart unless-stopped \
  -p "0.0.0.0:${API_PORT}:9000" -p "0.0.0.0:${CONSOLE_PORT}:9001" \
  -v "${DATA_DIR}:/data" \
  -e "RUSTFS_ACCESS_KEY=${RUSTFS_AK}" \
  -e "RUSTFS_SECRET_KEY=${RUSTFS_SK}" \
  -e RUSTFS_VOLUMES=/data \
  "${IMAGE}"

echo "STEP: wait for S3 API"
for i in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${API_PORT}/" || echo "000")
  echo "  attempt $i http=${code}"
  # 403/200 都说明 S3 API 已就绪（403 = 需要签名）
  [ "$code" = "403" ] || [ "$code" = "200" ] && break
  sleep 2
done
sudo docker ps --filter name=rustfs --format '{{.Names}} {{.Status}} {{.Ports}}'

echo "STEP: create buckets"
python3 - "$API_PORT" $BUCKETS <<'PYEOF' || echo "WARN: 建 bucket 失败（缺 boto3？）——请手动创建：${BUCKETS}"
import sys
port, buckets = sys.argv[1], sys.argv[2:]
try:
    import boto3
    from botocore.config import Config
except ImportError:
    sys.exit("no boto3")
import os
c = boto3.client('s3', endpoint_url='http://127.0.0.1:%s' % port,
                 aws_access_key_id=os.environ['RUSTFS_AK'],
                 aws_secret_access_key=os.environ['RUSTFS_SK'],
                 region_name='us-east-1', verify=False,
                 config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))
existing = {b['Name'] for b in c.list_buckets()['Buckets']}
for b in buckets:
    if b in existing:
        print('exists :', b)
    else:
        c.create_bucket(Bucket=b)
        print('created:', b)
PYEOF

echo "DONE. 记得把 endpoint=http://<本机可达IP>:${API_PORT} 写进 override.toml 的 [cloud.storage]"
echo "      （可用 config_cloud_storage.sh 生成该段配置）"
