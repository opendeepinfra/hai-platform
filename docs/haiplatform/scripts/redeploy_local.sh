#!/bin/bash
# 把「本机构建好的镜像」导入 MicroK8s 的 containerd 并部署到 StatefulSet。
#
# 为什么需要它：本测试环境的 registry 推送没有凭据（docker push 报 insufficient_scope），
# 所以走「docker save → multipass transfer → microk8s.ctr images import」这条旁路，
# 并把 StatefulSet 的 imagePullPolicy 设为 IfNotPresent。
#
# 用法:
#   bash redeploy_local.sh <tag>
#
# 可用环境变量覆盖（默认值对应当前 103 测试环境）:
#   REGISTRY   镜像仓库前缀   默认 registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform
#   NS         k8s 命名空间   默认 hai-platform
#   STS        StatefulSet    默认 hai-platform
#   CONTAINER  容器名         默认 hai-platform
#   VM         承载 Pod 的 VM 默认 k8s-master
#   OVERRIDE   override.toml  默认 /nfs-shared/hai-platform/override.toml（用于同步 manager_image）
#   OWNER      文件属主       默认当前用户（multipass 必须以非 root 用户运行）
set -e

TAG="${1:?usage: redeploy_local.sh <tag>}"
REGISTRY="${REGISTRY:-registry.cn-hangzhou.aliyuncs.com/opendeepinfra/hai-platform}"
IMG="${REGISTRY}:${TAG}"
NS="${NS:-hai-platform}"
STS="${STS:-hai-platform}"
CONTAINER="${CONTAINER:-hai-platform}"
VM="${VM:-k8s-master}"
OVERRIDE="${OVERRIDE:-/nfs-shared/hai-platform/override.toml}"
OWNER="${OWNER:-$(id -un)}"
TAR_HOME="${HOME}/hai-${TAG}.tar"

echo "=== redeploy tag : ${TAG}   $(date +%T)"
echo "=== image        : ${IMG}"

echo "STEP: docker save"
sudo rm -f "${TAR_HOME}"
sudo docker save "${IMG}" -o "${TAR_HOME}"
sudo chown "${OWNER}" "${TAR_HOME}"
ls -la "${TAR_HOME}"

echo "STEP: transfer to ${VM}   $(date +%T)"
multipass transfer "${TAR_HOME}" "${VM}:/home/ubuntu/hai-${TAG}.tar"

echo "STEP: ctr images import   $(date +%T)"
multipass exec "${VM}" -- sudo microk8s.ctr images import "/home/ubuntu/hai-${TAG}.tar"
multipass exec "${VM}" -- sudo rm -f "/home/ubuntu/hai-${TAG}.tar"
multipass exec "${VM}" -- sudo microk8s.ctr images ls -q | grep "${TAG}"

if sudo test -f "${OVERRIDE}"; then
  echo "STEP: sync manager_image in ${OVERRIDE}   $(date +%T)"
  sudo python3 - "${TAG}" "${OVERRIDE}" "${REGISTRY}" <<'PYEOF'
import io, re, sys
tag, path, registry = sys.argv[1], sys.argv[2], sys.argv[3]
src = io.open(path, encoding='utf-8').read()
new, n = re.subn(r"(manager_image\s*=\s*')[^']*(')", r"\g<1>%s:%s\g<2>" % (registry, tag), src)
if n:
    io.open(path, 'w', encoding='utf-8').write(new)
    print('manager_image -> %s:%s' % (registry, tag))
else:
    print('WARN: manager_image not found, skipped')
PYEOF
fi

echo "STEP: patch statefulset   $(date +%T)"
sudo kubectl -n "${NS}" patch statefulset "${STS}" --type=json \
  -p "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/image\",\"value\":\"${IMG}\"},{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/imagePullPolicy\",\"value\":\"IfNotPresent\"}]"

echo "STEP: recreate pod   $(date +%T)"
sudo kubectl -n "${NS}" delete pod "${STS}-0" --wait=true --timeout=120s || true

echo "STEP: wait for ready   $(date +%T)"
for i in $(seq 1 60); do
  ready=$(sudo kubectl -n "${NS}" get pod "${STS}-0" -o jsonpath="{.status.containerStatuses[0].ready}" 2>/dev/null || echo "")
  echo "  attempt $i ready=$ready"
  [ "$ready" = "true" ] && break
  sleep 10
done
sudo kubectl -n "${NS}" get pod -o wide
sudo kubectl -n "${NS}" exec "${STS}-0" -- supervisorctl status 2>&1 | head -12
rm -f "${TAR_HOME}"
echo "REDEPLOY DONE: ${TAG}"
