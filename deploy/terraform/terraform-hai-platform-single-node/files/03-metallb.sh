#!/usr/bin/env bash
# 03-metallb.sh —— 单节点集群上的 LoadBalancer 实现（MetalLB L2）。
#
# 为什么需要：one/hai-up.sh 给平台建的是 `type: LoadBalancer` 的 hai-platform-svc，
# 没有 LoadBalancer 实现时 EXTERNAL-IP 永远是 <pending>，override.toml 里的
# HAI_SERVER_ADDR（数据库/redis/api_server 地址）就没有稳定可达的地址。
#
# 三个实战要点（都是本环境实测踩出来的）：
#   1) **去掉 node.kubernetes.io/exclude-from-external-load-balancers 标签**：
#      kubeadm 会给控制面节点打这个标签，MetalLB 据此拒绝通告 LB IP ——
#      症状是 EXTERNAL-IP 有了但局域网 ARP 无人应答（speaker debug 日志：
#      reason=speaker's node has labeled 'node.kubernetes.io/exclude-from-...'）。
#      单节点必须删掉。
#   2) 地址池用**区间写法**（`a.b.c.d-a.b.c.d`），并显式写 `interfaces` 把通告钉在 LAN 网卡上
#      —— 103 有 enp8s0/enp9s0/InfiniBand/ZeroTier/docker 等多张网卡。
#   3) speaker v0.14.9 镜像本机 containerd 已缓存（原 Sealos 留下），controller 需联网拉。

set -euo pipefail
source "$(dirname "$0")/lib.sh"

step "A. 安装 MetalLB ${METALLB_VERSION}"
if kube get ns metallb-system >/dev/null 2>&1; then
  ok "metallb-system 已存在（跳过安装）"
else
  MANIFEST="https://raw.githubusercontent.com/metallb/metallb/${METALLB_VERSION}/config/manifests/metallb-native.yaml"
  log "apply ${MANIFEST}"
  kube apply -f "$MANIFEST" | tail -5
  kube create secret generic -n metallb-system memberlist \
    --from-literal=secretkey="$(openssl rand -base64 128)" --dry-run=client -o yaml \
    | kube apply -f - >/dev/null 2>&1 || true
fi
kube -n metallb-system rollout status deployment/controller --timeout=180s
kube -n metallb-system rollout status daemonset/speaker --timeout=180s
ok "MetalLB controller/speaker 就绪"

step "B. 去掉控制面节点的 exclude-from-external-load-balancers 标签"
if kube get node "${NODE_NAME}" \
     -o jsonpath='{.metadata.labels.node\.kubernetes\.io/exclude-from-external-load-balancers}' 2>/dev/null \
   | grep -q .; then
  kube label node "${NODE_NAME}" node.kubernetes.io/exclude-from-external-load-balancers- >/dev/null
  ok "已移除（否则 speaker 拒绝通告 LB IP，LAN 里 ARP 无人应答）"
else
  ok "标签不存在，无需处理"
fi

step "C. 配置地址池与 L2 通告（网卡：${METALLB_INTERFACE}）"
# 把 "192.168.100.150/32" 归一化成区间写法
POOL_RANGE="${METALLB_IP_RANGE}"
case "${POOL_RANGE}" in
  */*) POOL_RANGE="${POOL_RANGE%/*}-${POOL_RANGE%/*}" ;;
esac
kube apply -f - <<EOF | sed 's/^/      /'
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: hai-single-pool
  namespace: metallb-system
spec:
  addresses:
  - ${POOL_RANGE}
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: hai-single-l2
  namespace: metallb-system
spec:
  ipAddressPools:
  - hai-single-pool
  interfaces:
  - ${METALLB_INTERFACE}
EOF
kube -n metallb-system get ipaddresspool 2>/dev/null | sed 's/^/      /' || true
ok "地址池就绪：${POOL_RANGE}（仅在 ${METALLB_INTERFACE} 上通告）"
