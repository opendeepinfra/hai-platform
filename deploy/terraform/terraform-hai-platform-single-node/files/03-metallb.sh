#!/usr/bin/env bash
# 03-metallb.sh —— 单节点集群上的 LoadBalancer 实现（MetalLB L2）。
#
# 为什么需要：one/hai-up.sh 给平台建的是 `type: LoadBalancer` 的 hai-platform-svc，
# 没有 LoadBalancer 实现时 EXTERNAL-IP 永远是 <pending>，hai-up/override.toml 里的
# HAI_SERVER_ADDR（数据库/redis/api_server 地址）就没法填一个稳定可达的地址。
#
# 单节点 L2 是可行的：speaker 直接在本机网卡上应答 ARP。
# 地址池选 192.168.100.150/32 —— 该网段是 Mac 与 103 互通的网段，
# 于是**平台地址从 Mac 直接可达**（不像 VM 集群的 10.205.52.200 需要隧道/nginx 代理）。
#
# 镜像：speaker v0.14.9 本机 containerd 已缓存（原 Sealos 留下），controller 需联网拉取。

set -euo pipefail
source "$(dirname "$0")/lib.sh"

step "A. 安装 MetalLB $METALLB_VERSION"
if kube get ns metallb-system >/dev/null 2>&1; then
  ok "metallb-system 已存在（跳过安装）"
else
  MANIFEST="https://raw.githubusercontent.com/metallb/metallb/${METALLB_VERSION}/config/manifests/metallb-native.yaml"
  log "apply $MANIFEST"
  kube apply -f "$MANIFEST" | tail -5
  # 官方清单里的 memberlist secret 是固定 starter key，轮换掉（与本模块无关但更安全）
  kube create secret generic -n metallb-system memberlist \
    --from-literal=secretkey="$(openssl rand -base64 128)" --dry-run=client -o yaml \
    | kube apply -f - >/dev/null 2>&1 || true
fi
kube -n metallb-system rollout status deployment/controller --timeout=180s
kube -n metallb-system rollout status daemonset/speaker --timeout=180s
ok "MetalLB controller/speaker 就绪"

step "B. 配置地址池与 L2 通告"
kube apply -f - <<EOF | sed 's/^/      /'
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: hai-single-pool
  namespace: metallb-system
spec:
  addresses:
  - ${METALLB_IP_RANGE}
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: hai-single-l2
  namespace: metallb-system
spec:
  ipAddressPools:
  - hai-single-pool
EOF
kube -n metallb-system get ipaddresspool, l2advertisement 2>/dev/null | sed 's/^/      /' || true
ok "地址池就绪：${METALLB_IP_RANGE}"
