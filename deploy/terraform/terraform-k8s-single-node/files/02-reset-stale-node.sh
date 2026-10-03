#!/usr/bin/env bash
# 02-reset-stale-node.sh —— 清掉 103 上"陈旧 Sealos 节点"的残留。
#
# 背景：103 原本是某个 Sealos 集群的 GPU worker（kubelet v1.29.9 + cilium +
# NFD + nvidia device plugin 的镜像都还在本机），但那个集群的控制面
# （apiserver.cluster.local → 10.103.97.2:6443）已经不可达，kubelet 一直在刷
# `i/o timeout`，节点早在集群里消失。要把它变成"单机全节点"，必须先做干净复位。
#
# 安全边界（很重要）：
#   * **不** 全量 flush iptables —— 同机 Docker（RustFS，平台 S3）的 DOCKER* 链必须保留；
#     只清理 KUBE-* / CILIUM_* / CALI-* 这些旧 CNI / kube-proxy 自己的链。
#   * **不** 动 NVIDIA 驱动、nvidia-container-toolkit、containerd 本体、Docker。
#   * **不** 动 /root/.kube/config（它指向 VM 集群，现有平台还在用）。
#   * /etc/hosts 只删 4 行 Sealos 遗留（先备份）。

set -euo pipefail
source "$(dirname "$0")/lib.sh"

if [ "${RESET_STALE_NODE:-true}" != "true" ]; then
  warn "RESET_STALE_NODE=false —— 跳过陈旧节点清理"
  exit 0
fi

step "A. 备份 /etc/hosts 并清理 Sealos 遗留解析"
# 复用已有备份，避免每次 apply 都堆一个新备份文件。
HOSTS_BAK="$(sudo ls -1 /etc/hosts.hai-single.*.bak 2>/dev/null | head -1 || true)"
if [ -z "$HOSTS_BAK" ]; then
  HOSTS_BAK="/etc/hosts.hai-single.$(ts).bak"
  sudo cp -a /etc/hosts "$HOSTS_BAK"
  ok "已备份到 $HOSTS_BAK"
else
  ok "已存在备份 ${HOSTS_BAK}，复用（不重复备份）"
fi
for entry in 'sealos.hub' 'apiserver.cluster.local' 'lvscare.node.ip' 'hai.redoop.com'; do
  if sudo grep -qE "[[:space:]]${entry//./\\.}([[:space:]]|$)" /etc/hosts; then
    sudo sed -i -E "/[[:space:]]${entry//./\\.}([[:space:]]|$)/d" /etc/hosts
    ok "已删除 /etc/hosts 中的 $entry"
  fi
done

step "B. 停止旧 kubelet，卸载 /var/lib/kubelet 下的挂载"
sudo systemctl stop kubelet 2>/dev/null || true
while read -r mnt; do
  [ -n "$mnt" ] || continue
  sudo umount -f "$mnt" 2>/dev/null || true
done < <(mount | awk '/on \/var\/lib\/kubelet/ {print $3}' | sort -r)

step "C. kubeadm reset（只清理它自己注册的规则与目录）"
sudo kubeadm reset -f --cri-socket unix:///run/containerd/containerd.sock 2>&1 \
  | sed 's/^/      /' || warn "kubeadm reset 返回非 0（陈旧节点通常如此，继续）"
ok "kubeadm reset 完成"

step "C2. 清理旧集群遗留的 sandbox / 容器（reset 常因 CRI 超时清不掉）"
if timeout 60 bash -c 'sudo crictl pods -q 2>/dev/null | xargs -r sudo crictl rmp -f >/dev/null 2>&1'; then
  ok "遗留 sandbox 已清理"
else
  warn "sandbox 清理超时（多为已 Exited 的陈旧容器，不影响后续 init）"
fi
timeout 60 bash -c 'sudo crictl ps -aq 2>/dev/null | xargs -r sudo crictl rm -f >/dev/null 2>&1' || true
LEFT="$(sudo crictl pods -q 2>/dev/null | wc -l)"
echo "      剩余 sandbox 数：$LEFT"

step "D. 删除陈旧控制面/节点目录"
for d in /etc/kubernetes/kubelet.conf /etc/kubernetes/bootstrap-kubelet.conf \
         /etc/kubernetes/pki /etc/kubernetes/manifests /etc/kubernetes/node-feature-discovery; do
  sudo rm -rf "$d"
done
sudo rm -rf /var/lib/kubelet/* /var/lib/cni /etc/cni/net.d /var/lib/etcd
sudo mkdir -p /etc/cni/net.d /var/lib/kubelet
ok "已清理 /etc/kubernetes 运行时文件、/var/lib/kubelet、/etc/cni/net.d"

step "E. 删除旧 CNI 虚拟网卡"
for link in cilium_host cilium_net cilium_vxlan lxc_cilium cni0 flannel.1 kube-ipvs0; do
  if ip link show "$link" >/dev/null 2>&1; then
    sudo ip link delete "$link" 2>/dev/null || true
    ok "已删除网卡 $link"
  fi
done
# lxc* 通配（旧 cilium/calico 的 veth）
for link in $(ip -o link show | awk -F': ' '/: lxc/{print $2}' | cut -d@ -f1); do
  sudo ip link delete "$link" 2>/dev/null || true
  ok "已删除网卡 $link"
done

step "F. 定向清理旧 CNI / kube-proxy 的 iptables 链（保留 DOCKER*）"
purge_legacy_iptables
ok "旧 CNI/kube-proxy 链清理完成（Docker 链未触碰）"

step "G. 复核：Docker 负载仍然存活"
docker_snapshot | sed 's/^/      /'
if command -v docker >/dev/null 2>&1; then
  if sudo docker ps --format '{{.Names}}' 2>/dev/null | grep -q .; then
    ok "Docker 容器仍在运行"
  else
    warn "当前没有运行中的 Docker 容器（若平台依赖 RustFS，请确认它是否本来就没起）"
  fi
fi

ok '陈旧节点复位完成：现在是一台"干净"的机器，等待 kubeadm init'
