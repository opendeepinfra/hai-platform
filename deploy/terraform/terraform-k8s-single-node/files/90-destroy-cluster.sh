#!/usr/bin/env bash
# 90-destroy-cluster.sh —— terraform destroy 时拆掉本模块创建的集群。
#
# 只拆"我们建的"：
#   * 删掉 device plugin DaemonSet 与冒烟 Pod；
#   * kubeadm reset（保留 containerd 与镜像）；
#   * 删除本模块写的 kubeconfig 副本与 kubeadm 配置；
#   * **不** 卸载 NVIDIA 驱动/工具包，**不** 删 Docker 容器，**不** 动 /root/.kube/config。

set -uo pipefail
source "$(dirname "$0")/lib.sh"

step "A. 清理工作负载（尽力而为）"
if sudo test -f "$KC" && kube get nodes >/dev/null 2>&1; then
  kube delete daemonset -n kube-system nvidia-device-plugin-daemonset --ignore-not-found >/dev/null 2>&1 || true
  kube delete pod -n default hai-gpu-smoke --ignore-not-found --wait=false >/dev/null 2>&1 || true
  ok "已删除 device plugin DaemonSet 与冒烟 Pod"
else
  warn "apiserver 不可用，跳过工作负载清理"
fi

step "B. 停止 kubelet 并 kubeadm reset"
sudo systemctl stop kubelet 2>/dev/null || true
while read -r mnt; do
  [ -n "$mnt" ] || continue
  sudo umount -f "$mnt" 2>/dev/null || true
done < <(mount | awk '/on \/var\/lib\/kubelet/ {print $3}' | sort -r)
sudo kubeadm reset -f --cri-socket unix:///run/containerd/containerd.sock >/dev/null 2>&1 || true
ok "kubeadm reset 完成"

step "C. 删除本模块写入的文件"
sudo rm -rf /etc/kubernetes/kubelet.conf /etc/kubernetes/bootstrap-kubelet.conf \
            /etc/kubernetes/pki /etc/kubernetes/manifests /etc/kubernetes/admin.conf \
            /var/lib/kubelet/* /var/lib/cni
sudo rm -f /etc/cni/net.d/10-hai-bridge.conflist "$KUBECONFIG_PATH" "$HOME/.kube/$(basename "$KUBECONFIG_PATH")" \
           /root/kubeadm-hai-single.yaml
sudo rm -f /usr/local/bin/kubectl-hai
ok "已清理 kubeconfig 副本、CNI 配置与 kubeadm 配置"

step "D. 删除本集群的 CNI 网卡"
for link in cni0; do
  ip link show "$link" >/dev/null 2>&1 && sudo ip link delete "$link" 2>/dev/null || true
done
purge_legacy_iptables
ok "CNI 网卡与 iptables 链清理完成（Docker 链未触碰）"

step "E. 复核 Docker 负载"
docker_snapshot | sed 's/^/      /'
ok "集群已拆除（NVIDIA 驱动 / containerd / Docker 保持原样）"
