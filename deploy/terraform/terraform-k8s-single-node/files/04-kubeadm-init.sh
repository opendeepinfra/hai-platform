#!/usr/bin/env bash
# 04-kubeadm-init.sh —— 用 kubeadm 把 103 初始化为**单机全节点**
# （control-plane + worker 同机，去污点后平台 Pod 与任务 Pod 都能调度上来）。
#
# 关键取舍：
#   * podSubnet 10.244.0.0/16、serviceSubnet 10.96.0.0/12 —— 刻意避开现有 VM 集群的
#     calico 10.1.0.0/16 与 service 10.152.183.0/24、Multipass 网桥 10.205.52.0/24、
#     Docker 172.17/172.18、ZeroTier 10.135/10.171。
#   * kubeconfig 写到 /etc/kubernetes/admin.conf 与 /root/.kube/hai-single.conf，
#     **不覆盖** /root/.kube/config（它指向 VM 集群，现有平台与脚本还在用）。
#   * 同时装一个与集群版本匹配的 kubectl（默认 /usr/local/bin/kubectl-1.29），
#     因为本机自带的 kubectl 是 v1.36，与 1.29 控制面版本偏差过大（官方 skew 外）。

set -euo pipefail
source "$(dirname "$0")/lib.sh"

step "A. 安装与集群版本匹配的 kubectl"
if [ "${INSTALL_KUBECTL:-true}" = "true" ]; then
  if [ -x "$KUBECTL_PATH" ] && "$KUBECTL_PATH" version --client 2>/dev/null | grep -q "${KUBE_VERSION#v}"; then
    ok "$KUBECTL_PATH 已存在且版本匹配"
  else
    ARCH="$(dpkg --print-architecture)"
    TMP_KC="$(mktemp /tmp/kubectl.hai-single.XXXXXX)"
    INSTALLED=0

    # ① 用户指定的本地文件（离线/内网场景）
    if [ -n "${KUBECTL_LOCAL_PATH:-}" ] && [ -f "$KUBECTL_LOCAL_PATH" ]; then
      log "使用本地 kubectl：$KUBECTL_LOCAL_PATH"
      cp -f "$KUBECTL_LOCAL_PATH" "$TMP_KC" && INSTALLED=1
    fi

    # ② 依次尝试各下载源（每个源限时 90s，避免 dl.k8s.io 慢速下载把 apply 卡死）
    if [ "$INSTALLED" != "1" ]; then
      for base in ${KUBECTL_URLS:-https://dl.k8s.io}; do
        URL="${base%/}/release/${KUBE_VERSION}/bin/linux/${ARCH}/kubectl"
        log "尝试下载 ${URL}（限时 90s）"
        if curl -fsSL --connect-timeout 8 --max-time 90 -o "$TMP_KC" "$URL" 2>/dev/null \
           && [ -s "$TMP_KC" ] && head -c 4 "$TMP_KC" | grep -q $'\x7fELF'; then
          INSTALLED=1
          break
        fi
        warn "该源不可用或超时：$URL"
      done
    fi

    if [ "$INSTALLED" = "1" ]; then
      sudo install -m 0755 "$TMP_KC" "$KUBECTL_PATH"
      ok "已安装 ${KUBECTL_PATH}（$("$KUBECTL_PATH" version --client -o json 2>/dev/null | python3 -c 'import json,sys;print(json.load(sys.stdin)["clientVersion"]["gitVersion"])' 2>/dev/null || echo "$KUBE_VERSION")）"
    else
      # ③ 降级：用系统 kubectl（本机 v1.36；与 1.29 控制面版本偏差大，但 get/apply/delete/logs 等
      #    稳定 API 可用；如需精确匹配可设置 kubectl_local_path 或 kubectl_urls）
      SYS_KC="$(command -v kubectl || true)"
      [ -n "$SYS_KC" ] || die "kubectl 下载失败且系统没有 kubectl"
      KUBECTL_PATH="$SYS_KC"
      export KUBECTL_PATH
      warn "所有下载源都失败，降级使用系统 kubectl：${SYS_KC}（$("$SYS_KC" version --client -o json 2>/dev/null | python3 -c 'import json,sys;print(json.load(sys.stdin)["clientVersion"]["gitVersion"])' 2>/dev/null || echo 未知)）"
    fi
    rm -f "$TMP_KC"
  fi
  # 便捷命令 kubectl-hai：包装脚本固定使用本集群 kubeconfig（不动系统 kubectl）。
  # 用包装脚本而不是软链，这样即使 kubectl 版本不匹配（降级到系统 v1.36）也能用。
  sudo tee /usr/local/bin/kubectl-hai >/dev/null <<WRAP
#!/bin/sh
# hai-single-node kubectl 包装：默认使用本集群 kubeconfig
# （root 与普通用户分别用各自 \$HOME/.kube/hai-single.conf）
: "\${KUBECONFIG:=\$HOME/.kube/hai-single.conf}"
exec "$KUBECTL_PATH" --kubeconfig "\$KUBECONFIG" "\$@"
WRAP
  sudo chmod 0755 /usr/local/bin/kubectl-hai
  ok "已创建 /usr/local/bin/kubectl-hai（绑定本集群 kubeconfig，底层 ${KUBECTL_PATH}）"
else
  warn "INSTALL_KUBECTL=false —— 使用系统 kubectl（版本偏差自负）"
fi

step "B. 生成 kubeadm 配置"
KUBEADM_CFG="/root/kubeadm-hai-single.yaml"
sudo tee "$KUBEADM_CFG" >/dev/null <<EOF
apiVersion: kubeadm.k8s.io/v1beta3
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: ${NODE_IP}
  bindPort: 6443
nodeRegistration:
  name: ${NODE_NAME}
  criSocket: unix:///run/containerd/containerd.sock
---
apiVersion: kubeadm.k8s.io/v1beta3
kind: ClusterConfiguration
clusterName: hai-single
kubernetesVersion: ${KUBE_VERSION}
imageRepository: ${IMAGE_REPOSITORY}
networking:
  serviceSubnet: ${SERVICE_SUBNET}
  podSubnet: ${POD_SUBNET}
  dnsDomain: cluster.local
apiServer:
  certSANs:
  - ${NODE_IP}
  - ${NODE_NAME}
  - 127.0.0.1
  - localhost
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
cgroupDriver: systemd
EOF
ok "已写入 $KUBEADM_CFG"

step "C. 预拉控制面镜像（${IMAGE_REPOSITORY}）"
if sudo kubeadm config images pull --config "$KUBEADM_CFG" 2>&1 | sed 's/^/      /'; then
  ok "控制面镜像就绪"
else
  warn "镜像预拉失败，kubeadm init 时会再试一次"
fi

step "D. kubeadm init"
if sudo test -f "$KC" && kube get nodes >/dev/null 2>&1; then
  ok "已存在可用集群（${KC}），跳过 kubeadm init"
else
  if sudo test -f /etc/kubernetes/admin.conf; then
    warn "存在 /etc/kubernetes/admin.conf 但不可用，先做一次 reset"
    sudo kubeadm reset -f --cri-socket unix:///run/containerd/containerd.sock >/dev/null 2>&1 || true
  fi
  sudo kubeadm init --config "$KUBEADM_CFG" 2>&1 | sed 's/^/      /'
  ok "kubeadm init 完成"
fi

step "E. 安装 kubeconfig（不覆盖 /root/.kube/config）"
sudo mkdir -p /root/.kube "$HOME/.kube"
sudo cp -f /etc/kubernetes/admin.conf "$KUBECONFIG_PATH"
sudo chmod 600 "$KUBECONFIG_PATH"
USER_KC="$HOME/.kube/$(basename "$KUBECONFIG_PATH")"
sudo cp -f /etc/kubernetes/admin.conf "$USER_KC"
sudo chown "$(id -u):$(id -g)" "$USER_KC"
chmod 600 "$USER_KC"
ok "root: $KUBECONFIG_PATH ; 用户: $USER_KC"
if sudo grep -q 'server:' /root/.kube/config 2>/dev/null; then
  warn "/root/.kube/config 保持原样（$(sudo grep -h 'server:' /root/.kube/config | head -1 | awk '{print $2}')）"
fi

step "F. 单机全节点：去污点 + 打分组标签"
kube taint node "$NODE_NAME" node-role.kubernetes.io/control-plane- 2>/dev/null \
  || warn "去污点失败（可能已去）"
kube label node "$NODE_NAME" "${MARS_PREFIX}_mars_group=${MARS_GROUP}" --overwrite >/dev/null
kube label node "$NODE_NAME" nvidia.com/gpu.present=true --overwrite >/dev/null

# 关键：kubeadm 会给控制面节点打上
#   node.kubernetes.io/exclude-from-external-load-balancers
# 任何 LoadBalancer 实现（MetalLB 等）都会据此**拒绝在该节点通告 LB IP**：
# 症状 = Service 拿到 EXTERNAL-IP，但局域网里 ARP 无人应答、外部完全不可达
# （MetalLB debug 日志：reason=speaker's node has labeled
#  'node.kubernetes.io/exclude-from-external-load-balancers'）。
# 单节点集群里这台机器同时是唯一工作节点，必须去掉该标签。
if kube get node "$NODE_NAME" \
     -o jsonpath='{.metadata.labels.node\.kubernetes\.io/exclude-from-external-load-balancers}' 2>/dev/null \
   | grep -q .; then
  kube label node "$NODE_NAME" node.kubernetes.io/exclude-from-external-load-balancers- >/dev/null
  ok "已移除 exclude-from-external-load-balancers（否则 LB VIP 无法在 LAN 通告）"
else
  ok "无 exclude-from-external-load-balancers 标签"
fi
ok "节点已去污点，并打上 ${MARS_PREFIX}_mars_group=${MARS_GROUP}"

step "G. 节点状态（此时 CNI 还没装，NotReady 属正常）"
kube get nodes -o wide 2>&1 | sed 's/^/      /' || true
ok "kubeadm init 阶段完成"
