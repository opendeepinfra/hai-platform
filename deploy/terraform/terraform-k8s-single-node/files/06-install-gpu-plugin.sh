#!/usr/bin/env bash
# 06-install-gpu-plugin.sh —— 部署 NVIDIA device plugin，让节点暴露 nvidia.com/gpu。
#
# 说明：Hai Platform 自己的调度器**不**通过 k8s 的 nvidia.com/gpu 申请显卡
# （GPU 靠 NVIDIA_VISIBLE_DEVICES 限卡，见 03 脚本注释）。但仍然要装 device plugin：
#   1) 让标准 k8s GPU 工作负载（申请 nvidia.com/gpu 的 Pod）能被调度；
#   2) 让 `kubectl describe node` 直接体现 GPU 资源，便于验收；
#   3) NFD 未安装时，用我们自己打的 nvidia.com/gpu.present 标签做 nodeSelector。
#
# 镜像 `nvcr.io/nvidia/k8s-device-plugin:v0.17.1` 已在 103 的 containerd 里（原 Sealos 缓存），
# 因此 imagePullPolicy=IfNotPresent，无外网也能起。

set -euo pipefail
source "$(dirname "$0")/lib.sh"

if [ "${INSTALL_DEVICE_PLUGIN:-true}" != "true" ]; then
  warn "INSTALL_DEVICE_PLUGIN=false —— 跳过 device plugin"
  exit 0
fi

step "A. 确保节点带 nvidia.com/gpu.present=true 标签"
kube label node "$NODE_NAME" nvidia.com/gpu.present=true --overwrite >/dev/null
kube get node "$NODE_NAME" -o jsonpath='{.metadata.labels.nvidia\.com/gpu\.present}{"\n"}' | sed 's/^/      /'

step "B. 应用 device plugin DaemonSet"
kube apply -f - <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: nvidia-device-plugin-daemonset
  namespace: kube-system
  labels:
    app.kubernetes.io/managed-by: terraform-k8s-single-node
spec:
  selector:
    matchLabels:
      name: nvidia-device-plugin-ds
  updateStrategy:
    type: RollingUpdate
  template:
    metadata:
      labels:
        name: nvidia-device-plugin-ds
    spec:
      priorityClassName: system-node-critical
      nodeSelector:
        nvidia.com/gpu.present: "true"
      tolerations:
      - key: CriticalAddonsOnly
        operator: Exists
      - key: nvidia.com/gpu
        operator: Exists
        effect: NoSchedule
      containers:
      - name: nvidia-device-plugin-ctr
        image: ${DEVICE_PLUGIN_IMAGE}
        imagePullPolicy: IfNotPresent
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
        volumeMounts:
        - name: device-plugin
          mountPath: /var/lib/kubelet/device-plugins
        - name: cdi
          mountPath: /var/run/cdi
      volumes:
      - name: device-plugin
        hostPath:
          path: /var/lib/kubelet/device-plugins
      - name: cdi
        hostPath:
          path: /var/run/cdi
EOF
ok "DaemonSet 已提交"

step "C. 等 nvidia.com/gpu 出现在节点容量里（最多 180s）"
EXPECTED="$(nvidia-smi -L 2>/dev/null | grep -c '^GPU ' || echo 0)"
CAP=0
for i in $(seq 1 36); do
  CAP="$(kube get node "$NODE_NAME" -o jsonpath='{.status.capacity.nvidia\.com/gpu}' 2>/dev/null || true)"
  if [ -n "$CAP" ] && [ "$CAP" -ge 1 ] 2>/dev/null; then break; fi
  sleep 5
done
kube get pods -n kube-system -l name=nvidia-device-plugin-ds 2>&1 | sed 's/^/      /' || true
if [ -n "$CAP" ] && [ "${CAP:-0}" -ge 1 ] 2>/dev/null; then
  if [ "${CAP:-0}" = "$EXPECTED" ]; then
    ok "节点容量 nvidia.com/gpu = ${CAP}（与 nvidia-smi 的 $EXPECTED 块一致）"
  else
    warn "节点容量 nvidia.com/gpu = ${CAP}，但 nvidia-smi 报告 $EXPECTED 块，请核查"
  fi
else
  warn "180s 内未看到 nvidia.com/gpu；device plugin 日志："
  kube logs -n kube-system -l name=nvidia-device-plugin-ds --tail=30 2>&1 | sed 's/^/      /' || true
  die "device plugin 未生效"
fi

step "D. 汇总"
kube get node "$NODE_NAME" -o custom-columns=\
'NAME:.metadata.name,GPU_CAP:.status.capacity.nvidia\.com/gpu,GPU_ALLOC:.status.allocatable.nvidia\.com/gpu' \
  2>&1 | sed 's/^/      /'
ok "GPU device plugin 就绪"
