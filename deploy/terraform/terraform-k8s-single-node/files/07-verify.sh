#!/usr/bin/env bash
# 07-verify.sh —— 验收：集群可用 + GPU 在 Pod 里真的能用。
#
# 关键验收点（与平台的 GPU 用法对齐）：
#   1) 节点 Ready、控制面组件齐全；
#   2) 节点容量里有 nvidia.com/gpu；
#   3) **不申请 nvidia.com/gpu 资源** 的普通 Pod，只要带 NVIDIA_VISIBLE_DEVICES
#      就能看到 GPU —— 这正是 Hai Platform 任务 Pod 的形态
#      （launcher/init_manager 只注入环境变量，不写 GPU 资源请求），
#      也就是说它验证的是 containerd 默认运行时 = nvidia 这条链路。

set -euo pipefail
source "$(dirname "$0")/lib.sh"

SMOKE_NS="${SMOKE_NS:-default}"
SMOKE_POD="${SMOKE_POD:-hai-gpu-smoke}"
KEEP_SMOKE_POD="${KEEP_SMOKE_POD:-false}"

step "A. 集群概况"
kube get nodes -o wide 2>&1 | sed 's/^/      /'
echo
kube get pods -A -o wide 2>&1 | sed 's/^/      /' || true
echo
kube get node "$NODE_NAME" -o custom-columns=\
'NAME:.metadata.name,GPU_CAP:.status.capacity.nvidia\.com/gpu,GPU_ALLOC:.status.allocatable.nvidia\.com/gpu' \
  2>&1 | sed 's/^/      /'

step "B. 准备冒烟镜像：$GPU_SMOKE_IMAGE"
if [ "${SKIP_SMOKE:-false}" = "true" ]; then
  warn "SKIP_SMOKE=true —— 只检查状态，不跑 GPU 冒烟 Pod"
  step "C. containerd 运行时复核"
  CRI_JSON="$(sudo crictl info -o json 2>/dev/null || true)"
  if [ -n "$CRI_JSON" ]; then
    printf '%s' "$CRI_JSON" | python3 -c \
      'import json,sys; c=json.load(sys.stdin)["config"]; print("      defaultRuntimeName =", c.get("defaultRuntimeName")); print("      sandboxImage =", c.get("sandboxImage"))' 2>/dev/null || true
  fi
  exit 0
fi
IMG_PRESENT=0
if sudo ctr -n k8s.io images ls -q 2>/dev/null | grep -Fxq "$GPU_SMOKE_IMAGE"; then
  IMG_PRESENT=1
  ok "containerd 已有 $GPU_SMOKE_IMAGE"
elif [ "${GPU_SMOKE_IMPORT_FROM_DOCKER}" = "true" ] \
     && command -v docker >/dev/null 2>&1 \
     && sudo docker image inspect "$GPU_SMOKE_IMAGE" >/dev/null 2>&1; then
  log "从本机 Docker 导入 $GPU_SMOKE_IMAGE 到 k8s containerd"
  sudo docker save "$GPU_SMOKE_IMAGE" | sudo ctr -n k8s.io images import - >/dev/null
  IMG_PRESENT=1
  ok "已导入"
else
  log "尝试从 registry 拉取 $GPU_SMOKE_IMAGE"
  if sudo ctr -n k8s.io images pull "$GPU_SMOKE_IMAGE" >/dev/null 2>&1; then
    IMG_PRESENT=1
    ok "已拉取"
  fi
fi
[ "$IMG_PRESENT" = "1" ] || die "无法获得冒烟镜像 $GPU_SMOKE_IMAGE"

step "C. 跑 GPU 冒烟 Pod（不申请 nvidia.com/gpu，仅注入 NVIDIA_VISIBLE_DEVICES=0）"
kube delete pod -n "$SMOKE_NS" "$SMOKE_POD" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kube apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${SMOKE_POD}
  namespace: ${SMOKE_NS}
  labels:
    app.kubernetes.io/managed-by: terraform-k8s-single-node
spec:
  restartPolicy: Never
  containers:
  - name: gpu-smoke
    image: ${GPU_SMOKE_IMAGE}
    imagePullPolicy: IfNotPresent
    env:
    - name: NVIDIA_VISIBLE_DEVICES
      value: "0"
    command: ["/bin/sh", "-c", "nvidia-smi -L; echo '--- /dev ---'; ls -1 /dev/nvidia* 2>/dev/null || echo 'no /dev/nvidia*'"]
EOF

PHASE=""
for i in $(seq 1 30); do
  PHASE="$(kube get pod -n "$SMOKE_NS" "$SMOKE_POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  case "$PHASE" in Succeeded|Failed) break ;; esac
  sleep 2
done

echo "      phase=$PHASE"
kube logs -n "$SMOKE_NS" "$SMOKE_POD" 2>&1 | sed 's/^/      /' || true

step "D. 判定"
LOG="$(kube logs -n "$SMOKE_NS" "$SMOKE_POD" 2>/dev/null || true)"
if [ "$PHASE" = "Succeeded" ] && printf '%s' "$LOG" | grep -q '^GPU '; then
  ok "Pod 内 nvidia-smi 可见 GPU：$(printf '%s' "$LOG" | grep '^GPU ' | head -1)"
else
  warn "Pod 事件："
  kube describe pod -n "$SMOKE_NS" "$SMOKE_POD" 2>&1 | sed -n '/Events:/,$p' | sed 's/^/      /' || true
  die "GPU 冒烟失败（phase=${PHASE}）——请检查 containerd 默认运行时是否为 ${DEFAULT_RUNTIME}"
fi

if [ "$KEEP_SMOKE_POD" != "true" ]; then
  kube delete pod -n "$SMOKE_NS" "$SMOKE_POD" --wait=false >/dev/null 2>&1 || true
  ok "已清理冒烟 Pod（KEEP_SMOKE_POD=true 可保留）"
fi

echo
ok "验收通过：103 单节点 K8s + GPU 就绪"
echo "      下一步（平台部署）请见 README「接平台」一节："
echo "        export KUBECONFIG=$KUBECONFIG_PATH"
echo "        kubectl-hai get nodes"
