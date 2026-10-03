#!/usr/bin/env bash
# 01-preflight.sh —— 只读前置检查。
#
# 本脚本 **不修改任何系统状态**：不启停服务、不写配置文件、不动 iptables。
# 任何不满足的硬条件都会让 terraform apply 失败，避免"改到一半才发现卡住"。
#
# 可以用 `./create.sh --preflight-only`（等价 `terraform apply -target=null_resource.preflight`）
# 单独跑这一步做干跑验证。

set -uo pipefail
source "$(dirname "$0")/lib.sh"

FAILED=0
fail() { err "$*"; FAILED=$((FAILED + 1)); }

step "1/9 GPU 与 NVIDIA 运行时"
if command -v nvidia-smi >/dev/null 2>&1; then
  GPU_LINES="$(nvidia-smi -L 2>/dev/null | grep -c '^GPU ' || true)"
  if [ "${GPU_LINES:-0}" -ge 1 ]; then
    ok "检测到 ${GPU_LINES} 块 GPU"
    nvidia-smi -L | sed 's/^/      /'
  else
    fail "nvidia-smi -L 没有列出任何 GPU"
  fi
  nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null \
    | sed 's/^/      driver: /'
else
  fail "找不到 nvidia-smi（NVIDIA 驱动未安装？）"
fi

for f in /usr/bin/nvidia-container-runtime /usr/bin/nvidia-ctk /etc/cdi/nvidia.yaml; do
  if [ -e "$f" ]; then ok "$f 存在"; else fail "$f 缺失"; fi
done

step "2/9 containerd 与 CRI"
if systemctl is-active --quiet containerd; then
  ok "containerd 运行中（$(containerd --version 2>/dev/null | awk '{print $3}')）"
else
  fail "containerd 未运行"
fi
if sudo grep -q '^imports' /etc/containerd/config.toml 2>/dev/null; then
  ok "/etc/containerd/config.toml 已启用 conf.d imports"
else
  warn "/etc/containerd/config.toml 未启用 imports；本模块会直接改写 $DROPIN_PATH 之外的配置"
fi
if [ -S /run/containerd/containerd.sock ]; then
  ok "CRI socket /run/containerd/containerd.sock 存在"
else
  fail "CRI socket /run/containerd/containerd.sock 不存在"
fi

step "3/9 kubeadm / kubelet 版本"
KUBEADM_V="$(kubeadm version -o short 2>/dev/null | tr -d 'v' || true)"
KUBELET_V="$(kubelet --version 2>/dev/null | awk '{print $2}' | tr -d 'v' || true)"
WANT_V="${KUBE_VERSION#v}"
if [ "$KUBEADM_V" = "$WANT_V" ]; then ok "kubeadm $KUBEADM_V 与目标一致"
else fail "kubeadm=$KUBEADM_V 与 KUBE_VERSION=$WANT_V 不一致"; fi
if [ "$KUBELET_V" = "$WANT_V" ]; then ok "kubelet $KUBELET_V 与目标一致"
else fail "kubelet=$KUBELET_V 与 KUBE_VERSION=$WANT_V 不一致"; fi

step "4/9 端口占用（控制面需要空闲）"
for p in 6443 2379 2380 10257 10259; do
  if sudo ss -lntH "( sport = :$p )" 2>/dev/null | grep -q .; then
    fail "端口 $p 已被占用：$(sudo ss -lntpH "( sport = :$p )" | head -1 | sed 's/^ *//')"
  else
    ok "端口 $p 空闲"
  fi
done
if sudo ss -lntH '( sport = :10250 )' 2>/dev/null | grep -q .; then
  warn "10250 已被 kubelet 占用（陈旧 Sealos 节点），apply 时会先 reset 再重新 init"
fi

step "5/9 网段冲突检查（避免踩到 VM 集群 / Docker / ZeroTier）"
if check_subnet_conflicts "$POD_SUBNET" "$SERVICE_SUBNET" "$BRIDGE_SUBNET"; then
  ok "pod/service/bridge 网段与主机现有网段不冲突"
else
  fail "网段与主机现有网段冲突，请调整 pod_subnet / service_subnet / bridge_subnet"
fi

step "6/9 内核与资源"
if [ "$(sysctl -n net.ipv4.ip_forward)" = "1" ]; then ok "net.ipv4.ip_forward=1"
else fail "net.ipv4.ip_forward != 1"; fi
if [ "$(sysctl -n net.bridge.bridge-nf-call-iptables 2>/dev/null || echo 0)" = "1" ]; then
  ok "net.bridge.bridge-nf-call-iptables=1"
else
  fail "net.bridge.bridge-nf-call-iptables != 1（bridge CNI + Service 需要它）"
fi
if [ -n "$(swapon --show --noheadings 2>/dev/null)" ]; then
  fail "交换分区已启用，kubelet 默认会拒绝启动"
else
  ok "未启用 swap"
fi

FREE_DISK_GB="$(df -BG --output=avail / | tail -1 | tr -dc '0-9')"
# 注意：103 的 locale 是中文，free 的标题行是「内存：」，所以按行号取而不是按标题匹配。
MEM_GB="$(free -g | awk 'NR==2{print $2}')"
[ "${FREE_DISK_GB:-0}" -ge "$MIN_FREE_DISK_GB" ] \
  && ok "根分区可用 ${FREE_DISK_GB}G ≥ ${MIN_FREE_DISK_GB}G" \
  || fail "根分区可用 ${FREE_DISK_GB}G < ${MIN_FREE_DISK_GB}G"
[ "${MEM_GB:-0}" -ge "$MIN_MEM_GB" ] \
  && ok "内存 ${MEM_GB}G ≥ ${MIN_MEM_GB}G" \
  || fail "内存 ${MEM_GB}G < ${MIN_MEM_GB}G"

step "7/9 现状：陈旧节点与既有 kubeconfig（只报告，不改动）"
if [ -f "$KC" ]; then
  warn "$KC 已存在：说明本机已有 kubeadm 集群；apply 会按 RESET_STALE_NODE=${RESET_STALE_NODE} 决定是否重建"
fi
if [ -f /etc/kubernetes/kubelet.conf ]; then
  SRV="$(sudo grep -h 'server:' /etc/kubernetes/kubelet.conf 2>/dev/null | head -1 | awk '{print $2}')"
  warn "陈旧 kubelet 配置存在，指向 ${SRV:-未知 apiserver}"
fi
if [ -f /root/.kube/config ]; then
  SRV="$(sudo grep -h 'server:' /root/.kube/config 2>/dev/null | head -1 | awk '{print $2}')"
  warn "/root/.kube/config 指向 ${SRV:-未知}；本模块 **不会** 覆盖它（本集群用 ${KUBECONFIG_PATH}）"
fi
for f in /etc/cni/net.d/*; do
  [ -e "$f" ] && warn "残留 CNI 配置：$f"
done

step "8/9 共享 containerd 的 Docker 负载（重启 containerd 的影响面）"
if pgrep -x dockerd >/dev/null 2>&1; then
  DOCKER_CTR="$(ps -o args= -C dockerd | grep -o -- '--containerd=[^ ]*' | head -1)"
  warn "Docker 正在运行（${DOCKER_CTR:-未显式指定 containerd}）"
  if [ "${DOCKER_CTR:-}" = "--containerd=/run/containerd/containerd.sock" ]; then
    warn "Docker 与 k8s **共用同一个 containerd**：改运行时配置需要重启 containerd；"
    warn "运行中的容器不会被杀（shim 独立），但 docker CLI 可能需要重启 dockerd（会重启容器）"
  fi
  docker_snapshot | sed 's/^/      /'
else
  ok "未运行 Docker，无额外影响面"
fi

step "9/9 依赖下载可达性"
APT_HOST="$(grep -rh '^deb ' /etc/apt/sources.list /etc/apt/sources.list.d/*.list 2>/dev/null \
            | awk '{print $2}' | sed -E 's#^[a-z]+://([^/]+)/.*#\1#' | head -1)"
for h in "$IMAGE_REPOSITORY" dl.k8s.io "${APT_HOST:-archive.ubuntu.com}"; do
  [ -n "$h" ] || continue
  if timeout 8 bash -c "cat </dev/null >/dev/tcp/$h/443" 2>/dev/null \
     || timeout 8 bash -c "cat </dev/null >/dev/tcp/$h/80" 2>/dev/null; then
    ok "$h 可达"
  else
    warn "$h 不可达（若已有本地镜像/离线包可忽略）"
  fi
done

echo
if [ "$FAILED" -eq 0 ]; then
  ok "前置检查全部通过，可以 apply"
else
  die "前置检查有 ${FAILED} 项失败，请先修复"
fi
