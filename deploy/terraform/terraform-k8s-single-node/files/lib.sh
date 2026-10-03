#!/usr/bin/env bash
# lib.sh —— terraform-k8s-single-node 所有步骤脚本共用的函数库。
#
# 约定：
#   * 所有脚本都由 Terraform 的 local-exec 在 **103 宿主机本机** 执行（不是在你的 Mac 上）。
#   * 脚本以 SSH 登录用户（fireflyer）身份运行，需要 root 时显式 `sudo`（已免密）。
#   * 只允许触碰本集群自己的资源；**不要**动 Docker 的 iptables 链 / DOCKER 网桥，
#     因为同一台机器上的运行中容器（RustFS，平台 S3 存储）共用这个 Docker。

set -uo pipefail

C_RESET=$'\033[0m'
C_BOLD=$'\033[1m'
C_RED=$'\033[1;31m'
C_GREEN=$'\033[1;32m'
C_YELLOW=$'\033[1;33m'
C_CYAN=$'\033[1;36m'

log()  { printf '%s==>%s %s\n' "$C_CYAN" "$C_RESET" "$*"; }
step() { printf '\n%s==> %s%s\n' "$C_BOLD$C_CYAN" "$*" "$C_RESET"; }
ok()   { printf '%s[ OK ]%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn() { printf '%s[WARN]%s %s\n' "$C_YELLOW" "$C_RESET" "$*"; }
err()  { printf '%s[FAIL]%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; }
die()  { err "$*"; exit 1; }

# 变量默认值（Terraform 会用 environment 覆盖；单独手工执行时也有合理默认）。
: "${NODE_IP:=192.168.100.103}"
: "${KUBE_VERSION:=v1.29.9}"
: "${IMAGE_REPOSITORY:=registry.k8s.io}"
: "${SERVICE_SUBNET:=10.96.0.0/12}"
: "${POD_SUBNET:=10.244.0.0/16}"
: "${BRIDGE_SUBNET:=10.244.0.0/24}"
: "${BRIDGE_CNI_VERSION:=0.3.1}"
: "${MARS_PREFIX:=hai}"
: "${MARS_GROUP:=training}"
: "${DROPIN_PATH:=/etc/containerd/conf.d/99-nvidia.toml}"
: "${PAUSE_IMAGE:=registry.k8s.io/pause:3.9}"
: "${DEFAULT_RUNTIME:=nvidia}"
: "${RESTART_CONTAINERD:=true}"
: "${RESET_STALE_NODE:=true}"
: "${INSTALL_KUBECTL:=true}"
: "${KUBECTL_PATH:=/usr/local/bin/kubectl-1.29}"
: "${CNI_PLUGIN_SOURCE:=apt}"
: "${CNI_PLUGINS_TARBALL:=}"
: "${INSTALL_DEVICE_PLUGIN:=true}"
: "${DEVICE_PLUGIN_IMAGE:=nvcr.io/nvidia/k8s-device-plugin:v0.17.1}"
: "${GPU_SMOKE_IMAGE:=ubuntu:20.04}"
: "${GPU_SMOKE_IMPORT_FROM_DOCKER:=true}"
: "${KUBECONFIG_PATH:=/root/.kube/hai-single.conf}"
: "${ALLOW_DOCKER_RESTART:=false}"
: "${RESTORE_CONTAINERD_DROPIN:=true}"
: "${MIN_FREE_DISK_GB:=40}"
: "${MIN_MEM_GB:=8}"
: "${STATE_DIR:=/opt/terraform/k8s-single-node}"

# 真实节点名：优先 NODE_NAME，其次 hostname。
if [ -z "${NODE_NAME:-}" ]; then
  NODE_NAME="$(hostname)"
fi

# 统一的 kubectl 入口（本集群的 kubeconfig，绝不使用 /root/.kube/config 指向的 VM 集群）。
# 动态读取 KUBECTL_PATH：04 步骤在下载失败时会把它降级为系统 kubectl。
KC="/etc/kubernetes/admin.conf"

kube() {
  local bin="${KUBECTL_PATH:-}"
  if [ -z "$bin" ] || [ ! -x "$bin" ]; then
    bin="$(command -v kubectl || true)"
  fi
  [ -n "$bin" ] || die "找不到 kubectl"
  sudo "$bin" --kubeconfig "$KC" "$@"
}

# 生成时间戳（用于备份文件名）。
ts() { date +%Y%m%d-%H%M%S; }

# 检查某个 IP/CIDR 是否与主机上已存在的网段冲突。用法：check_subnet_conflicts 10.244.0.0/16 10.96.0.0/12
check_subnet_conflicts() {
  python3 - "$@" <<'PY'
import ipaddress, subprocess, sys

def host_nets():
    out = subprocess.run(["ip", "-4", "-o", "addr", "show"],
                         capture_output=True, text=True).stdout
    nets = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 4:
            iface, cidr = parts[1], parts[3]
            try:
                nets.append((iface, ipaddress.ip_network(cidr, strict=False)))
            except ValueError:
                pass
    return nets

bad = 0
nets = host_nets()
for arg in sys.argv[1:]:
    want = ipaddress.ip_network(arg, strict=False)
    for iface, have in nets:
        if want.overlaps(have):
            print(f"  CONFLICT: {arg} overlaps {iface} {have}")
            bad += 1
        elif str(want) != str(have):
            pass
if bad == 0:
    print("  no overlap with host interface subnets: " +
          ", ".join(f"{i}={n}" for i, n in nets))
sys.exit(1 if bad else 0)
PY
}

# 删除指定表里属于旧 CNI / kube-proxy 的链（CILIUM_* / CALI-* / KUBE-*），
# 绝不动 DOCKER* 与系统链。
purge_legacy_iptables() {
  local tables=(filter nat mangle raw) table chain
  for table in "${tables[@]}"; do
    # 先删跳转到这些链的规则，再删链本身。
    while read -r rule; do
      [ -n "$rule" ] || continue
      local del="${rule/-A /-D }"
      sudo iptables -t "$table" $del 2>/dev/null || true
    done < <(sudo iptables -t "$table" -S 2>/dev/null \
             | grep -E '^-A .* (-j|-g) (CILIUM_|CALI-|KUBE-)' || true)

    for chain in $(sudo iptables -t "$table" -S 2>/dev/null \
                   | awk '/^-N (CILIUM_|CALI-|KUBE-)/{print $2}'); do
      sudo iptables -t "$table" -F "$chain" 2>/dev/null || true
      sudo iptables -t "$table" -X "$chain" 2>/dev/null || true
    done
  done
}

# 打印 Docker 侧现状（RustFS 是平台的对象存储，必须保持存活）。
docker_snapshot() {
  if command -v docker >/dev/null 2>&1; then
    echo "--- docker ps ---"
    sudo docker ps --format '{{.Names}}\t{{.Status}}\t{{.Ports}}' 2>&1 || true
  fi
}
