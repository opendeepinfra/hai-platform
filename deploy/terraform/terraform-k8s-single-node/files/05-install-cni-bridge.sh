#!/usr/bin/env bash
# 05-install-cni-bridge.sh —— 装最小可用 CNI（bridge + host-local + portmap）。
#
# 为什么用极简 bridge 而不是 Cilium/Calico：
#   * 单节点不需要跨节点路由/隧道；平台 Pod 与任务 Pod 都在同一台机器上；
#   * 零镜像依赖：Ubuntu 的 containernetworking-plugins 包即可（华为云镜像可达）；
#   * 不与 103 上已有的 Docker 网桥（172.17/172.18）、Multipass 网桥（10.205.52.0/24）、
#     ZeroTier（10.135/10.171）以及 VM 集群的 calico 10.1.0.0/16 打架。
#
# 需要 NetworkPolicy / 原 Sealos 环境一致的 CNI 时，可改用 Cilium
# （本机 containerd 已缓存 cilium v1.13.4 镜像），见 README「换成 Cilium」。

set -euo pipefail
source "$(dirname "$0")/lib.sh"

step "A. 准备 CNI 插件二进制（/opt/cni/bin）"
NEED_PLUGINS="bridge host-local loopback portmap"
if [ "${CNI_PLUGIN_SOURCE}" = "tarball" ]; then
  [ -n "${CNI_PLUGINS_TARBALL}" ] || die "CNI_PLUGIN_SOURCE=tarball 时必须给 CNI_PLUGINS_TARBALL"
  TMP="$(mktemp -d)"
  log "下载 $CNI_PLUGINS_TARBALL"
  curl -fsSL -o "$TMP/cni.tgz" "$CNI_PLUGINS_TARBALL"
  sudo mkdir -p /opt/cni/bin
  sudo tar -xzf "$TMP/cni.tgz" -C /opt/cni/bin --strip-components=1 2>/dev/null \
    || sudo tar -xzf "$TMP/cni.tgz" -C /opt/cni/bin
  rm -rf "$TMP"
  ok "CNI 插件已解包到 /opt/cni/bin"
else
  MISSING=""
  for p in $NEED_PLUGINS; do
    [ -x "/opt/cni/bin/$p" ] || MISSING="$MISSING $p"
  done
  if [ -n "$MISSING" ]; then
    log "缺少插件:$MISSING —— 用 apt 安装 containernetworking-plugins"
    sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq containernetworking-plugins
  fi

  # 关键：Ubuntu/Debian 的 containernetworking-plugins 把二进制装到 /usr/lib/cni，
  # 而 containerd 的默认 bin_dir 是 /opt/cni/bin（本机 config 里 bin_dir 为空 = 用默认值）。
  # 因此要把缺失的插件从包目录补到 /opt/cni/bin。
  for d in /usr/lib/cni /usr/libexec/cni; do
    [ -d "$d" ] || continue
    for p in $MISSING; do
      if [ ! -x "/opt/cni/bin/$p" ] && [ -x "$d/$p" ]; then
        sudo install -m 0755 "$d/$p" "/opt/cni/bin/$p"
        ok "已从 $d 安装插件 $p → /opt/cni/bin/$p"
      fi
    done
  done

  for p in $NEED_PLUGINS; do
    [ -x "/opt/cni/bin/$p" ] || die "安装后仍缺少 /opt/cni/bin/${p}（可改用 cni_plugin_source=tarball 指定官方发布包）"
  done
  ok "CNI 插件就绪：$NEED_PLUGINS"
fi

step "B. 写 CNI 配置（唯一配置文件，避免与残留冲突）"
sudo rm -f /etc/cni/net.d/*.conf /etc/cni/net.d/*.conflist
CNI_CONF=/etc/cni/net.d/10-hai-bridge.conflist
sudo tee "$CNI_CONF" >/dev/null <<EOF
{
  "cniVersion": "${BRIDGE_CNI_VERSION}",
  "name": "hai-bridge",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "cni0",
      "isGateway": true,
      "ipMasq": true,
      "hairpinMode": true,
      "ipam": {
        "type": "host-local",
        "ranges": [[{ "subnet": "${BRIDGE_SUBNET}" }]],
        "routes": [{ "dst": "0.0.0.0/0" }]
      }
    },
    {
      "type": "portmap",
      "capabilities": { "portMappings": true }
    }
  ]
}
EOF
ok "已写入 ${CNI_CONF}（subnet=${BRIDGE_SUBNET}）"

step "C. 等节点 Ready（最多 180s）"
READY=0
for i in $(seq 1 36); do
  STATUS="$(kube get node "$NODE_NAME" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  if [ "$STATUS" = "True" ]; then READY=1; break; fi
  sleep 5
done
kube get nodes -o wide 2>&1 | sed 's/^/      /'
[ "$READY" = "1" ] || die "节点在 180s 内未 Ready，请检查 kubelet：journalctl -u kubelet -n 100"
ok "节点 $NODE_NAME 已 Ready"

step "D. 集群组件状态"
kube get pods -A -o wide 2>&1 | sed 's/^/      /' || true
ok "CNI 安装完成"
