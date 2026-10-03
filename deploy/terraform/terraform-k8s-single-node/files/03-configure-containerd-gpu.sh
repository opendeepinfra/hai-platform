#!/usr/bin/env bash
# 03-configure-containerd-gpu.sh —— 让 k8s 的 Pod 能看到 GPU。
#
# 现状（实测）：103 的 containerd 已经有 nvidia 运行时与 CDI 规格，但
#   1) 默认运行时是 runc，而 **Hai Platform 不给任务 Pod 申请 nvidia.com/gpu**
#      （见 k8s/v1_api.py 的 get_node_resource：只设 cpu/memory/可选 rdma/hca），
#      GPU 是靠 init_manager.py 注入 NVIDIA_VISIBLE_DEVICES 来"限卡"的。
#      => 只有把 CRI 默认运行时设为 nvidia，任务容器才会拿到 /dev/nvidia*。
#   2) 旧 drop-in 里的 sandbox_image 指向已失联的 sealos.hub:5000。
#
# 本步骤：重写 /etc/containerd/conf.d/99-nvidia.toml（先备份），校验 TOML，
# 重启 containerd，并确认同机的 Docker/RustFS 未被影响。

set -euo pipefail
source "$(dirname "$0")/lib.sh"

step "A. 生成 containerd GPU drop-in"
DROPIN_DIR="$(dirname "$DROPIN_PATH")"
sudo mkdir -p "$DROPIN_DIR"
NEW_CONF="$(mktemp)"
cat >"$NEW_CONF" <<EOF
# 由 deploy/terraform/terraform-k8s-single-node 生成 —— 请勿手工编辑。
# 作用：给 CRI 提供 nvidia 运行时（默认运行时），并修正 sandbox_image。
version = 2

[plugins]

  [plugins."io.containerd.grpc.v1.cri"]
    sandbox_image = "${PAUSE_IMAGE}"
    enable_cdi = true

    [plugins."io.containerd.grpc.v1.cri".containerd]
      default_runtime_name = "${DEFAULT_RUNTIME}"
      snapshotter = "overlayfs"

      [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
        runtime_type = "io.containerd.runc.v2"

        [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
          SystemdCgroup = true

      [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.nvidia]
        runtime_type = "io.containerd.runc.v2"

        [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.nvidia.options]
          BinaryName = "/usr/bin/nvidia-container-runtime"
          SystemdCgroup = true

    [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "/etc/containerd/certs.d"
EOF

CHANGED=1
if sudo test -f "$DROPIN_PATH" && sudo diff -q "$NEW_CONF" "$DROPIN_PATH" >/dev/null 2>&1; then
  CHANGED=0
  ok "$DROPIN_PATH 内容已是最新，无需改动"
else
  # 只在还没有任何备份时备份一次，避免反复 apply 堆备份文件。
  EXISTING_BAK="$(sudo ls -1 "${DROPIN_PATH}".bak.* 2>/dev/null | head -1 || true)"
  if [ -z "$EXISTING_BAK" ] && sudo test -f "$DROPIN_PATH"; then
    BAK="${DROPIN_PATH}.bak.$(ts)"
    sudo cp -a "$DROPIN_PATH" "$BAK"
    ok "已备份原配置到 $BAK"
  elif [ -n "$EXISTING_BAK" ]; then
    ok "已存在备份 ${EXISTING_BAK}，复用（不重复备份）"
  fi
  sudo install -m 0644 "$NEW_CONF" "$DROPIN_PATH"
  ok "已写入 ${DROPIN_PATH}（默认运行时=${DEFAULT_RUNTIME}，sandbox=${PAUSE_IMAGE}）"
fi
rm -f "$NEW_CONF"

step "B. 校验配置能被打通（containerd config dump）"
if sudo containerd config dump >/dev/null 2>&1; then
  ok "containerd 配置语法校验通过"
else
  err "containerd 配置校验失败，回滚最近一次备份"
  LAST_BAK="$(sudo ls -1t "${DROPIN_PATH}".bak.* 2>/dev/null | head -1 || true)"
  if [ -n "$LAST_BAK" ]; then sudo cp -a "$LAST_BAK" "$DROPIN_PATH"; fi
  die "请检查 $DROPIN_PATH"
fi

if [ "$CHANGED" -eq 0 ]; then
  ok "配置未变化，跳过 containerd 重启"
  exit 0
fi

step "C. 重启 containerd（Docker 共用同一个 containerd）"
if [ "${RESTART_CONTAINERD:-true}" != "true" ]; then
  warn "RESTART_CONTAINERD=false —— 配置已写入但未生效，请自行重启 containerd"
  exit 0
fi
sudo systemctl restart containerd
for i in $(seq 1 20); do
  if [ -S /run/containerd/containerd.sock ] && sudo crictl info >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
sudo crictl info >/dev/null 2>&1 || die "containerd 重启后 CRI 未就绪"
ok "containerd 已重启，CRI 就绪"

step "D. 复核生效结果"
# 注意：containerd 的 CRI 把这两个值放在 config.containerd 下（实测 crictl v1.29 / containerd 2.3.3）。
CRI_JSON="$(sudo crictl info -o json 2>/dev/null || true)"
if [ -n "$CRI_JSON" ]; then
  DEFAULT_NAME="$(printf '%s' "$CRI_JSON" | python3 -c \
    'import json,sys; print(json.load(sys.stdin)["config"]["containerd"].get("defaultRuntimeName",""))' 2>/dev/null || true)"
  printf '%s' "$CRI_JSON" | python3 -c \
    'import json,sys; d=json.load(sys.stdin)["config"]["containerd"]; print("      containerd runtimes:", ", ".join(sorted(d.get("runtimes",{}))))' 2>/dev/null || true
  SANDBOX_IMG="$(sudo containerd config dump 2>/dev/null | awk -F'"' '/sandbox_image/{print $2; exit}')"
  if [ "$DEFAULT_NAME" = "$DEFAULT_RUNTIME" ]; then
    ok "CRI defaultRuntimeName = $DEFAULT_NAME"
  else
    warn "CRI defaultRuntimeName = ${DEFAULT_NAME:-未知}（期望 ${DEFAULT_RUNTIME}）"
  fi
  if [ -z "$SANDBOX_IMG" ]; then
    # containerd 2.x 的 config dump 不再输出 sandbox_image；退化为"看本地 pause 镜像"。
    PAUSE_LOCAL="$(sudo crictl images 2>/dev/null | awk '/pause/{print $1":"$2}' | tr '\n' ' ')"
    if [ -n "$PAUSE_LOCAL" ]; then
      ok "config dump 未暴露 sandbox_image；本地已有 pause 镜像：$PAUSE_LOCAL"
    else
      warn "本地没有 pause 镜像（首次建 Pod 时会去拉 ${PAUSE_IMAGE}）"
    fi
  elif [ "$SANDBOX_IMG" = "$PAUSE_IMAGE" ]; then
    ok "sandboxImage = $SANDBOX_IMG"
  else
    warn "sandboxImage = ${SANDBOX_IMG}（期望 ${PAUSE_IMAGE}）"
  fi
else
  warn "无法读取 crictl info，请手工确认 $DROPIN_PATH 是否生效"
fi

step "E. 确认同机 Docker 负载未受影响"
docker_snapshot | sed 's/^/      /'
if command -v docker >/dev/null 2>&1; then
  if sudo docker info >/dev/null 2>&1; then
    ok "docker CLI 正常（dockerd 已自动重连 containerd）"
  else
    warn "docker CLI 当前不可用。运行中的容器仍在（shim 独立），如需恢复 docker，"
    warn "请执行：sudo systemctl restart docker —— 注意这会重启所有容器（RustFS 短暂中断）"
    if [ "${ALLOW_DOCKER_RESTART:-false}" = "true" ]; then
      warn "ALLOW_DOCKER_RESTART=true，执行 sudo systemctl restart docker"
      sudo systemctl restart docker
      sleep 5
    fi
  fi
  if sudo docker ps --format '{{.Names}}' 2>/dev/null | grep -q rustfs; then
    ok "RustFS 容器仍在运行"
  fi
fi

ok "containerd GPU 运行时配置完成"
