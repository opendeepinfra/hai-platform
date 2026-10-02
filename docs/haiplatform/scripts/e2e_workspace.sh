#!/bin/bash
# hai-cli workspace 7 个子命令端到端测试
# 用法: bash e2e_workspace.sh [--stage=1|2|3]
#   stage 1: init / push / diff / list
#   stage 2: pull / download
#   stage 3: remove -f / remove
set -u

STAGE="${1:-all}"
WS_NAME="${WS_NAME:-demo}"
WS_DIR="/tmp/wsdemo"
USER_NAME=haiadmin
GROUP=hfai
CLUSTER_WS="/nfs-shared/hai-platform/workspace/${GROUP}/${USER_NAME}/workspaces/${WS_NAME}"
LOG=/tmp/e2e_workspace.log
: > "$LOG"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
ok()   { log "PASS | $*"; }
bad()  { log "FAIL | $*"; }
hr()   { log "----------------------------------------------------------"; }

hc() {
  # 以 fireflyer 身份执行 hai-cli（token 在 fireflyer 家目录）
  ( cd "$WS_DIR" && sudo -u fireflyer -- env "HOME=/home/fireflyer" hai-cli "$@" ) 2>&1 | tee -a "$LOG"
  return "${PIPESTATUS[0]}"
}

run_stage1() {
  hr; log "STAGE 1: init / push / diff / list"
  rm -rf "$WS_DIR"; mkdir -p "$WS_DIR/sub"
  printf 'hello p0\n' > "$WS_DIR/a.txt"
  head -c 4096 /dev/urandom > "$WS_DIR/sub/b.bin"
  chmod 640 "$WS_DIR/sub/b.bin"
  printf 'deep\n' > "$WS_DIR/sub/deep.txt"
  chown -R fireflyer:fireflyer "$WS_DIR"

  log ">> workspace init ${WS_NAME} -p s3"
  if hc workspace init "$WS_NAME" -p s3; then ok "init"; else bad "init"; fi
  [ -f "$WS_DIR/.hfai/workspace.yml" ] && ok "workspace.yml 生成" || bad "workspace.yml 缺失"
  cat "$WS_DIR/.hfai/workspace.yml" | tee -a "$LOG"

  log ">> workspace push"
  if hc workspace push; then ok "push"; else bad "push"; fi

  log ">> 集群侧文件清单"
  sudo ls -la "$CLUSTER_WS" 2>&1 | tee -a "$LOG"
  if [ -f "$CLUSTER_WS/a.txt" ] && [ -f "$CLUSTER_WS/sub/b.bin" ]; then
    ok "集群侧文件已落盘"
  else
    bad "集群侧文件缺失"
  fi
  local m1 m2
  m1=$(md5sum "$WS_DIR/a.txt" | awk '{print $1}')
  m2=$(sudo md5sum "$CLUSTER_WS/a.txt" 2>/dev/null | awk '{print $1}')
  [ "$m1" = "$m2" ] && ok "a.txt md5 一致 ($m1)" || bad "a.txt md5 不一致 local=$m1 cluster=$m2"

  log ">> workspace diff（push 后应立即无差异）"
  hc workspace diff && ok "diff 执行完成" || bad "diff 失败"

  log ">> workspace list"
  hc workspace list && ok "list 执行完成" || bad "list 失败"
}

run_stage2() {
  hr; log "STAGE 2: pull / download"
  # cluster_files/list 有 30s 缓存（FR-04），等待过期后再验证增量 pull
  log "等待 32s 让 list 缓存过期..."
  sleep 32
  # 在集群侧造一个新文件（模拟训练产物）
  sudo mkdir -p "$CLUSTER_WS/ckpt"
  printf 'checkpoint-data\n' | sudo tee "$CLUSTER_WS/ckpt/model.pt" >/dev/null
  sudo chown -R fireflyer:fireflyer "$CLUSTER_WS/ckpt"
  ok "集群侧新增 ckpt/model.pt"

  log ">> workspace pull"
  if hc workspace pull; then ok "pull"; else bad "pull"; fi
  if [ -f "$WS_DIR/ckpt/model.pt" ]; then
    ok "pull 取回 ckpt/model.pt"
    diff <(printf 'checkpoint-data\n') "$WS_DIR/ckpt/model.pt" >/dev/null && ok "pull 内容一致" || bad "pull 内容不一致"
  else
    bad "pull 未取回 ckpt/model.pt"
  fi
  [ -f "$WS_DIR/a.txt" ] && ok "本地独有/已有文件未被破坏" || bad "a.txt 丢失"

  log ">> workspace download ckpt"
  rm -rf "$WS_DIR/ckpt"
  if hc workspace download ckpt; then ok "download"; else bad "download"; fi
  [ -f "$WS_DIR/ckpt/model.pt" ] && ok "download 落盘 ckpt/model.pt" || bad "download 未落盘"
}

run_stage3() {
  hr; log "STAGE 3: remove -f / remove"
  log ">> workspace remove -f a.txt"
  if hc workspace remove "$WS_NAME" --yes -f a.txt; then ok "remove -f"; else bad "remove -f"; fi
  if sudo test -f "$CLUSTER_WS/a.txt"; then bad "集群侧 a.txt 仍存在"; else ok "集群侧 a.txt 已删除"; fi
  [ -f "$WS_DIR/a.txt" ] && ok "本地 a.txt 未受影响" || bad "本地 a.txt 被误删"

  log ">> workspace remove（整体）"
  if hc workspace remove "$WS_NAME" --yes; then ok "remove 整体"; else bad "remove 整体"; fi
  if sudo test -d "$CLUSTER_WS"; then bad "集群侧工作区目录仍存在"; else ok "集群侧工作区已删除"; fi

  log ">> workspace list（应不再显示）"
  hc workspace list
}

case "$STAGE" in
  1) run_stage1 ;;
  2) run_stage2 ;;
  3) run_stage3 ;;
  all) run_stage1; run_stage2; run_stage3 ;;
  *) echo "unknown stage $STAGE"; exit 2 ;;
esac

hr
log "E2E 结果统计: PASS=$(grep -c 'PASS |' "$LOG") FAIL=$(grep -c 'FAIL |' "$LOG")"
grep 'FAIL |' "$LOG" || true
