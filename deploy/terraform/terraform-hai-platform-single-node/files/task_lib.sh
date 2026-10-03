#!/usr/bin/env bash
# task_lib.sh —— 通过 hai-cli 提交平台任务并等待结果的公共逻辑（π 测试 / GPU 测试共用）。
#
# 隔离要点：hai-cli 的配置默认写在 ~/.hfai/conf.yml（103 上它现在指向 VM 平台）。
# 这里统一用 HFAI_CLIENT_CONFIG 指到 ${HAI_CONF_DIR}/conf.yml（见 client/api/api_config.py:10），
# 因此**不会覆盖** VM 平台的 hai-cli 配置。

set -uo pipefail
source "$(dirname "$0")/lib.sh"

TOKEN="${USER_INFO##*:}"                 # haiadmin:10020:123456 -> 123456
HFAI_CONF="${HAI_CONF_DIR}/conf.yml"

# hai-cli 包装：以 fireflyer 身份 + 独立配置目录运行
hcli() {
  sudo -u fireflyer env HFAI_CLIENT_CONFIG="$HFAI_CONF" hai-cli "$@"
}

# 准备隔离配置目录并登录
hcli_login() {
  local lb="$1"
  sudo mkdir -p "$HAI_CONF_DIR"
  sudo chown -R fireflyer:fireflyer "$HAI_CONF_DIR"
  hcli init "$TOKEN" --url "http://$lb" >/dev/null 2>&1 || true
  # hai-cli init 会把 URL 规范成带尾斜杠（http://x.y/），而下游拼成 "{url}/query/..."
  # 会产生 "//query/..."，haproxy 的 path_beg /query/ 匹配不上 -> 503。这里去掉尾斜杠。
  sudo -u fireflyer sh -c "sed -i -E 's#^([[:space:]]*url:[[:space:]]*).*#\\1http://$lb#' '$HFAI_CONF'" 2>/dev/null || true
  ok "hai-cli 已登录（配置：${HFAI_CONF}）"
}

# 把任务脚本写到平台共享工作区（任务 Pod 会挂载该目录）
stage_task_script() {
  local src="$1" name="$2"
  TASK_DIR="${HAI_DIR}/workspace/${ROOT_USER}/jupyter/notebooks/${name}_$(date +%s)"
  sudo mkdir -p "$TASK_DIR"
  sudo cp -f "$src" "$TASK_DIR/$(basename "$src")"
  sudo chmod -R a+rwX "$TASK_DIR"
  echo "$TASK_DIR/$(basename "$src")"
}

# 提交并返回 task id
hcli_submit() {
  local script_path="$1" name="$2"
  local out tid
  out="$(hcli python "$script_path" -- --nodes 1 -g "$TRAINING_GROUP" --name "$name" -f 2>&1)"
  # ⚠️ 回显必须走 stderr：本函数的 stdout 只允许是 task id。
  # 否则调用方 TASK_ID="$(hcli_submit ...)" 会把整张表 + 首行 WARNING 当成 task id，
  # 后续 `hcli status "$TASK_ID"` 必然查不到任务（曾实测：状态查询全部报"连接错误"）。
  echo "$out" | sed 's/^/      /' >&2
  # 提交结果是一张表，第一列即 task id；**不能**用 grep -oE '[0-9]{4,}'
  # （路径里的时间戳会先被匹配到）。
  tid="$(echo "$out" | sed -nE 's/^\|[[:space:]]*([0-9]+)[[:space:]]*\|.*/\1/p' | head -1)"
  [ -n "$tid" ] || return 1
  echo "$tid"
}

# 等待任务进入终态，回显 "<chain_status> <job_status>"
hcli_wait() {
  local tid="$1" tries="${2:-90}" i s cs js
  for i in $(seq 1 "$tries"); do
    s="$(hcli status "$tid" -j 2>/dev/null | tr -d '\n' || true)"
    cs="$(echo "$s" | grep -oE '"chain_status": *"[^"]*"' | head -1 | sed -E 's/.*"([^"]*)"$/\1/')"
    js="$(echo "$s" | grep -oE '"status": *"[^"]*"'       | head -1 | sed -E 's/.*"([^"]*)"$/\1/')"
    echo "      ($i/$tries) task $tid chain=${cs:-?} job=${js:-?}" >&2
    case "$js" in
      succeeded|failed|stopped) echo "$cs $js"; return 0 ;;
    esac
    case "$cs" in
      finished|failed|stopped) echo "$cs $js"; return 0 ;;
    esac
    sleep 10
  done
  echo "timeout ${js:-none}"
  return 1
}
