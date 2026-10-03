#!/bin/bash
# 把 env_root（{env_path}/hfai_envs）挂载进任务容器。
#
# 为什么需要：任务容器默认只挂载用户的 workspace 目录（见 mars_db.storage 表），而 env_root 是
# **所有用户共享**的父目录，不在该挂载点之下。若不额外挂载，任务内
# `$HAIENV_PATH=<env_path>/hfai_envs/<user>` 不存在 → `source haienv` 必然失败。
#
# 生产环境应通过 `/operating/mount_point/create`（需 ops/cluster_manager 角色）或编排仓库落地；
# 本脚本用等价的幂等 SQL 直接写 storage 表，便于测试环境复现。
#
# 用法：
#   KUBECONFIG=~/.kube/hai-single.conf bash mount_env_root.sh /nfs-shared/hai-single/hai-platform/workspace/hfai_envs
#   KUBECONFIG=... bash mount_env_root.sh <env_root> --check      # 只查看，不写
#
# ⚠️ 多集群主机注意（踩过的坑）：本脚本早期版本固定用 `sudo kubectl`，而 sudo 会丢掉
#    KUBECONFIG 等环境变量，于是 kubectl 用 **root 的 kubeconfig** —— 在配了多套集群的机器上
#    会把挂载行写进**另一套集群**。现在默认用调用者的 kubectl（不再强制 sudo）：
#      * 需要 sudo 时显式指定：KUBECTL="sudo kubectl"
#      * 目标集群：KUBECONFIG=... （建议显式传；脚本会先打印当前 context/节点，便于确认）
#    PGHOST_ 默认 127.0.0.1（单节点/all-in-one 部署里 Postgres 就在平台 pod 内）；
#    独立数据库的部署请显式传 PGHOST_。
set -u

NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
KUBECTL="${KUBECTL:-kubectl}"
PGHOST_="${PGHOST_:-127.0.0.1}"

ARGS=()
for a in "$@"; do [ "$a" = "--check" ] || ARGS+=("$a"); done
CHECK_ONLY=0
for a in "$@"; do [ "$a" = "--check" ] && CHECK_ONLY=1; done
ENV_ROOT="${ARGS[0]:-${ENV_ROOT:-/nfs-shared/hai-single/hai-platform/workspace/hfai_envs}}"

kc() { ${KUBECTL} ${KUBECONFIG:+--kubeconfig "${KUBECONFIG}"} -n "${NS}" "$@"; }
psql_() {
  kc exec "${POD}" -- env PGPASSWORD=root psql -h "${PGHOST_}" -U root -d mars_db -c "$1"
}

echo "=== 目标集群自检（确认没打错集群）"
kc config current-context 2>/dev/null || true
kc get nodes --no-headers 2>/dev/null | head -5 || true
echo "=== env_root: ${ENV_ROOT}   pghost: ${PGHOST_}   check_only: ${CHECK_ONLY}"

if [ "${CHECK_ONLY}" = "0" ]; then
  echo "=== 确保 env_root 存在且 777"
  mkdir -p "${ENV_ROOT}" 2>/dev/null || sudo mkdir -p "${ENV_ROOT}"
  chmod 777 "${ENV_ROOT}" 2>/dev/null || sudo chmod 777 "${ENV_ROOT}"

  echo "=== 幂等插入 storage 挂载记录"
  psql_ "insert into \"storage\"(\"host_path\",\"mount_path\",\"owners\",\"conditions\",\"mount_type\",\"read_only\",\"action\",\"active\")
  select '${ENV_ROOT}','${ENV_ROOT}','{public}','{}','Directory',false,'add',true
  where not exists (select 1 from \"storage\" where \"host_path\"='${ENV_ROOT}' and \"mount_path\"='${ENV_ROOT}');"
fi

echo "=== 该 env_root 的挂载记录（应为 1 行）"
psql_ "select host_path, mount_path, owners, mount_type, read_only, action, active from \"storage\"
       where \"mount_type\"='Directory' and \"host_path\"='${ENV_ROOT}';"

echo "=== 当前所有 Directory 类型挂载"
psql_ "select host_path, mount_path, owners, mount_type, read_only, action, active from \"storage\" where mount_type='Directory';"
