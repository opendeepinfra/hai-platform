#!/bin/bash
# 把 env_root（{env_path}/hfai_envs）挂载进任务容器（103 测试环境专用）。
#
# 为什么需要：任务容器默认只挂载 `/nfs-shared/hai-platform/workspace/{user_name}`（见 mars_db.storage 表），
# 而 env_root 是**所有用户共享**的父目录，不在该挂载点之下。若不额外挂载，
# 任务内 `$HAIENV_PATH=/nfs-shared/.../hfai_envs/<user>` 不存在 → `source haienv` 必然失败（AC-03 前置）。
#
# 生产环境应通过 `/operating/mount_point/create`（需 ops/cluster_manager 角色）或编排仓库落地；
# 本脚本用等价的幂等 SQL 直接写 storage 表，便于 103 上复现。
#
# 用法（host 103）：bash mount_env_root.sh [env_root]
set -u

NS="${NS:-hai-platform}"
POD="${POD:-hai-platform-0}"
PGHOST_="${PGHOST_:-10.205.52.200}"
ENV_ROOT="${1:-${ENV_ROOT:-/nfs-shared/hai-platform/workspace/hfai_envs}}"

psql_() {
  sudo kubectl -n "${NS}" exec "${POD}" -- env PGPASSWORD=root \
    psql -h "${PGHOST_}" -U root -d mars_db -c "$1"
}

echo "=== 确保 env_root 存在且 777: ${ENV_ROOT}"
sudo mkdir -p "${ENV_ROOT}" && sudo chmod 777 "${ENV_ROOT}"

echo "=== 幂等插入 storage 挂载记录"
psql_ "insert into \"storage\"(\"host_path\",\"mount_path\",\"owners\",\"conditions\",\"mount_type\",\"read_only\",\"action\",\"active\")
select '${ENV_ROOT}','${ENV_ROOT}','{public}','{}','Directory',false,'add',true
where not exists (select 1 from \"storage\" where \"host_path\"='${ENV_ROOT}' and \"mount_path\"='${ENV_ROOT}');"

echo "=== 当前 Directory 类型挂载"
psql_ "select host_path, mount_path, owners, conditions, mount_type, read_only, action, active from \"storage\" where mount_type='Directory';"
