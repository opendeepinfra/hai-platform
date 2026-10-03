-- 把 env root（{env_path}/hfai_envs）挂进任务 pod —— 幂等，可重复执行。
--
-- 为什么必须挂：任务容器默认只挂载「用户 workspace 目录」这一条（mars_db.storage 里的
-- Directory 行，或从 spec.workspace 推出的 runtime mount）。env root 是所有用户共享的父目录，
-- 不在其下；不额外挂载时，任务内 HAIENV_PATH（= {env_path}/hfai_envs/<user>，由
-- server_model/task_impl/single_task_impl.py:61-62 设置）不存在，`source haienv <name>`
-- 必然失败：FileNotFoundError …/hfai_envs → "no valid env found" → 任务里没有 torch。
--
-- 用法（单节点/all-in-one 部署，Postgres 在平台 pod 内）：
--   kubectl -n hai-platform exec hai-platform-0 -- \
--     env PGPASSWORD=root psql -h 127.0.0.1 -U root -d mars_db -f mount_env_root.sql
--   或：KUBECONFIG=~/.kube/hai-single.conf \
--       bash docs/haiplatform/scripts/mount_env_root.sh /nfs-shared/hai-single/hai-platform/workspace/hfai_envs
--
-- 建议在部署初始化里调用（本仓库 04-hai-up.sh 已有 psql -d mars_db -c 的钩子，可加一行 -f 本文件；
-- 注意那个文件当前有未提交改动，故未直接改它）。
--
-- 若部署的 env_path 不同（见 override.toml 的 [cloud.storage.service] env_path），
-- 把下面的路径换成 '{env_path}/hfai_envs'。

insert into "storage"("host_path", "mount_path", "owners", "conditions",
                      "mount_type", "read_only", "action", "active")
select '/nfs-shared/hai-single/hai-platform/workspace/hfai_envs',
       '/nfs-shared/hai-single/hai-platform/workspace/hfai_envs',
       '{public}', '{}', 'Directory', false, 'add', true
where not exists (select 1 from "storage"
                  where "host_path" = '/nfs-shared/hai-single/hai-platform/workspace/hfai_envs'
                    and "mount_type" = 'Directory');

-- 自检：应输出 1 行
select host_path, mount_path, owners, mount_type, read_only, action, active
from "storage"
where "mount_type" = 'Directory'
  and "host_path" = '/nfs-shared/hai-single/hai-platform/workspace/hfai_envs';
