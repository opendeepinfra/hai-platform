-- ---------------------------------------------------------------------------
-- hai-cli images（用户自定义镜像）：`file_type` 枚举补 `image`（上传通道前置，S8-2 / OPS-08）
--
-- 背景：`file_type` 是 PostgreSQL enum（db_schemas/010.table_user_downloaded_files.sql:8），
-- 原值为 workspace/dataset/env/doc/pypi/website。上传通道（`images push`）的 stage1/stage2
-- 会通过 /ugc/set_sync_status 与 sync_to_cluster 写 `user_sync_status.file_type='image'`，
-- 未迁移前会直接报 `invalid input value for enum file_type: "image"`（用例 FI-12 的预期症状）。
--
-- 本文件必须**幂等**：init_postgresql.sh 每次启动都会全量重放 db_schemas/*.sql
-- （见 deploy/dbs/files/init_postgresql.sh 的说明），重复执行不得报错（对齐 035 的做法）。
--
-- 两点注意：
-- 1) PG 不支持删除枚举值，因此本迁移**只加不删**；三级回滚（逆迁移）按设计 §9.5 明确不做。
-- 2) `alter type ... add value` 在 PostgreSQL 12 之前**不能**出现在事务块内，因此这里写成
--    单条语句（init_postgresql.sh 用 `psql -f`，不带 `-1`，每条语句独立提交），
--    也不要包进 `do $$ ... $$`。
-- ---------------------------------------------------------------------------

alter type public.file_type add value if not exists 'image';

comment on type public.file_type is
    '用户文件 / 同步类型: workspace/dataset/env/doc/pypi/website/image（image = 用户自定义镜像，由 hai-cli images 上传通道写入）';
