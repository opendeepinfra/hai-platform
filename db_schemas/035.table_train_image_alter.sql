-- ---------------------------------------------------------------------------
-- hai-cli images（用户自定义镜像）：train_image 表补齐控制面所需的列与唯一键
-- 设计 docs/haiplatform/images/images-server-design.md §3 / §7；任务列表 S3-1
--
-- 本文件必须**幂等**：init_postgresql.sh 每次启动都会全量重放 db_schemas/*.sql
-- （见 deploy/dbs/files/init_postgresql.sh 的说明），重复执行不得报错。
-- ---------------------------------------------------------------------------

-- 1) 新增列：message（失败原因 / 状态说明）、user_name（谁加载的，Q-8）
alter table public.train_image
    add column if not exists "message" varchar not null default '';

alter table public.train_image
    add column if not exists "user_name" varchar not null default '';

comment on column public.train_image.message is '状态说明 / 失败原因（API-16 回报写入）';
comment on column public.train_image.user_name is '加载该镜像的用户（审计用；镜像按 shared_group 共享）';

-- 2) 唯一键由 (image_tar) 改为 (shared_group, image_tar)（Q-2 / R-4）
--    旧索引必须删除：两个组加载同一个 tar 在旧索引下必然冲突。
drop index if exists public.train_image_image_uindex;

--    新建索引包在 DO 块里 fail-soft：若历史数据已存在重复行，唯一索引创建失败时
--    只告警、不让整个迁移重放失败（否则平台 pod 起不来）。重复行需人工清理后重建。
do $$
begin
    begin
        create unique index if not exists train_image_group_tar_uindex
            on public.train_image (shared_group, image_tar);
    exception when others then
        raise warning 'train_image_group_tar_uindex 创建失败（可能存在重复的 shared_group+image_tar 行），请人工清理后重试: %', sqlerrm;
    end;
end $$;

-- 3) 状态列注释（状态机：processing/loading/loaded/failed/deleted，服务端单点写入）
comment on column public.train_image.status is
    '镜像状态机: processing/loading/loaded/failed/deleted；任务侧只认精确 loaded，客户端按子串 deleted 过滤';
