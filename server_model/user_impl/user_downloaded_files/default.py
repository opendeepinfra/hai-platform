
from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from conf.utils import SyncStatus
from db import MarsDB

if TYPE_CHECKING:
    from .implement import UserDownloadedFiles


def _db_value(value):
    """
    把枚举归一化成可以直接进 SQL 的值，非枚举原样返回。

    硬约束：绝不能把 FileType / SyncStatus 这类枚举对象直接作为参数交给驱动，一律先取 .value。
    """
    return value.value if isinstance(value, Enum) else value


class UserDownloadedFilesExtras:
    """
    用户上传（集群 -> bucket）文件的记账与用量统计，对应表 "user_downloaded_files"
    （db_schemas/010.table_user_downloaded_files.sql，表已存在，P0 不做任何 DDL）。

    本文件所有的 SQL 必须遵守 db/mars_db.py:210-228 对 Connection.execute 的 patch：
    1. 需要类型转换时写 CAST(%s AS type)，禁止用 `%s` 紧跟 `::` 的转型写法；
    2. 带参数的 SQL 里字面 % 必须写成 %%；本文件的 SQL 不含字面 %；
    3. 参数只能传 tuple，枚举只能传 .value。
    """

    async def insert_downloaded_file(self, file_type, file_path, file_size, file_mtime, file_md5, status):
        """
        记账：upsert 到 "user_downloaded_files"，冲突键即主键 (file_path, file_md5)。

        :param file_type: conf.utils.FileType
        :param file_path: 文件路径（截断到列宽 2047）
        :param file_size: 文件大小（byte）
        :param file_mtime: 文件修改时间（截断到列宽 255）
        :param file_md5: 文件 md5（截断到列宽 255）
        :param status: conf.utils.SyncStatus
        """
        user = self.user
        sql = '''
            insert into "user_downloaded_files" (
                "user_name", "user_role", "file_type", "file_path",
                "file_size", "file_mtime", "file_md5", "status"
            )
            values (%s, %s, CAST(%s AS file_type), %s, %s, %s, %s, CAST(%s AS sync_status))
            on conflict ("file_path", "file_md5") do update set
                "status" = excluded."status",
                "file_size" = excluded."file_size",
                "file_mtime" = excluded."file_mtime"
        '''
        params = (
            user.user_name,
            user.role,
            _db_value(file_type),
            (file_path or '')[:2047],
            file_size,
            str(file_mtime or '')[:255],
            str(file_md5 or '')[:255],
            _db_value(status),
        )
        await MarsDB().a_execute(sql, params)

    async def update_downloaded_file_status(self, file_path, file_md5, status):
        """
        更新一条记账记录的状态（running -> finished / failed）。

        :param file_path: 与 insert_downloaded_file 相同的文件路径
        :param file_md5: 与 insert_downloaded_file 相同的 md5
        :param status: conf.utils.SyncStatus
        """
        sql = '''
            update "user_downloaded_files"
            set "status" = CAST(%s AS sync_status)
            where "file_path" = %s
              and "file_md5" = %s
        '''
        params = (_db_value(status), (file_path or '')[:2047], str(file_md5 or '')[:255])
        await MarsDB().a_execute(sql, params)

    async def get_usage_in_mb(self) -> int:
        """
        已用容量（整数 MB）。只统计 status='finished' 且未被软删的记录。

        :return: 字节数之和整除 1048576
        """
        user = self.user
        sql = '''
            select coalesce(sum("file_size"), 0)
            from "user_downloaded_files"
            where "user_name" = %s
              and "status" = CAST(%s AS sync_status)
              and "deleted_at" is null
        '''
        params = (user.user_name, _db_value(SyncStatus.FINISHED))
        res = await MarsDB().a_execute(sql, params)
        return int(res.fetchone()[0]) // 1048576

    async def get_file_count(self) -> int:
        """
        当前用户的记账记录条数（不含软删）。

        注：口径与原任务描述一致——这里不额外过滤 status（用量 get_usage_in_mb 才只算 finished）。
        """
        user = self.user
        sql = '''
            select count(*)
            from "user_downloaded_files"
            where "user_name" = %s
              and "deleted_at" is null
        '''
        res = await MarsDB().a_execute(sql, (user.user_name,))
        return int(res.fetchone()[0])
