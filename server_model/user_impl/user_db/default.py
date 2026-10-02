
from __future__ import annotations

from enum import Enum

from db import MarsDB


# get_sync_status 对外返回的固定字段，与 `hai workspace list` 的 7 列表头一一对应
_SYNC_STATUS_COLUMNS = ('name', 'local_path', 'cluster_path', 'push_status', 'last_push', 'pull_status', 'last_pull')


def _db_value(value):
    """
    把枚举归一化成可以直接进 SQL 的值，非枚举原样返回。

    硬约束：绝不能把 FileType / SyncStatus / SyncDirection 这类枚举对象直接作为参数交给驱动，
    一律先取 .value（注意 `class FileType(str, Enum)` 的成员本身也是 str，很容易漏掉这一点）。
    """
    return value.value if isinstance(value, Enum) else value


class UserDbExtras:
    """
    用户 DB 访问层（同步版），与 aio_user_db/default.py 的 AioUserDbExtras 语义保持一致。

    本文件所有的 SQL 必须遵守 db/mars_db.py:210-228 对 Connection.execute 的 patch：
    1. 需要类型转换时写 CAST(%s AS type)，禁止用 `%s` 紧跟 `::` 的转型写法；
    2. 带参数的 SQL 里字面 % 必须写成 %%；本文件的 SQL 不含字面 %；
    3. 参数只能传 tuple，枚举只能传 .value。

    注意：`cloud_storage/api.py` 在后台线程里回调，因此这里必须是同步（阻塞）实现。
    """

    def set_sync_status(self, file_type, name, direction, status, local_path='', cluster_path=''):
        """
        upsert 到 "user_sync_status"，冲突键即主键 (user_name, file_type, name)。

        :param file_type: conf.utils.FileType
        :param name: workspace / env / dataset 名字
        :param direction: conf.utils.SyncDirection；push 更新 push_status/last_push，pull 更新 pull_status/last_pull
        :param status: conf.utils.SyncStatus
        :param local_path: 本地目录；传空串表示保留库里的旧值
        :param cluster_path: 集群侧目录；传空串表示保留库里的旧值
        """
        user = self.user
        direction_value = _db_value(direction)
        assert direction_value in ('push', 'pull'), f'不支持的同步方向: {direction}'
        # 列名只可能是白名单里的两个字面量，绝不拼接调用方传入的字符串
        status_column, time_column = ('push_status', 'last_push') if direction_value == 'push' \
            else ('pull_status', 'last_pull')
        sql = f'''
            insert into "user_sync_status" (
                "user_name", "user_role", "file_type", "name",
                "{status_column}", "{time_column}", "local_path", "cluster_path"
            )
            values (%s, %s, CAST(%s AS file_type), %s, CAST(%s AS sync_status), current_timestamp, %s, %s)
            on conflict ("user_name", "file_type", "name") do update set
                "user_role" = excluded."user_role",
                "{status_column}" = excluded."{status_column}",
                "{time_column}" = excluded."{time_column}",
                "local_path" = coalesce(nullif(excluded."local_path", ''), "user_sync_status"."local_path"),
                "cluster_path" = coalesce(nullif(excluded."cluster_path", ''), "user_sync_status"."cluster_path"),
                "deleted_at" = null,
                "updated_at" = current_timestamp
        '''
        params = (
            user.user_name,
            user.role,
            _db_value(file_type),
            name[:2047],
            _db_value(status),
            (local_path or '')[:2047],
            (cluster_path or '')[:2047],
        )
        MarsDB().execute(sql, params)

    def get_sync_status(self, file_type, name='*') -> list[dict]:
        """
        查询当前用户的同步状态。

        :param file_type: conf.utils.FileType
        :param name: '*' / 空 / None 表示该 file_type 下全部；否则只查该 name
        :return: list[dict]，每个 dict 恰好 7 个键：
                 name / local_path / cluster_path / push_status / last_push / pull_status / last_pull
                 （无记录时为 NULL 的字段由 to_char 返回 None）
        """
        user = self.user
        if not name:
            name = '*'
        sql = '''
            select
                "name",
                "local_path",
                "cluster_path",
                "push_status",
                to_char("last_push", 'YYYY-MM-DD HH24:MI:SS') as "last_push",
                "pull_status",
                to_char("last_pull", 'YYYY-MM-DD HH24:MI:SS') as "last_pull"
            from "user_sync_status"
            where "user_name" = %s
              and "file_type" = CAST(%s AS file_type)
              and "deleted_at" is null
              and (%s = '*' or "name" = %s)
            order by "updated_at" desc
        '''
        params = (user.user_name, _db_value(file_type), name, name)
        rows = MarsDB().execute(sql, params).fetchall()
        return [{column: row[column] for column in _SYNC_STATUS_COLUMNS} for row in rows]

    def soft_delete_sync_status(self, file_type, name):
        """
        软删（deleted_at = current_timestamp）当前用户的一条同步状态记录。

        :param file_type: conf.utils.FileType
        :param name: workspace / env / dataset 名字
        """
        user = self.user
        sql = '''
            update "user_sync_status"
            set "deleted_at" = current_timestamp
            where "user_name" = %s
              and "file_type" = CAST(%s AS file_type)
              and "name" = %s
        '''
        params = (user.user_name, _db_value(file_type), name[:2047])
        MarsDB().execute(sql, params)

    # ------------------------------------------------------------------ 记账（同步版）
    # resumable_upload_with_retry 跑在 run_in_executor 的线程里（同步语义），
    # 因此这里必须提供与 UserDownloadedFiles 上同名方法等价的同步实现。

    def insert_downloaded_file(self, file_type, file_path, file_size, file_mtime, file_md5, status):
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
        MarsDB().execute(sql, params)

    def update_downloaded_file_status(self, file_path, file_md5, status):
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
        MarsDB().execute(sql, params)
