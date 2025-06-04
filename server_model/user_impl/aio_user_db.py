import time
import itertools

import sqlalchemy
import pandas as pd
from base_model.base_user import BaseUser
from conf.utils import SyncDirection, SyncStatus
from datetime import datetime
from db import sql_params, a_db_engine


class AioUserDb:
    """
    用于处理和 User 表有关的数据库操作
    """
    def __init__(self, user: BaseUser):
        self.user = user

    async def insert(self):
        """
        @return:
        """
        user = self.user
        sql = f'''
            insert into "user" ("user_id", "user_name", "token", "role", "nick_name") 
            values ('{user.user_id}', '{user.user_name}', '{user.token}', '{user.role}', '{user.nick_name}')
        '''
        try:
            async with a_db_engine.begin() as conn:
                res = await conn.execute(sqlalchemy.text(sql))
            return user
        except Exception as exp:
            print(exp)
            return None  # 运行失败

    async def insert_quota(self, resource, quota):
        """

        @param resource:
        @param quota:
        @return:
        """
        user = self.user
        _sql = """
            insert into "quota" ("user_name", "resource", "quota") 
            values (%s, %s, %s) 
            on conflict ("user_name", "resource") do update set "quota" = excluded."quota"
        """
        sql, params = sql_params.format(_sql, (user.user_name, resource, quota))
        async with a_db_engine.begin() as conn:
            await conn.execute(sqlalchemy.text(sql), params)

    async def insert_external_quota_change_log(self, external_user, resource, quota, original_quota):
        """

        :param external_user:
        :param resource:
        :param original_quota:
        :param quota:
        :return:
        """
        user = self.user
        _sql = """
             insert into "external_quota_change_log" ("editor", "external_user", "resource", "quota", "original_quota") 
             values (%s, %s, %s, %s, %s) 
        """
        sql, params = sql_params.format(_sql, (user.user_name, external_user, resource, quota, original_quota))
        async with a_db_engine.begin() as conn:
            await conn.execute(sqlalchemy.text(sql), params)

    async def insert_quotas(self, resource_quotas):
        """

        @param resource_quotas: [(resource, quota), (resource, quota)]
        @return:
        """

        user = self.user
        if len(resource_quotas) == 0:
            return
        params = ','.join([f"('{user.user_name}', %s, %s)"] * len(resource_quotas))
        _sql = f"""
            insert into "quota" ("user_name", "resource", "quota") values {params} 
            on conflict ("user_name", "resource") do update set "quota" = excluded."quota"
        """
        sql, params = sql_params.format(
            _sql, list(itertools.chain.from_iterable(resource_quotas))
        )
        async with a_db_engine.begin() as conn:
            res = await conn.execute(sqlalchemy.text(sql), params)

    async def delete_quota(self, resource):
        """

        @param resource:
        @param quota:
        @return:
        """
        user = self.user
        _sql = 'delete from "quota" where "user_name" = %s and "resource" = %s'
        sql, params = sql_params.format(_sql, (user.user_name, resource))
        async with a_db_engine.begin() as conn:
            res = await conn.execute(sqlalchemy.text(sql), params)

    async def insert_image(self, description, image_ref):
        user = self.user
        _sql = 'insert into "user_image" ("user_name", "description", "image_ref") values (%s, %s, %s) on conflict do nothing'
        sql, params = sql_params.format(_sql, (user.user_name, description, image_ref))
        async with a_db_engine.begin() as conn:
            res = await conn.execute(sqlalchemy.text(sql), params)

    async def insert_downloaded_file(self, file_type, file_path, file_size, file_mtime, file_md5, status):
        user = self.user
        assert (
            file_type and file_path and file_md5 and status and user.user_name), '必须指定file_type/file_path/file_md5/status/user.user_name'
        _sql = 'insert into "user_downloaded_files" ("user_name", "user_role", "file_type", "file_path", "file_size", "file_mtime", "file_md5", "status") values (%s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing'
        sql, params = sql_params.format(_sql, (user.user_name, user.role, file_type, file_path, file_size, file_mtime, file_md5, status))
        async with a_db_engine.begin() as conn:
            res = await conn.execute(sqlalchemy.text(sql), params)

    async def update_downloaded_file_status(self, file_path, file_md5, status):
        user = self.user
        assert (
            file_path and file_md5 and status and user.user_name), '必须指定file_path/file_md5/status/user.user_name'
        _sql = 'update "user_downloaded_files" set "status" = %s where "user_name" = %s and "file_path" = %s and "file_md5" = %s'
        sql, params = sql_params.format(_sql, (status, user.user_name, file_path, file_md5))
        async with a_db_engine.begin() as conn:
            res = await conn.execute(sqlalchemy.text(sql), params)

    async def set_sync_status(self, file_type, name, direction, status, local_path='', cluster_path='', try_insert=True):
        user = self.user
        assert (
            file_type and name and direction in [SyncDirection.PULL, SyncDirection.PUSH] and status and user.user_name), '必须指定file_type/name/direction/status/user.username'
        should_insert = False
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if try_insert:
            sql = sqlalchemy.text(f'''
                select *
                from "user_sync_status"
                where "user_name" = '{user.user_name}'
                    and "file_type" = '{file_type}'
                    and "name" = '{name}'
                    and "deleted_at" is null''')
            async with a_db_engine.begin() as conn:
                res = await conn.execute(sql)
            should_insert = len(list(res)) == 0
        if should_insert:
            state = f"'{status}', '{now}', null, null" if direction == SyncDirection.PULL else f"null, null, '{status}', '{now}'"
            sql = sqlalchemy.text(f'''
            insert into "user_sync_status" ("user_name","user_role", "file_type", "name", "pull_status", "last_pull", "push_status", "last_push", "local_path", "cluster_path")
            values
            ('{user.user_name}', '{user.role}', '{file_type}', '{name}', {state}, '{local_path}', '{cluster_path}') on conflict do nothing''')
        else:
            if status == SyncStatus.INIT:
                raise Exception(f'workspace {name} 已经初始化为其他本地目录, 请使用新的workspace')
            (sync_status_column, sync_time_column) = ('pull_status', 'last_pull') if direction == SyncDirection.PULL else ('push_status', 'last_push')
            sql = sqlalchemy.text(f'''
            update "user_sync_status"
            set "{sync_status_column}" = '{status}',
                "{sync_time_column}" = '{now}'
            where "user_name" = '{user.user_name}'
                and "file_type" = '{file_type}'
                and "name" = '{name}' ''')
        async with a_db_engine.begin() as conn:
            res = await conn.execute(sql)
    
    async def delete_sync_status(self, file_type, name):
        user = self.user
        assert (
            file_type and name and user.user_name), '必须指定file_type/name/user.username'
        sql = sqlalchemy.text(f'''
            select *
            from "user_sync_status"
            where "user_name" = '{user.user_name}'
                and "file_type" = '{file_type}'
                and "name" = '{name}'
                and "deleted_at" is null''')
        async with a_db_engine.begin() as conn:
            res = await conn.execute(sql)
        if len(list(res)) == 0:
            return
        deleted_name = f'{name}-deleted_at-{time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())}'
        sql = sqlalchemy.text(f'''
        update "user_sync_status"
        set "deleted_at" = current_timestamp,
            "name" = '{deleted_name}'
        where "user_name" = '{user.user_name}'
            and "file_type" = '{file_type}'
            and "name" = '{name}'
        ''')
        async with a_db_engine.begin() as conn:
            res = await conn.execute(sql)

    async def set_active(self, active: bool):
        user = self.user
        _sql = f'''
            UPDATE "user" SET "active" = %s
            WHERE "user_id" = %s 
        '''
        sql, params = sql_params.format(_sql, (active, user.user_id))
        async with a_db_engine.begin() as conn:
            res = await conn.execute(sqlalchemy.text(sql), params)
