from base_model.base_user import BaseUser
from conf.utils import SyncDirection, SyncStatus
from datetime import datetime
from db import db_engine


class UserDb:
    """
    用于处理和 User 表有关的数据库操作
    """
    def __init__(self, user: BaseUser):
        self.user = user

    def insert(self):
        """
        @return:
        """
        user = self.user
        sql = f'''
            insert into "user" ("user_id", "user_name", "token", "role", "nick_name") 
            values ('{user.user_id}', '{user.user_name}', '{user.token}', '{user.role}', '{user.nick_name}')
        '''
        try:
            db_engine.execute(sql)
            return user
        except Exception as exp:
            print(exp)
            return None  # 运行失败

    def insert_quota(self, resource, quota):
        """

        @param resource:
        @param quota:
        @return:
        """
        user = self.user
        sql = """
            insert into "quota" ("user_name", "resource", "quota") 
            values (%s, %s, %s) 
            on conflict ("user_name", "resource") do update set "quota" = excluded."quota"
        """
        db_engine.execute(sql, (user.user_name, resource, quota))

    def delete_quota(self, resource):
        """

        @param resource:
        @param quota:
        @return:
        """
        user = self.user
        sql = 'delete from "quota" where "user_name" = %s and "resource" = %s'
        db_engine.execute(sql, (user.user_name, resource))

    def insert_image(self, description, image_ref):
        user = self.user
        sql = 'insert into "user_image" ("user_name", "description", "image_ref") values (%s, %s, %s) on conflict do nothing'
        db_engine.execute(sql, (user.user_name, description, image_ref))

    def insert_downloaded_file(self, file_type, file_path, file_size, file_mtime, file_md5, status):
        user = self.user
        assert (
            file_type and file_path and file_md5 and status and user.user_name), '必须指定file_type/file_path/file_md5/status/user.user_name'
        sql = 'insert into "user_downloaded_files" ("user_name", "user_role", "file_type", "file_path", "file_size", "file_mtime", "file_md5", "status") values (%s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing'
        db_engine.execute(sql, (user.user_name, user.role, file_type, file_path, file_size, file_mtime, file_md5, status))

    def update_downloaded_file_status(self, file_path, file_md5, status):
        user = self.user
        assert (
            file_path and file_md5 and status and user.user_name), '必须指定file_path/file_md5/status/user.user_name'
        sql = 'update "user_downloaded_files" set "status" = %s where "user_name" = %s and "file_path" = %s and "file_md5" = %s'
        db_engine.execute(sql, (status, user.user_name, file_path, file_md5))

    def set_sync_status(self, file_type, name, direction, status):
        user = self.user
        assert (
            file_type and name and direction in [SyncDirection.PULL, SyncDirection.PUSH] and status and user.user_name), '必须指定file_type/name/direction/status/user.username'
        if status == SyncStatus.INIT:
            raise Exception(f'workspace {name} 已经初始化为其他本地目录, 请使用新的workspace')
        (sync_status_column, sync_time_column) = ('pull_status', 'last_pull') if direction == SyncDirection.PULL else ('push_status', 'last_push')
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = f'''
        update "user_sync_status"
        set "{sync_status_column}" = '{status}',
            "{sync_time_column}" = '{now}'
        where "user_name" = '{user.user_name}'
            and "file_type" = '{file_type}'
            and "name" = '{name}' '''
        db_engine.execute(sql)

    def set_active(self, active: bool):
        user = self.user
        _sql = f'''
            UPDATE "user" SET "active" = %s
            WHERE "user_id" = %s 
        '''
        db_engine.execute(_sql, (active, user.user_id))
