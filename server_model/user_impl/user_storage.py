

import tokenize
import uuid
from io import BytesIO

import pandas as pd
from cached_property import cached_property
from sqlalchemy import text

from base_model.base_user import BaseUser
from db import db_engine, a_db_engine


class STORAGE_ACTION:
    ADD = 'add'
    REMOVE = 'remove'


class UserStorage:
    def __init__(self, user: BaseUser):
        self.user = user
        self._storage = None

    # 注意 k8sworker 复用了这个逻辑，改动的时候要小心
    def safe_eval(self, s, task=None):
        # tokenize，判断输入是否合法
        g = tokenize.tokenize(BytesIO(s.encode('utf-8')).readline)
        tokens = []
        for tokenum, tokval, _, _, _ in g:
            try:
                assert tokenum in {
                    tokenize.NUMBER, tokenize.STRING, tokenize.OP, tokenize.NAME, tokenize.ENCODING,
                    tokenize.ENDMARKER, tokenize.NEWLINE
                }, f'token 非法: {tokval}'
                if tokenum == tokenize.NAME:
                    if tokval not in {'and', 'or', 'is', 'None', 'task', 'user'}:
                        # 如果不在这里，那必须之前是 task. / user.
                        assert tokens[-1] == '.', f'当前 token 是 {tokval}，前一个必须是 .'
                        assert tokens[-2] in {'task', 'user'}, f'当前 token 是 {tokval}，前前一个必须是 task / user'
            except Exception as e:
                # token 有问题，这条作废
                return
            tokens.append(tokval)
        try:
            # 正常，直接 eval
            return eval(s, {'task': task, 'user': self.user})
        except Exception as e:
            # eval 出错了，这条作废
            return

    def process_storage(self, df):
        # 解析 storage，按照以下规则排序
        # rank0 有 condition 用户挂载点 (最优先)
        # rank1 无 condition 用户挂载点
        # rank2 有 condition 用户组挂载点
        # rank3 无 condition 用户组挂载点
        # rank 已经在 sql 操作中打好了，但是 sql 比较难做取交集的操作
        user_group_set = set(self.user.db_str_group_list.replace("'", '').split(','))
        df['owners'] = df.owners.apply(lambda o: list(set(o) & user_group_set))
        return df

    def storage_sql(self, where=None):
        if where is None:
            where = f'where owners && array[{self.user.db_str_group_list}]::varchar[]'
        # 这里来 sort，可以看看 process_storage
        return text(f'''
            select
                "host_path", "mount_path", "owners", "conditions",
                "mount_type", "read_only", "action", "rank", "need_task"
            from (
                 select
                    "host_path", "mount_path", "owners", "conditions",
                    "mount_type", "read_only", "action", "active",
                    case
                        when "owners" @> array['{self.user.user_name}']::varchar[] and "conditions" != array[]::varchar[] then 0
                        when "owners" @> array['{self.user.user_name}']::varchar[] and "conditions" = array[]::varchar[] then 1
                        when not "owners" @> array['{self.user.user_name}']::varchar[] and "conditions" != array[]::varchar[] then 2
                        else 3
                    end as "rank",
                    case
                        when "host_path" like '%{{task.%' then true
                        when "mount_path" like '%{{task.%' then true
                        when "conditions"::text like '%task.%' then true
                        else false
                    end as "need_task"
                from "storage"
                {where}
            ) as "tmp"
            where "active"
            order by "rank" DESC 
        ''')

    async def create_storage_df(self):
        if self._storage is None:
            async with a_db_engine.begin() as conn:
                results = await conn.execute(self.storage_sql())
            self._storage = pd.DataFrame.from_records([{**r} for r in results])
            self._storage = self.process_storage(self._storage)
        return self._storage

    @cached_property
    def storage_df(self):
        if self._storage is None:
            self._storage = pd.read_sql(self.storage_sql(), db_engine)
            self._storage = self.process_storage(self._storage)
        return self._storage

    def personal_storage(self, task=None):
        if task is None:
            # 没有 task，就把 need_task 的挂载点给删了
            storage_df = self.storage_df[~self._storage.need_task].copy()
        else:
            storage_df = self.storage_df.copy()
        res = {}
        # 依次遍历每一个 rank
        for _, df in storage_df.groupby('rank', sort=False):
            rank_res = {}
            dup_mounts = []
            # 先执行 remove
            for _, row in df[df.action == STORAGE_ACTION.REMOVE].iterrows():
                if not all(self.safe_eval(c, task=task) for c in row.conditions):
                    continue
                mount_path = self.safe_eval(f"f'{row.mount_path}'", task=task)
                # 解析出问题了，直接跳过
                if mount_path is None:
                    continue
                if res.get(mount_path) is not None:
                    res.pop(mount_path)
            # 再执行 add
            for _, row in df[df.action == STORAGE_ACTION.ADD].iterrows():
                if not all(self.safe_eval(c, task=task) for c in row.conditions):
                    continue
                mount_path = self.safe_eval(f"f'{row.mount_path}'", task=task)
                # 解析出问题了，直接跳过
                if mount_path is None:
                    continue
                # 出错了，同一个 rank 有相同的 mount_path 定义
                if rank_res.get(mount_path):
                    dup_mounts.append(mount_path)
                host_path = self.safe_eval(f"f'{row.host_path}'", task=task)
                # 解析出问题了，直接跳过
                if host_path is None:
                    continue
                rank_res[mount_path] = {
                    'host_path': host_path,
                    'mount_path': mount_path,
                    'mount_type': row.mount_type,
                    'read_only': row.read_only,
                    'name': uuid.uuid4().hex
                }
            # 同一个 rank 有重复的，直接删了
            res = {**res, **{k: v for k, v in rank_res.items() if k not in dup_mounts}}
        return list(res.values())
