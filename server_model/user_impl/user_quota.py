from typing import List

import pandas as pd
import sqlalchemy

from conf.flags import TASK_PRIORITY
from db import db_engine, a_db_engine
from base_model.base_user import BaseUser


class UserQuota:
    def __init__(self, user: BaseUser):
        self.user = user
        self._quota = None

    def __process_quota_df(self, df: pd.DataFrame):
        # 有用户自己的 quota，就不用别的组 quota 了
        user_own_quota = df[df.user_name == self.user.user_name]
        df = pd.concat([df[~df.resource.isin(user_own_quota.resource)], user_own_quota])[['resource', 'quota']]
        return df.groupby('resource').max()

    async def create_quota_df(self):
        sql = f'select "resource", "quota", "user_name" from "quota" where "user_name" in ({self.user.db_str_group_list})'
        async with a_db_engine.begin() as conn:
            result = await conn.execute(sqlalchemy.text(sql))
        df = pd.DataFrame(result, columns=['resource', 'quota', 'user_name'])
        self._quota = self.__process_quota_df(df)

    async def prefetch_quota_df(self):
        if self._quota is None:
            await self.create_quota_df()

    @property
    def quota_df(self):
        if self._quota is None:
            df = pd.read_sql(
                f'select "resource", "quota", "user_name" from "quota" where "user_name" in ({self.user.db_str_group_list})',
                db_engine)
            self._quota = self.__process_quota_df(df)
        return self._quota

    def quota(self, resource: str):
        if resource in self.quota_df.index:
            return self.quota_df.loc[resource].quota
        return 0

    @property
    def port_quota(self) -> int:
        return int(self.quota(resource='port'))

    @property
    def node_quota(self) -> dict:
        quota_df = self.quota_df
        return quota_df[quota_df.index.str.contains('node-')]['quota'].to_dict()

    @property
    def node_quota_limit(self) -> dict:
        quota_df = self.quota_df
        return quota_df[quota_df.index.str.contains('node_limit-')]['quota'].to_dict()

    @property
    def user_linux_group(self):
        """
            返回用户的 linux 组
        """
        df = self.quota_df
        gid_quota_df = df[df.index.str.endswith(':${gid}')]
        return [row.name.replace('${gid}', str(row.quota)) for _, row in gid_quota_df.iterrows()]

    def prefix_quotas(self, prefix):
        df = self.quota_df
        # 根据 quota 排序，在前面的是 default
        df2 = df[(df.index.str.startswith(prefix)) & (df.quota >= 1)].sort_values(by='quota', ascending=False)
        return [idx[len(prefix):].replace('${user_name}', self.user.user_name) for idx in df2.index.to_list()]

    @property
    def train_environments(self) -> List[str]:
        """
        返回用户能够看到的 train_environment
        @return: list
        """
        return self.prefix_quotas('train_environment:')

    @classmethod
    def df_to_jupyter_quota(cls, df: pd.DataFrame):
        res = {}
        for k, v in df.quota.to_dict().items():
            res[k.replace('jupyter:', '')] = {
                'cpu': int(str(v)[-5:-2]),
                'memory': int(str(v)[-9:-5]),
                'quota': int(str(v)[-2:])
            }
        return res

    @property
    def jupyter_quota(self):
        return self.__class__.df_to_jupyter_quota(self.quota_df[self.quota_df.index.str.startswith('jupyter:')])

    @property
    def api_get_all_user(self):
        """
        有没有权限获取所有的用户、任务数据
        """
        return self.quota('api_all_user') > 0

    def available_priority(self, group):
        # auto 是肯定可以用的
        res_priority_dict = {
            TASK_PRIORITY.AUTO.name: TASK_PRIORITY.AUTO.value
        }
        if not self.user.is_internal:
            # 外部用户只允许使用 auto
            return res_priority_dict
        priority_mapping = {k: v for k, v in TASK_PRIORITY.items()}
        priority_mapping.pop(TASK_PRIORITY.AUTO.name)
        priority_series = self.quota_df[
            self.quota_df.index.str.startswith(f'node-{group}-') & (self.quota_df.quota > 0)
            ].index
        if len(priority_series) > 0:
            for _, _, quota_priority in priority_series.str.split('-', expand=True):
                if priority_mapping.get(quota_priority) is not None:
                    res_priority_dict[quota_priority] = priority_mapping[quota_priority]
        return res_priority_dict
