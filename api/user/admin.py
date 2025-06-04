from fastapi import Depends, HTTPException

from api.app import app
from api.depends import request_limitation, get_api_user
from conf.flags import USER_ROLE, TASK_PRIORITY
from server_model.selector import UserSelector
from server_model.user import User
from scheduler.base_model.get_dfs import user_df_sql
from db import a_db_engine
import pandas as pd
import sqlalchemy
from .priority_utils import get_priority_str, check_permission


async def get_internal_user_priority_quota(user: User = Depends(get_api_user)):
    """
    用于获取外部用户的 quota 列表，和获取内部用的列表，两个接口
    :return:
    """
    check_permission(user, 'internal_quota_limit_editor')
    user_group_sql = """
    select
        "user"."user_name", array_cat(array_agg("user_group"."group") filter (where "user_group"."group" is not null), array['public', "user"."role"::varchar]) AS "user_groups"
    from "user"
    left join "user_group" on "user_group"."user_name" = "user"."user_name"
    group by "user"."user_name", "user"."role"
    """
    async with a_db_engine.begin() as conn:
        res = await conn.execute(sqlalchemy.text(user_df_sql()))
        user_group_res = await conn.execute(sqlalchemy.text(user_group_sql))
    user_df = pd.DataFrame.from_records([{**r} for r in res])
    user_group_df = pd.DataFrame.from_records([{**r} for r in user_group_res])
    user_df = pd.merge(user_df, user_group_df, on='user_name')
    user_df['user_groups'] = user_df['user_groups'].apply(lambda g: list(set(g)))
    user_df = user_df[user_df.resource.str.startswith('node') & (user_df.role == USER_ROLE.INTERNAL)
                      & (user_df.group != '') & user_df.group]
    return {
        'success': 1,
        'data': user_df.to_dict('records')
    }


async def set_user_gpu_quota_limit(internal_username: str, priority: int, group: str, quota: int,
                                   user: User = Depends(get_api_user)):
    check_permission(user, 'internal_quota_limit_editor')
    if priority < TASK_PRIORITY.BELOW_NORMAL.value:
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': '不能设置更低优先级'
        })

    priority_str = get_priority_str(priority)
    internal_user = UserSelector.from_user_name(internal_username)
    if internal_user.role != USER_ROLE.INTERNAL:
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': '只能修改外部用户的quota_limit'
        })

    resource = f'node_limit-{group}-{priority_str}'

    # 修改 quota_limit
    await internal_user.aio_db.insert_quota(resource, quota)

    return {
        'success': 1,
    }
