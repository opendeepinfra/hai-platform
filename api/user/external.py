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
from .priority_utils import check_permission, get_priority_str


async def get_external_user_priority_quota(user: User = Depends(get_api_user)):
    """
    用于获取外部用户的 quota 列表，和获取内部用的列表，两个接口
    :return:
    """
    check_permission(user, 'external_quota_editor')
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
    user_df = user_df[user_df.resource.str.startswith('node') & (user_df.role == USER_ROLE.EXTERNAL)
                      & (user_df.group != '') & user_df.group]
    return {
        'success': 1,
        'data': user_df.to_dict('records')
    }


async def set_external_user_quota(
        external_username: str, priority: int, group: str, quota: int,
        user: User = Depends(get_api_user)):
    check_permission(user, 'external_quota_editor')

    if priority > TASK_PRIORITY.NORMAL.value:
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': '不能设置更高优先级'
        })

    priority_str = get_priority_str(priority)
    external_user = UserSelector.from_user_name(external_username)

    if external_user.role != USER_ROLE.EXTERNAL:
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': '只能修改外部用户的quota'
        })

    resource = f'node-{group}-{priority_str}'
    await external_user.quota.create_quota_df()
    original_quota = external_user.quota.quota(resource)

    # 修改 quota
    await external_user.aio_db.insert_quota(resource, quota)

    # 存入数据库 external_quota_change_log
    await user.aio_db.insert_external_quota_change_log(external_username, resource, quota, original_quota)

    return {
        'success': 1,
    }
