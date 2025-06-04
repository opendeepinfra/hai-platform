

import pandas as pd
import sqlalchemy
from fastapi import HTTPException

from server_model.user import User
from .query_db import QueryDB


async def get_api_user_in_query_db(token: str):
    async with QueryDB.a_query_engine.begin() as conn:
        users = [{**r} for r in await conn.execute(sqlalchemy.text(f"""
        select "user_id", "user_name", "token", "role", "user_groups", "shared_group", "nick_name"
        from "user" where "token" = '{token}'
        limit 1
        """))]
        if len(users) == 0:
            raise HTTPException(status_code=401, detail={
                'success': 0,
                'msg': '根据token未找到用户'
            })
    user = User(**users[0])
    user.group_list = users[0]['user_groups']
    sql = f'''select "resource", "quota", "user_name" from "user" where "user_name" = '{user.user_name}' '''
    async with QueryDB.a_query_engine.begin() as conn:
        result = await conn.execute(sqlalchemy.text(sql))
    user.quota._quota = pd.DataFrame(result, columns=['resource', 'quota', 'user_name'])
    return user
