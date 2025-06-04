

import traceback

from fastapi import Body
from fastapi import Depends

from server_model.user import User
from . import query_v1
from .depends import get_api_user_in_query_db


async def uni_query_api(query_body: dict = Body(default={}), user: User = Depends(get_api_user_in_query_db)):
    """
    统一的 query 接口，可以同时查 user task resource
    """
    try:
        version = query_body['version']
    except Exception:
        return {
            'success': 0,
            'msg': '必须指定 version'
        }
    try:
        query_func = {
            '1': query_v1.query
        }
        result = await query_func[str(version)](query_body, user)
        return {
            'success': 1,
            'result': result
        }
    except Exception as e:
        traceback.print_exc()
        return {
            'success': 0,
            'msg': str(e)
        }
