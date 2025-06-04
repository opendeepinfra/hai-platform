

import os

from .api_config import get_mars_token as mars_token
from .api_config import get_mars_url as mars_url
from .api_utils import async_requests, RequestMethod


# 得暂时兼容老的 client
async def create_jupyter_port_svc(usage: str, dist_port: int, **kwargs):
    return {
        'success': 0,
        'msg': "接口已经删除了"
    }


async def get_jupyter_memory_metrics(**kwargs):
    return {
        'success': 0,
        'msg': "接口已经删除了"
    }


async def jupyter_swap_memory(enable: bool, **kwargs):
    return {
        'success': 0,
        'msg': "接口已经删除了"
    }
