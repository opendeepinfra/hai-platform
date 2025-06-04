from .api_config import get_mars_url as mars_url
from .api_config import get_mars_token as mars_token
from .api_utils import async_requests, RequestMethod


async def get_user_message(**kwargs):
    """
    获取用户的存储路径
    """
    token = kwargs.get('token', mars_token())
    url = f'{mars_url()}/monitor_v2/get_user_message?token={token}'
    result = await async_requests(RequestMethod.POST, url)
    return result['messages']
