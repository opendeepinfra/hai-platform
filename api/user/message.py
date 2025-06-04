
from fastapi import Depends

from api.depends import get_api_user, request_limitation
from api.app import app
from server_model.user import User

@app.post('/get_user_message', dependencies=[Depends(request_limitation)])
async def get_user_message(user: User = Depends(get_api_user)):
    '''获取前端展示给用户的消息'''
    # 显式使用异步获取数据
    await user.message.fetch_user_messages()
    return {
        'success': 1,
        'messages': user.message.messages
    }