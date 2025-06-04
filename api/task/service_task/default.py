

from fastapi import HTTPException

from server_model.selector import AioTrainingTaskSelector, AioBaseTaskSelector, AioUserSelector
from server_model.user import User


JUPYTER_ADMIN_GROUP = 'hub_admin'


async def get_user(token: str = None):
    user = await AioUserSelector.find_one(token=token)
    if user is None:
        raise HTTPException(status_code=200, detail={
            'success': 0,
            'msg': '根据token未找到用户'
        })
    return user


async def get_task(id: int):
    task = await AioTrainingTaskSelector.find_one(None, id=id)
    if task is None:
        raise HTTPException(status_code=200, detail={
            'success': 0,
            'msg': '未找到 task'
        })
    return task


async def get_base_task(id: int):
    task = await AioBaseTaskSelector.find_one(None, id=id)
    if task is None:
        raise HTTPException(status_code=200, detail={
            'success': 0,
            'msg': '未找到 task'
        })
    return task


def check_user_task(user: User, task=None):
    # 有 task 就判断是不是自己的 task
    if (task and task.user_name == user.user_name) or user.in_group(JUPYTER_ADMIN_GROUP):
        return
    raise HTTPException(status_code=403, detail='无权限进行此操作!')


async def checkpoint_api():
    return {
        'success': 1,
        'result': "success",
        'msg': 'not implemented'
    }
