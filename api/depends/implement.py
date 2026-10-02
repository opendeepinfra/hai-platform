
from .default import *
from .custom import *

from typing import List

import ujson
from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel

from base_model.training_task import TrainingTask
from conf.flags import TASK_TYPE, USER_ROLE
from server_model.selector import AioTrainingTaskSelector, AioUserSelector, AioBaseTaskSelector
from server_model.user import User


JUPYTER_ADMIN_GROUP = 'hub_admin'   # 能够操作其他人容器的权限组
TASK_ADMIN_GROUP = 'task_admin'     # 能够操作其他人任务的权限组


class RestTask(BaseModel):
    code_file: str
    workspace: str = ''
    token: str
    # 用于训练任务
    nb_name: str = None
    environments: dict = {}
    nodes: int = None
    group: str = None
    includes: str = ''
    template: str = 'DEFAULT'
    is_queue_job: int = 0
    priority: int = 0
    can_restart: int = 0
    task_type: str = TASK_TYPE.TRAINING_TASK
    force_restart1: int = 0
    # 用于升级任务
    upgrade_nodes: List[str] = None
    upgrade_name: str = None
    version: str = None
    include_failed: int = 0
    whole_life_state: int = 0
    mount_code: int = 2
    schedule_zone: str = None
    train_image: str = None  # 和 template 相对，指的是用户使用哪个 image 来运行
    options: dict = {}  # create_experiment 里的 options


async def request_limitation():
    if True:
        return


def get_internal_api_user_with_token(allowed_groups=[], allowed_scopes=[], **kwargs):
    return get_api_user_with_token(allowed_groups=[USER_ROLE.INTERNAL] + allowed_groups, allowed_scopes=allowed_scopes, **kwargs)


async def get_api_user_with_name(user_name: str = None):
    if user_name is None:
        raise HTTPException(status_code=401, detail={
            'success': 0,
            'msg': '非法请求'
        })
    user = await AioUserSelector.find_one(user_name=user_name)
    if user is None:
        raise HTTPException(status_code=401, detail={
            'success': 0,
            'msg': '根据user_name未找到用户'
        })
    return user


def check_user_access_to_task(task: TrainingTask, user: User, allow_shared_task=False):
    if task.task_type == TASK_TYPE.JUPYTER_TASK:
        # jupyter 任务, 允许 admin 操作
        if task.user_name != user.user_name and not user.in_any_group([JUPYTER_ADMIN_GROUP, TASK_ADMIN_GROUP]):
            raise HTTPException(status_code=403, detail='无权操作其他人的容器')
    elif task.user_name != user.user_name and not user.in_group(TASK_ADMIN_GROUP) \
            and not (allow_shared_task and user.shared_task_tag in task.tags):
        msg = f'[未授权操作] [{user.user_name}] 不能操作 [{task.user_name}] 的任务[{task.job_info}]，已经阻断!'
        raise HTTPException(status_code=403, detail=msg)


def get_api_task(check_user=True, chain_task=True, allow_shared_task=False):
    async def __func(user: User = Depends(get_api_user_with_token()), chain_id: str = None, nb_name: str = None, id: int = None) -> TrainingTask:
        aio_selector = AioTrainingTaskSelector if chain_task else AioBaseTaskSelector
        if id is not None:
            t: TrainingTask = await aio_selector.find_one(None, id=id)
        elif chain_id is not None:
            t: TrainingTask = await aio_selector.find_one(None, chain_id=chain_id)
        else:
            t: TrainingTask = await aio_selector.find_one(None, nb_name=nb_name, user_name=user.user_name)
        if t is None:
            raise HTTPException(status_code=401, detail={
                'success': 0,
                'msg': f'no task of user [{user.user_name}] with id [{id}] or chain_id [{chain_id}] or nb_name [{nb_name}]!'
            })
        if check_user:
            check_user_access_to_task(task=t, user=user, allow_shared_task=allow_shared_task)
        if t.user_name == user.user_name:
            t.user = user
        return t
    return __func


async def get_new_nb_name(nb_name: str):
    return nb_name[len('DL_CLUSTER_'):]


async def get_ugc_user(request: Request):
    '''
    /ugc/* 的鉴权依赖：身份**只**来自查询串（或 Body）里的 token（SEC-01）。

    - 客户端可能同时传 username/group/userid，这里一律忽略
    - 失败时抛带 success 的 HTTPException，客户端不会看到裸 {"detail": ...}（CON-3）
    '''
    token = request.query_params.get('token')
    if token is None:
        try:
            token = ujson.loads(await request.body()).get('token')
        except Exception:
            token = None
    if token is None:
        raise HTTPException(status_code=403, detail={
            'success': 0, 'code': 'UNAUTHORIZED', 'msg': '需要指定 token'
        })

    user = await AioUserSelector.from_token(token=token)
    if user is None:
        expired_user = await AioUserSelector.from_token(token=token, allow_expired=True)
        if expired_user is not None:
            raise HTTPException(status_code=401, detail={
                'success': 0, 'code': 'UNAUTHORIZED', 'msg': '该 token 已经过期了，请重新提供凭证'
            })
        raise HTTPException(status_code=403, detail={
            'success': 0, 'code': 'UNAUTHORIZED', 'msg': '根据 token 未找到用户'
        })
    if not user.active:
        raise HTTPException(status_code=401, detail={
            'success': 0, 'code': 'UNAUTHORIZED', 'msg': '您的账号为不活跃状态，无法访问集群服务'
        })
    return user



class API_NOTES(BaseModel):
    content: str


class ChainIds(BaseModel):
    chain_id_list: str = ''
    saved_path: str = ''
