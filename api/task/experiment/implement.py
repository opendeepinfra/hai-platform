

from .default import *
from .custom import *

import datetime
import inspect
import json
import time
import urllib
from typing import Optional, List
from fastapi import Depends, Request, Query

from logm import logger
from api.depends import get_api_user_with_token, get_api_task
from api.task_schema import TaskSchema
from api.operation import operate_task_base, create_task_base_queue_v2
from base_model.training_task import TrainingTask
from conf.flags import TASK_OP_CODE, TASK_PRIORITY, QUE_STATUS, STOP_CODE
from db import MarsDB
from db import a_redis as redis
from server_model.auto_task_impl import AutoTaskApiImpl
from server_model.task_impl import AioDbOperationImpl
from server_model.training_task_impl import TaskApiImpl, DashboardApiImpl
from server_model.user import User
from server_model.task_runtime_config import TaskRuntimeConfig
from utils import convert_to_external_node, convert_to_external_task


async def create_task_v2(task_schema: TaskSchema, request: Request,
                         user: User = Depends(get_api_user_with_token())):
    raw_task_schema = await request.json()  # 原始的任务配置
    res = await create_task_base_queue_v2(user=user, task_schema=task_schema, raw_task_schema=raw_task_schema, remote_apply=True)
    return res


async def resume_task(
    task: TrainingTask = Depends(get_api_task()),
    user: User = Depends(get_api_user_with_token())
):
    task.re_impl(AioDbOperationImpl)
    try:
        await redis.delete(f'ban:{task.user_name}:{task.nb_name}:{task.chain_id}')
        task = await task.resume(remote_apply=True)
    except Exception as e:
        if "already exists" in str(e):
            return {
                'success': 0,
                'msg': f'您名字为 {task.nb_name} 的任务正在运行，不能重复创建，请稍后重试'
            }
        else:
            logger.exception(e)
            return {
                'success': 0,
                'msg': '未能在数据库中成功创建队列，请联系系统组',
            }
    task: TrainingTask = convert_to_external_task(task)
    return {
        'success': 1,
        'msg': '直接插入队列成功，请等待调度',
        'task': task.trait_dict()
    }


async def stop_task(op: TASK_OP_CODE = TASK_OP_CODE.STOP, task: TrainingTask = Depends(get_api_task())):
    await redis.set(f'ban:{task.user_name}:{task.nb_name}:{task.chain_id}', 1)
    res = await operate_task_base(operate_user=task.user_name, task=task, task_op_code=op, remote_apply=False)
    return res


async def suspend_task_by_name(
        task: TrainingTask = Depends(get_api_task(check_user=False)),
        user: User = Depends(get_api_user_with_token()),
        restart_delay: int = 0,
):
    operate_user = task.user_name if user.in_group('suspend_task') else user.user_name   # 允许管理员打断其他人的任务
    res = await operate_task_base(operate_user=operate_user, task=task, task_op_code=TASK_OP_CODE.SUSPEND, restart_delay=restart_delay, remote_apply=False)
    return res


async def task_node_log_api(task: TrainingTask = Depends(get_api_task()), rank: int = 0, last_seen: str = 'null', service: str = None):
    try:
        last_seen = json.loads(last_seen)
    except:
        last_seen = None
    if last_seen:
        try:
            last_seen['timestamp'] = datetime.datetime.strptime(last_seen['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
        except:
            last_seen['timestamp'] = datetime.datetime.strptime(last_seen['timestamp'], "%Y-%m-%dT%H:%M:%S")
    task.re_impl(AutoTaskApiImpl)
    res = await task.log(rank, last_seen=last_seen, service=service)
    # 兜底逻辑，任务没启动就失败了，日志文件都没有，标记 stop
    if task.queue_status == QUE_STATUS.FINISHED and res['stop_code'] == STOP_CODE.NO_STOP and res['data'] == '还没产生日志':
        res['stop_code'] = STOP_CODE.STOP
    return res


async def task_sys_log_api(task: TrainingTask = Depends(get_api_task())):
    res = await task.re_impl(TaskApiImpl).sys_log()
    # if not user.is_internal:
    #     res['data'] = ''  # 系统错误日志里含节点信息，先不给外部用户看
    return res


async def task_search_in_global(content, task: TrainingTask = Depends(get_api_task()), user=Depends(get_api_user_with_token())):
    content = urllib.parse.unquote(content)
    res = await task.re_impl(TaskApiImpl).search_in_global(content)
    return res


async def chain_perf_series_api(task: TrainingTask = Depends(get_api_task()),
                                user: User = Depends(get_api_user_with_token()), typ: str = 'gpu', rank: int = 0, data_interval: Optional[str]= '5min'):
    # data_interval 使用query参数传入
    if data_interval not in ('1min', '5min'):
        data_interval = '5min'
    try:
        data = await task.re_impl(DashboardApiImpl).get_chain_time_series(typ, rank, data_interval=data_interval)
        if not user.is_internal:
            for item in data:
                if 'node' in item:
                    item['node'] = convert_to_external_node(item['node'], 'rank', item['rank'])
        return {
                'success': 1,
                'data': data
        }
    except ValueError as e:
        return{
            'success':0,
            'msg': str(e)
        }


async def tag_task(tag: str, task: TrainingTask = Depends(get_api_task())):
    await task.re_impl(AioDbOperationImpl).tag_task(tag, remote_apply=True)
    return {
        'success': 1,
        'msg': f'训练任务[{task.job_info}] 设置 tag {tag} 标记成功'
    }


async def untag_task(tag: str, task: TrainingTask = Depends(get_api_task())):
    await task.re_impl(AioDbOperationImpl).untag_task(tag, remote_apply=True)
    return {
        'success': 1,
        'msg': f'训练任务[{task.job_info}] 取消 tag {tag} 标记成功'
    }


async def delete_tags(tag: List[str] = Query(default=None), user: User = Depends(get_api_user_with_token())):
    if tag is None:
        return {
            'success': 0,
            'msg': '请指定要删除的 tag'
        }
    await MarsDB().a_execute(f"""
    delete from "task_tag" where "user_name" = '{user.user_name}' and tag in ('{"','".join(tag)}')
    """)
    return {
        'success': 1,
        'msg': f'成功删除 tag {tag}'
    }


async def get_task_tags(user: User = Depends(get_api_user_with_token())):
    tags = [
        r.tag for r in
        await MarsDB().a_execute(f"""select distinct "tag" from "task_tag" where user_name = '{user.user_name}' """)
    ]
    return {
        'success': 1,
        'result': tags
    }


async def a_is_api_limited(key=None, waiting_seconds: int = 10) -> bool:
    """
    api限流，本次操作后waiting_seconds秒内再次操作会返回True（被限制），否则返回False（未被限制）
    :param key:
    :param waiting_seconds: 单位为秒
    :return:
    """
    try:
        key = f'{inspect.getframeinfo(inspect.currentframe().f_back)[2]}{key}'
        exist_key = await redis.exists(key)
        if exist_key:
            return True
        else:
            await redis.set(key, waiting_seconds)
            await redis.expire(key, waiting_seconds)
            return False
    except:
        return False


async def update_priority(
        priority: int = None,
        custom_rank: float = None,
        t: TrainingTask = Depends(get_api_task()),
        user: User = Depends(get_api_user_with_token()),
):
    if t.queue_status == QUE_STATUS.FINISHED:
        return {
            'success': 0,
            'msg': f'不能更新已经结束任务的优先级'
        }
    if priority is None and custom_rank is None:
        return {
            'success': 0,
            'msg': f'必须指定要更新的字段'
        }
    # 限流的时间
    waiting_seconds = 10
    if await a_is_api_limited(key=t.id, waiting_seconds=waiting_seconds):
        return {
            'success': 0,
            'msg': f'该任务在{waiting_seconds}秒内已经更新过优先级'
        }
    if priority:
        if not user.is_internal:
            priority = -1
        try:
            priority = int(priority)
            if priority not in [
                TASK_PRIORITY.EXTREME_HIGH.value, TASK_PRIORITY.VERY_HIGH.value, TASK_PRIORITY.HIGH.value,
                TASK_PRIORITY.ABOVE_NORMAL.value, TASK_PRIORITY.AUTO.value
            ]:
                raise Exception()
        except:
            return {'success': 0, 'msg': '优先级设置不对，请参考 hfai.client.EXP_PRIORITY'}
    if priority is not None:
        await t.re_impl(AioDbOperationImpl).update(('priority', ), (priority, ), remote_apply=False)
    runtime_config_json = {
        'update_priority_called': True
    }
    if custom_rank is not None:
        runtime_config_json['custom_rank'] = custom_rank
    await TaskRuntimeConfig(t).a_insert('runtime_priority', runtime_config_json, chain=True, update=True)
    return {
        'success': 1,
        'msg': f'成功修改 [{t.user_name}][{t.nb_name}] 的{" priority 为 " + str(priority) if priority is not None else ""}'
               f'{" custom_rank 为 " + str(custom_rank) if custom_rank is not None else ""}',
        'timestamp': time.time()
    }


async def switch_group(group: str = None, task: TrainingTask = Depends(get_api_task())):
    """
    修改任务的分组， 会在返回的 data 中提供 task 的 chain_id
    :param group:
    :param task:
    :return:
    """
    await MarsDB().a_execute("""
    update "task_ng" set "group" = %s where "chain_id" = %s
    """, (group, task.chain_id))
    return {
        'success': 1,
        'data': {
            'chain_id': task.chain_id
        }
    }
