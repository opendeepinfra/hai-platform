# 实验的接口
import datetime
import json
from typing import Optional
import uuid
import time
import munch
from fastapi import Depends, HTTPException
from collections import defaultdict
import conf
from api.app import app
from api.depends import get_api_user_name, request_limitation, RestTask, \
    get_task_with_check, get_task_no_check, get_api_user
from api.operation import create_task_base_queue, operate_task_base
from base_model.training_task import TrainingTask
from conf.flags import TASK_OP_CODE, chain_status_to_queue_status, TASK_TYPE, \
    TASK_PRIORITY, VALIDATION_TASK_FLAG, QUE_STATUS, RunJobCode, TASK_FLAG
from db import a_redis as redis
from db import a_db_engine, sql_params
from server_model.selector import AioTrainingTaskSelector
from server_model.training_task_impl import TaskApiImpl, DashboardApiImpl
from server_model.user import User
from server_model.task_impl import AioDbOperationImpl
from server_model.auto_task_impl import AutoTaskApiImpl
from utils import convert_to_external_task, convert_to_external_node, a_is_api_limited, Timer
import urllib
import aiohttp
import base64
import sqlalchemy
from logm import logger


async def get_tasks(page: int, page_size: int,
                    user: User = Depends(get_api_user),
                    filters: str = "[]", select_pods: bool = True,
                    only_training: bool = True,
                    only_star: bool = False
                    ):
    """
    filter 现在没用了，建议以后加
    @param filters:
        [
            {
                key: ''
                method: '',
                pattern: ...
            }
        ]
    @param page:
    @param page_size:
    @param user_name:
    @param select_pods:
    @param only_training:
    @param only_star:
    @return:
    """
    user_name = user.user_name
    filters = json.loads(filters)
    # note: 因为我们的内存足够大，所以目前来说 filter 以及 page 操作都可以把个人用户的数据筛选出来再做
    sql_where = f'"task_type" != %s and "user_name" = %s'
    sql_args = (TASK_TYPE.JUPYTER_TASK, user_name)
    if only_training:
        sql_where += f' and "nb_name" not like %s'
        sql_args += (f'%{VALIDATION_TASK_FLAG}',)
    if only_star:
        sql_where += f' and "suspend_code" & {TASK_FLAG.STAR} = {TASK_FLAG.STAR}'

    chain_tasks = await AioTrainingTaskSelector.where('no_cls_impl', sql_where, sql_args, limit=5000, order_desc=True)
    assert page >= 1, '设置的页码必须大于等于1'
    assert page_size <= 50, '一页中的数据不要超过 50'
    def get_my_type(p):
        if isinstance(p, list):
            try:
                return p[0]
            except:
                return ''
        else:
            return p

    for f in filters:
        if chain_tasks:
            filter_key = f['key']
            method = f['method']
            pattern = f['pattern']

            # note 为了加速，现在 chain_tasks 里面不是 实例了；所以有些 attr 没有，需要前端做二次处理
            if filter_key == 'chain_status':
                filter_key = 'queue_status'
                pattern = chain_status_to_queue_status(pattern)

            if not hasattr(chain_tasks[0], filter_key):
                continue
            if method == 'contains':
                chain_tasks = list(filter(lambda ct: pattern in type(get_my_type(pattern))(getattr(ct, filter_key)), chain_tasks))
            elif method == 'in':
                chain_tasks = list(filter(lambda ct: type(get_my_type(pattern))(getattr(ct, filter_key)) in pattern, chain_tasks))
            elif method == 'gte':
                chain_tasks = list(filter(lambda ct: type(get_my_type(pattern))(getattr(ct, filter_key)) >= pattern, chain_tasks))
            elif method == 'lte':
                chain_tasks = list(filter(lambda ct: type(get_my_type(pattern))(getattr(ct, filter_key)) <= pattern, chain_tasks))
            elif method == 'equal':
                chain_tasks = list(filter(lambda ct: type(get_my_type(pattern))(getattr(ct, filter_key)) == pattern, chain_tasks))
    # 在筛选出来的数据中，补全 pod 信息
    all_tasks = chain_tasks
    # 分页之后再select_pods
    chain_tasks = chain_tasks[(page - 1) * page_size: page * page_size]
    result = []
    for ct in chain_tasks:
        ct = TrainingTask(AutoTaskApiImpl, **ct)
        if select_pods:
            await ct.aio_select_pods()
        del ct._trait_values['config']
        if not user.is_internal:
            ct = convert_to_external_task(ct)
        result.append(ct._trait_values)
    return {
        'success': 1,
        'chain_tasks': result,
        'total': len(all_tasks)
    }


async def create_task(task: RestTask,
                      user: User = Depends(get_api_user)):
    res = await create_task_base_queue(task.nb_name, user,
                                       task.code_file, task.workspace,
                                       task.group, task.nodes,
                                       0, task.task_type,
                                       task.template,
                                       environments=task.environments,
                                       priority=task.priority,
                                       whole_life_state=task.whole_life_state,
                                       mount_code=task.mount_code,
                                       schedule_zone=task.schedule_zone,
                                       train_image=task.train_image,
                                       )
    if 'task' in res:
        if not user.is_internal:
            res['task'] = convert_to_external_task(res['task'])
        res['task'] = res['task']._trait_values
        del res['task']['config']
    return res


async def get_task(task: TrainingTask = Depends(get_task_with_check), user: User = Depends(get_api_user)):
    task.re_impl(AutoTaskApiImpl)
    await task.aio_select_pods()
    if not user.is_internal:
        task = convert_to_external_task(task)
    del task._trait_values['config']
    return {
        'success': 1,
        'task': task._trait_values
    }


async def rerun_task(task: TrainingTask = Depends(get_task_with_check),
                     user: User = Depends(get_api_user)):
    # recreate task
    task = munch.Munch.fromDict(task._trait_values)
    task.template = task.backend
    task.gpus = task.gpus.split(';')
    task.is_queue_job = 1
    res = await create_task(task, user)
    return res


async def stop_task(task: TrainingTask = Depends(get_task_with_check)):
    await redis.set(f'ban:{task.user_name}:{task.nb_name}:{task.chain_id}', 1)
    res = await operate_task_base(operate_user=task.user_name, task=task, task_op_code=TASK_OP_CODE.STOP)
    return res


async def suspend_task_by_name(
        task: TrainingTask = Depends(get_task_no_check),
        user_name=Depends(get_api_user_name),
        restart_delay: int = 0):
    res = await operate_task_base(operate_user=user_name, task=task, task_op_code=TASK_OP_CODE.SUSPEND, restart_delay=restart_delay)
    return res


async def task_node_log_api(task: TrainingTask = Depends(get_task_with_check), rank: int = 0, last_seen: str = 'null',
                            user=Depends(get_api_user), service: str = None):
    timer = Timer(module_name='server-get_task_log', fatal_threshold=10, warning_threshold=2)
    tag = f'{task.id}-{rank}-{last_seen}'
    timer.record()
    if service is not None and task.task_type != TASK_TYPE.JUPYTER_TASK:
        return {'success': 0, 'msg': '普通训练任务不支持获取服务日志.'}
    try:
        last_seen = json.loads(last_seen)
    except:
        last_seen = None
    timer.record(f'loads last_seen --- {tag}')
    if last_seen:
        try:
            last_seen['timestamp'] = datetime.datetime.strptime(last_seen['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
        except:
            last_seen['timestamp'] = datetime.datetime.strptime(last_seen['timestamp'], "%Y-%m-%dT%H:%M:%S")
    task.re_impl(AutoTaskApiImpl)
    timer.record(f're_implement --- {tag}')
    res = await task.log(rank, just_error=False, last_seen=last_seen, service=service)
    timer.record(f'获取日志 --- {tag}')
    # if not user.is_internal:
    #     for rank, node in enumerate(task.assigned_nodes):
    #         # 这里只转换了 task.assigned_nodes 中的节点
    #         res['data'] = res['data'].replace(node, convert_to_external_node(node, 'rank', rank))
    #     res['error_msg'] = ""
    return res


async def task_sys_log_api(task: TrainingTask = Depends(get_task_with_check), user=Depends(get_api_user)):
    res = await task.re_impl(TaskApiImpl).sys_log()
    # if not user.is_internal:
    #     res['data'] = ''  # 系统错误日志里含节点信息，先不给外部用户看
    return res


async def task_search_in_global(content, task: TrainingTask = Depends(get_task_with_check), user=Depends(get_api_user)):
    content = urllib.parse.unquote(content)
    res = await task.re_impl(TaskApiImpl).search_in_global(content)
    return res


async def task_perf_api(task: TrainingTask = Depends(get_task_with_check)):
    return {
        'success': 1,
        'data': await task.re_impl(DashboardApiImpl).get_latest_point()
    }


async def chain_perf_series_api(task: TrainingTask = Depends(get_task_with_check), 
                                user: User = Depends(get_api_user), typ: str = 'gpu', rank: int = 0, data_interval: Optional[str]= '5min'):
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


async def validate_task(file: str,
                        task: TrainingTask = Depends(get_task_no_check),
                        user: User = Depends(get_api_user), chosen_ranks='all'
                        ):
    if task.task_type != TASK_TYPE.TRAINING_TASK:
        return {
            'success': 1,
            'created': 0,
            'msg': '错误！只能validate训练任务'
        }
    if not user.is_internal:
        return {
            'success': 1,
            'created': 0,
            'msg': '错误！没有validate节点权限'
        }
    if chosen_ranks == 'all':
        chosen_nodes = task.assigned_nodes
    else:
        rank_list = chosen_ranks.split(',')
        if any([not rank.isnumeric() for rank in rank_list]) or any([not(0 <= int(rank) < len(task.assigned_nodes)) for rank in rank_list]):
            return {
                'success': 1,
                'created': 0,
                'msg': '错误！ 传入的rank必须是个整数，并且在[0, task节点个数-1]内'
            }
        rank_list.sort(key=lambda x: int(x))
        chosen_nodes = [task.assigned_nodes[int(rank)] for rank in rank_list]
    # 在提交 validation 任务之前，获取各个节点的 gpu 使用率情况，传递给 zhw validation 处理
    gpu_utils = None
    # 运行的任务才做这个
    if task.queue_status == QUE_STATUS.SCHEDULED:
        query = ' or '.join(
            [f'DCGM_FI_DEV_GPU_UTIL{{kubernetes_node="{node}"}}' for node in
             chosen_nodes])
        # 10s 拿不到就算了
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(10)) as session:
            async with session.get(
                    f'{conf.POLYAXON_SETTING.prometheus_server}/api/v1/query',
                    params=f'query={query}') as r:
                if r.status == 200:
                    result_ = await r.text()
                    result = json.loads(result_)
                    gpu_utils = defaultdict(lambda: defaultdict(float))
                    for res in result['data']['result']:
                        gpu_utils[res['metric']['kubernetes_node']][int(res['metric']['gpu'])] = float(res['value'][1])
    res = await create_task_base_queue(  # 创建虚拟任务
        nb_name=f'validation_{task.nb_name}',
        user=user,
        code_file=file,
        workspace='',
        group='',
        nodes=task.nodes,
        restart_count=0,
        task_type=TASK_TYPE.VIRTUAL_TASK,
        template='cuda_11',
        priority=TASK_PRIORITY.AUTO.value,
        chain_id=f'validation_{task.chain_id}_{str(uuid.uuid4())[0:8]}',
        whole_life_state=0,
        mount_code=2,
        assigned_nodes=chosen_nodes,
        environments=(
            {'VALIDATION_TASK_GPU_UTILS_b64':
                 base64.b64encode(json.dumps(gpu_utils).encode()).decode()}
            if gpu_utils is not None else {})
    )
    if 'task' in res:
        res['task'] = res['task']._trait_values
        del res['task']['config']
    res['msg'] = 'validation任务' + res['msg']
    res['created'] = int(res['success'] == RunJobCode.QUEUED.value)  # 只有queue的任务才认为创建成功了
    res['success'] = 1
    return res


async def validate_nodes(nodes: str,
                         file: str,
                        user: User = Depends(get_api_user)
                        ):
    nodes = nodes.replace(',', ';')
    assigned_nodes = nodes.split(';')
    assigned_nodes = [node for node in assigned_nodes if node != '']
    if len(assigned_nodes) == 0:
        return {
            'success': 1,
            'created': 0,
            'msg': '错误！请指定validate节点'
        }
    for node in assigned_nodes:
        if 'dev' in node:
            return {
                'success': 1,
                'created': 0,
                'msg': '错误！只能validate计算节点'
            }
    if not user.is_internal:
        return {
            'success': 1,
            'created': 0,
            'msg': '错误！没有validate节点权限'
        }
    res = await create_task_base_queue(  # 创建虚拟任务
        nb_name=f'validation_task_{str(uuid.uuid4())[0:8]}',
        user=user,
        code_file=file,
        workspace='',
        group='',
        nodes=len(assigned_nodes),
        restart_count=0,
        task_type=TASK_TYPE.VIRTUAL_TASK,
        template='cuda_11',
        priority=TASK_PRIORITY.AUTO.value,
        chain_id=f'validation_{str(uuid.uuid4())[0:8]}',
        whole_life_state=0,
        mount_code=2,
        assigned_nodes=assigned_nodes
    )
    if 'task' in res:
        if not user.is_internal:
            res['task'] = convert_to_external_task(res['task'])
        res['task'] = res['task']._trait_values
        del res['task']['config']
    res['msg'] = 'validation任务' + res['msg']
    res['success'] = 1
    res['created'] = 1
    return res


async def running_tasks():
    chain_tasks = await AioTrainingTaskSelector.where('no_cls_impl', 'queue_status != %s', (QUE_STATUS.FINISHED, ), limit=10000, order_desc=True)
    result = []
    for ct in chain_tasks:
        result.append({
            'id': ct['id'],
            'user_name': ct['user_name'],
            'nb_name': ct['nb_name'],
            'task_type': ct['task_type'],
            'queue_status': ct['queue_status']
        })
    return {
        'success': 1,
        'tasks': result
    }


async def star_task(task: TrainingTask = Depends(get_task_with_check)):
    await task.re_impl(AioDbOperationImpl).update(('suspend_code',), (task.suspend_code | TASK_FLAG.STAR,))
    return {
        'success': 1,
        'msg': f'训练任务[{task.job_info}] 设置 STAR 标记成功'
    }


async def unstar_task(task: TrainingTask = Depends(get_task_with_check)):
    await task.re_impl(AioDbOperationImpl).update(('suspend_code',), (task.suspend_code & ~TASK_FLAG.STAR,))
    return {
        'success': 1,
        'msg': f'训练任务[{task.job_info}] 取消 STAR 标记成功'
    }


async def switch_schedule_zone(schedule_zone: str = None, task: TrainingTask = Depends(get_task_with_check)):
    await task.re_impl(AioDbOperationImpl).update(('config_json',), ({'schedule_zone': schedule_zone},))
    return {
        'success': 1,
        'msg': f'训练任务[{task.job_info}] schedule_zone 切换到 {schedule_zone} 成功' if schedule_zone is not None else f'训练任务[{task.job_info}] 取消设置 schedule_zone 成功'
    }


async def update_priority(
        priority: int = None, custom_rank: float = None, t: TrainingTask = Depends(get_task_with_check),
        user: User = Depends(get_api_user)
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
    fields = ['config_json']
    values = [{}]
    if priority is not None:
        fields.append('priority')
        values.append(priority)
        values[0]['update_priority_called'] = True
    if custom_rank is not None:
        values[0]['custom_rank'] = custom_rank
    await t.re_impl(AioDbOperationImpl).update(fields, values)
    # 记录操作
    if priority is not None:
        _sql = f'''
        insert into "priority_change_log" ("task_id", "dist_priority")
        values (%s, %s)
        '''
        sql, params = sql_params.format(_sql, (t.id, priority))
        try:
            async with a_db_engine.begin() as conn:
                res = await conn.execute(sqlalchemy.text(sql), params)
        except Exception as exp:
            logging.error(exp)
            # log 不影响接口的正常使用

    return {
        'success': 1,
        'msg': f'成功修改 [{t.user_name}][{t.nb_name}] 的{" priority 为 " + str(priority) if priority is not None else ""}'
               f'{" custom_rank 为 " + str(custom_rank) if custom_rank is not None else ""}',
        'timestamp': time.time()
    }


async def disable_warn(warn_type: int, t: TrainingTask = Depends(get_task_with_check)):
    if t.queue_status == QUE_STATUS.FINISHED:
        return {
            'success': 0,
            'msg': f'不能设置已经结束任务的静默报警'
        }
    await redis.set(f'disable_warn:{t.id}', f'{warn_type}')
    return {
        'success': 1,
        'msg': f'设置warning静默成功'
    }
