
from fastapi import Depends

from api.app import app
from api.depends import get_task_with_check
from api.duplicated_depends import QUEUE_CONTROL_LOG
from base_model.training_task import TrainingTask
from conf.flags import QUE_STATUS
from db import a_redis as redis
from server_model.selector import AioBaseTaskSelector


async def get_queued():
    queued = await AioBaseTaskSelector.find_list(None, queue_status=QUE_STATUS.QUEUED, order_desc=True)
    return {
        'success': 1,
        'data': queued
    }


@app.get('/get_running_api')
async def get_running():
    running = await AioBaseTaskSelector.find_list(None, queue_status=QUE_STATUS.SCHEDULED, order_desc=True)
    for t in running:
        t.exp_est_time = await redis.get(f'exp_est_time:{t.user_name}:{t.id}')
        t.created_at = t.begin_at
        t.updated_at = 0
    return {
        'success': 1,
        'data': running
    }


@app.get('/get_queue_control_log_api')
async def get_queue_control_log():
    return {
        'success': 1,
        'data': QUEUE_CONTROL_LOG[::-1]
    }


@app.post('/cancel_queue_api/{token}/{id}')
async def cancel_queue(t: TrainingTask = Depends(get_task_with_check)):
    return {'success': 0, 'msg': '这个接口没用了'}
