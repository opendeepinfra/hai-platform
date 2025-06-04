# note: 这个已经没用了
import json
import time
from collections import defaultdict

import aiofiles
from fastapi import Depends, HTTPException

from api.app import app
from api.depends import request_limitation, ChainIds
from conf.flags import QUE_STATUS
from db import redis_conn
from server_model.selector import AioTrainingTaskSelector, AioUserSelector
from server_model.training_task_impl import TaskApiImpl

color_map = {
    '挂起': 'good',
    '运行中': 'bad',
    '加入队列': 'terrible',
    '启动节点': 'rail_response',
    'running': 'rail_idle',
    'stop_terminating': 'rail_load',
    'failed_terminating': 'rail_load',
    'succeeded_terminating': 'rail_load'
}


def tracing_parse(tracing, pid=0):
    tracing['ph'] = 'X'
    tracing['pid'] = pid
    tracing['dur'] *= 1000000
    tracing['ts'] *= 1000000
    if tracing['name'] in color_map:
        tracing['cname'] = color_map[tracing['name']]
    return tracing


async def get_lifecycle_from_chain_id(chain_id_list: list):
    tracing_list = []
    pod_lifecycle = defaultdict(list)
    for num, chain_id in enumerate(chain_id_list):
        info = redis_conn.get(f'chain_id_lifecycle:{chain_id}')
        if not info:
            continue
        lifecycle_list = info.decode().strip('\n').split('\n')
        lifecycle_main_list = [lifecycle for lifecycle in lifecycle_list if json.loads(lifecycle)['tid'] == 'main']
        task = await AioTrainingTaskSelector.find_one(TaskApiImpl, chain_id=chain_id)
        tracing_list.append({"name": "process_name", "ph": "M", "pid": num, "args": {"name": chain_id}})
        for i in range(len(lifecycle_main_list) - 1):
            tracing = json.loads(lifecycle_main_list[i])
            tracing['dur'] = json.loads(lifecycle_main_list[i+1])['ts'] - tracing['ts']
            tracing_list.append(tracing_parse(tracing, pid=num))
        tracing = json.loads(lifecycle_main_list[-1])
        if task.queue_status != QUE_STATUS.FINISHED:
            tracing['dur'] = time.time() - tracing['ts']
            tracing_list.append(tracing_parse(tracing, pid=num))
        task = await task.aio_re_pods()
        for pod in task.pods:
            lifecycle_pod_list = [lifecycle for lifecycle in lifecycle_list
                                  if json.loads(lifecycle)['tid'] == f"Rank {pod.pod_id.split('-')[-1]}"]
            start_time = None
            for i in range(len(lifecycle_pod_list) - 1):
                tracing = json.loads(lifecycle_pod_list[i])
                if tracing['name'] == '结束':
                    pod_lifecycle[tracing['args']['node']].append(
                        {
                            'start': start_time,
                            'end': tracing['ts'],
                            'pid': num,
                            'tid': tracing['tid']
                        }
                    )
                    start_time = None
                    continue
                if not start_time:
                    start_time = tracing['ts']
                tracing['dur'] = json.loads(lifecycle_pod_list[i+1])['ts'] - tracing['ts']
                tracing_list.append(tracing_parse(tracing, pid=num))
            tracing = json.loads(lifecycle_pod_list[-1])
            if tracing['name'] != '结束':
                tracing['dur'] = time.time() - tracing['ts']
                tracing_list.append(tracing_parse(tracing, pid=num))
                end_time = time.time()
            else:
                end_time = tracing['ts']
            pod_lifecycle[tracing['args']['node']].append(
                {
                    'start': start_time,
                    'end': end_time,
                    'pid': num,
                    'tid': tracing['tid']
                }
            )
        for k, v in pod_lifecycle.items():
            v.sort(key=lambda x: x['start'])
            for i in range(len(v) - 1):
                if v[i + 1]['pid'] != v[i]['pid']:
                    tracing = {
                        'name': 'connect',
                        'ph': 's',
                        'id': f'{k}_{i}',
                        'pid': v[i]['pid'],
                        'tid': v[i]['tid'],
                        'ts': v[i]['end'] * 1000000
                    }
                    tracing_list.append(tracing)
                    tracing = {
                        'name': 'connect',
                        'ph': 'f',
                        'bp': 'e',
                        'id': f'{k}_{i}',
                        'pid': v[i + 1]['pid'],
                        'tid': v[i + 1]['tid'],
                        'ts': v[i + 1]['start'] * 1000000
                    }
                    tracing_list.append(tracing)
    return json.dumps(tracing_list, ensure_ascii=False)


@app.post('/get_lifecycle_from_chain_id_api/{token}', dependencies=[Depends(request_limitation)])
async def get_lifecycle_from_chain_id_api(token, req: ChainIds):
    user = await AioUserSelector.find_one(token=token)
    if not user.user_name == 'hf_system':
        raise HTTPException(status_code=401, detail={
            'success': 0,
            'msg': '只有hf_system才有权限获取chain_id的timeline'
        })

    chain_id_list = req.chain_id_list
    saved_path = req.saved_path
    rst = await get_lifecycle_from_chain_id(chain_id_list.split(','))
    if saved_path:
        async with aiofiles.open(saved_path, "w") as fp:
            await fp.write(f'{rst}')
    return {
        'success': 1,
        'msg': rst
    }
