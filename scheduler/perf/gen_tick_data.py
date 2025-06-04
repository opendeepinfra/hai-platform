

"""
按照要求生成 tick_data，并且加上线上的测试用例
"""



import os
import time
import zlib
import psutil
import pickle
import random
import pandas as pd
from conf.flags import QUE_STATUS, SCHEDULE_THRESHOLD_SECOND
from scheduler.base_model.base_types import SCHEDULER_RESULT, PROCESS_RESULT, TickData


GEN_CONFIG = [
    {
        'USER_COUNT': 10,
        'USER_TASK_COUNT': 200,
        'NODE_COUNT': 1200,
        'TEST_COUNT': 3
    },
    # {
    #     'USER_COUNT': 1000,
    #     'USER_TASK_COUNT': 100,
    #     'NODE_COUNT': 3000,
    #     'TEST_COUNT': 3
    # },
    # {
    #     'USER_COUNT': 100,
    #     'USER_TASK_COUNT': 1000,
    #     'NODE_COUNT': 3000,
    #     'TEST_COUNT': 3
    # }
]


def gen_tick_data_from_config(user_count, user_task_count, node_count):
    resource_df = pd.DataFrame([
        [128, 1, f'node{i}', 8, 'Ready', 'group', 128, 'group', 'not_working', 'leaf', 'spine', True, 'room1' if random.random() > 0.5 else 'room2']
        for i in range(node_count)
    ], columns=[
        'cpu', 'nodes', 'NAME', 'GPU_NUM', 'STATUS', 'mars_group', 'memory', 'group', 'working', 'LEAF',
        'SPINE', 'active', 'room'
    ])
    def gen_nodes(k):
        return [k, random.sample(resource_df.NAME.to_list(), k)]
    user_df = pd.DataFrame([
        [f'user{i}', 'node', 'group',
         random.randint(1, 1000) if random.random() > 0.1 else random.randint(10000, 100000), 'internal', 50]
        for i in range(user_count)
    ], columns=[
        'user_name', 'resource', 'group', 'quota', 'role', 'priority'
    ])
    task_df = pd.DataFrame([
        [
            i * user_task_count + j, f'{i}-{j}', f'user{i}', '', 'group',
            'training', QUE_STATUS.QUEUED if random.random() > 0.9 else QUE_STATUS.SCHEDULED, 50, i * user_task_count + j,
            random.randint(0, SCHEDULE_THRESHOLD_SECOND) if random.random() > 0.5 else random.randint(
                SCHEDULE_THRESHOLD_SECOND + 1, SCHEDULE_THRESHOLD_SECOND + 1000),
            f'{i}-{j}', {}, 'internal', SCHEDULER_RESULT.NOT_SURE, PROCESS_RESULT.NOT_SURE, '', 0, i * user_task_count + j, 'queued',
            None, None, None
        ] + gen_nodes(random.randint(1, 10) if random.random() > 0.1 else random.randint(30, 60))
        for i in range(user_count) for j in range(user_task_count)
    ], columns=[
        'id', 'nb_name', 'user_name', 'code_file', 'group', 'task_type',
        'queue_status', 'priority', 'first_id', 'running_seconds', 'chain_id', 'config_json', 'user_role',
        'scheduler_result', 'process_result', 'scheduler_msg', 'created_seconds', 'custom_rank', 'worker_status',
        'memory', 'cpu', 'assigned_gpus', 'nodes', 'assigned_nodes'
    ])
    tick_data = TickData(
        seq=1,
        valid=True,
        task_df=task_df,
        user_df=user_df,
        resource_df=resource_df
    )
    tick_data.task_df.index = tick_data.task_df.id
    # 选一部分任务变成 scheduled
    return tick_data


def gen_tick_data():
    test_tick_data = []
    for config in GEN_CONFIG:
        for i in range(config['TEST_COUNT']):
            test_tick_data.append(gen_tick_data_from_config(config['USER_COUNT'], config['USER_TASK_COUNT'], config['NODE_COUNT']))
    for file_name in os.listdir('/weka-jd/prod/platform_team/tick_data'):
        with open(f'/weka-jd/prod/platform_team/tick_data/{file_name}', 'rb') as f:
            test_tick_data.append(pickle.loads(zlib.decompress(f.read())))
    return test_tick_data
