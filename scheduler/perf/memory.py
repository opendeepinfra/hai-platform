

"""
检查内存泄漏的问题
"""

import os
import gc
import time
import psutil
from conf.flags import QUE_STATUS
from .gen_tick_data import gen_tick_data_from_config
from scheduler.base_model.base_types import SCHEDULER_RESULT, PROCESS_RESULT
from scheduler.modules.assigners.training.pre_assign import pre_assign, set_user_rate
from scheduler.modules.assigners.training.grant_permission import within_limits, running, waiting_init
from scheduler.modules.matchers.match_training_task import get_match_task_func as get_match_training_task_func


TEST_COUNT = 10


def check_memory_leak():
    tick_data = gen_tick_data_from_config(100, 100, 1000)
    resource_df, user_df, task_df = tick_data.resource_df.copy(), tick_data.user_df.copy(), tick_data.task_df.copy()
    resource_df, user_df, task_df = pre_assign(resource_df, user_df, task_df)
    set_user_rate({})
    match_func = get_match_training_task_func()
    gc.collect()
    mem1 = psutil.Process(os.getpid()).memory_info().rss
    for i in range(TEST_COUNT):
        resource_df, user_df, task_df = tick_data.resource_df.copy(), tick_data.user_df.copy(), tick_data.task_df.copy()
        resource_df, user_df, task_df = pre_assign(resource_df, user_df, task_df)
        within_limits.apply_rule(len(resource_df), user_df=user_df, task_df=task_df)
        resource_df, user_df, task_df = tick_data.resource_df.copy(), tick_data.user_df.copy(), tick_data.task_df.copy()
        task_df['queue_status'] = QUE_STATUS.SCHEDULED
        resource_df, user_df, task_df = pre_assign(resource_df, user_df, task_df)
        running.apply_rule(len(resource_df), user_df=user_df, task_df=task_df)
        resource_df, user_df, task_df = tick_data.resource_df.copy(), tick_data.user_df.copy(), tick_data.task_df.copy()
        task_df['queue_status'] = QUE_STATUS.QUEUED
        resource_df, user_df, task_df = pre_assign(resource_df, user_df, task_df)
        waiting_init.apply_rule(len(resource_df), user_df=user_df, task_df=task_df)
        task_df['scheduler_result'] = SCHEDULER_RESULT.CAN_RUN
        task_df['process_result'] = PROCESS_RESULT.NOT_SURE
        resource_df['nodes'] = 1
        # 设置成 False，因为 match func 会记录上次打断的任务
        match_func(resource_df=resource_df, task_df=task_df, valid=False)
    set_user_rate({})
    gc.collect()
    mem2 = psutil.Process(os.getpid()).memory_info().rss
    # 超过 1kb 认为有内存泄漏
    if mem2 - mem1 > 1024:
        print('检查到了内存泄漏，内存增加了', round((mem2 - mem1) / 1024, 2), 'kb')
