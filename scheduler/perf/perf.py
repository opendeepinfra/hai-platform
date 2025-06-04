

import time
import pickle
from dataclasses import dataclass, field
from conf.flags import QUE_STATUS
from ci.test_case.scheduler_cases import get_new_schedule
from scheduler.base_model.base_types import SCHEDULER_RESULT, PROCESS_RESULT, TickData
from scheduler.base_model.connection import ProcessConnection
from scheduler.modules.assigners.training.pre_assign import pre_assign
from scheduler.modules.assigners.training.grant_permission import within_limits, running, waiting_init
from scheduler.modules.matchers.match_training_task import get_match_task_func as get_match_training_task_func


TEST_COUNT = 3


@dataclass
class PerfTickData(object):
    tick_data: TickData
    process_connection_time: int = 0
    # 单独测试均为全量 tick_data 参与
    pre_assign_time: int = 0
    within_limits_time: int = 0
    running_time: int = 0
    waiting_init_time: int = 0
    match_time: int = 0
    within_limits_result: list = field(default_factory=list)
    running_result: list = field(default_factory=list)
    waiting_init_result: list = field(default_factory=list)
    match_startup_result: set = field(default_factory=set)
    match_suspend_result: set = field(default_factory=set)
    # 整体调度结果
    schedule_startup_result: set = field(default_factory=set)
    schedule_suspend_result: set = field(default_factory=set)
    schedule_keep_running_result: set = field(default_factory=set)


def perf_func(func):
    t1 = time.time()
    res = func()
    t2 = time.time()
    return res, (t2 - t1) * 1000


def perf_tick_data(tick_data: TickData) -> PerfTickData:
    tick_data.task_df['schedule_zone'] = None
    result = PerfTickData(tick_data=tick_data)
    # perf process connection
    conn = ProcessConnection()
    total_time = 0
    for i in range(TEST_COUNT):
        _, t1 = perf_func(lambda: conn.put(tick_data))
        _, t2 = perf_func(lambda: conn.get())
        total_time += t1 + t2
    result.process_connection_time = total_time / TEST_COUNT
    # perf pre assign
    total_time = 0
    for i in range(TEST_COUNT):
        resource_df, user_df, task_df = tick_data.resource_df.copy(), tick_data.user_df.copy(), tick_data.task_df.copy()
        resource_df['group'] = 'group'
        user_df['group'] = 'group'
        task_df['group'] = 'group'
        _, t = perf_func(lambda: pre_assign(resource_df, user_df, task_df))
        total_time += t
    result.pre_assign_time = total_time / TEST_COUNT
    resource_df, user_df, task_df = tick_data.resource_df.copy(), tick_data.user_df.copy(), tick_data.task_df.copy()
    resource_df['group'] = 'group'
    user_df['group'] = 'group'
    task_df['group'] = 'group'
    resource_df, user_df, task_df = pre_assign(resource_df, user_df, task_df)
    # perf within_limits
    total_time = 0
    for i in range(TEST_COUNT):
        within_limits_result, t = perf_func(lambda: within_limits.apply_rule(len(resource_df), task_df=task_df, user_df=user_df))
        result.within_limits_result = within_limits_result
        total_time += t
    result.within_limits_time = total_time / TEST_COUNT
    # perf running
    total_time = 0
    resource_df, user_df, task_df = tick_data.resource_df.copy(), tick_data.user_df.copy(), tick_data.task_df.copy()
    resource_df['group'] = 'group'
    user_df['group'] = 'group'
    task_df['group'] = 'group'
    task_df['queue_status'] = QUE_STATUS.SCHEDULED
    resource_df, user_df, task_df = pre_assign(resource_df, user_df, task_df)
    for i in range(TEST_COUNT):
        running_result, t = perf_func(lambda: running.apply_rule(len(resource_df), task_df=task_df, user_df=user_df))
        result.running_result = running_result
        total_time += t
    result.running_time = total_time / TEST_COUNT
    # perf waiting_init
    resource_df, user_df, task_df = tick_data.resource_df.copy(), tick_data.user_df.copy(), tick_data.task_df.copy()
    resource_df['group'] = 'group'
    user_df['group'] = 'group'
    task_df['group'] = 'group'
    task_df['queue_status'] = QUE_STATUS.QUEUED
    resource_df, user_df, task_df = pre_assign(resource_df, user_df, task_df)
    total_time = 0
    for i in range(TEST_COUNT):
        waiting_init_result, t = perf_func(lambda: waiting_init.apply_rule(len(resource_df), task_df=task_df, user_df=user_df))
        result.running_result = waiting_init_result
        total_time += t
    result.waiting_init_time = total_time / TEST_COUNT
    # perf match
    total_time = 0
    match_func = get_match_training_task_func()
    # match_func = get_match_training_task_func_new()
    resource_df, user_df, task_df = tick_data.resource_df.copy(), tick_data.user_df.copy(), tick_data.task_df.copy()
    resource_df['group'] = 'group'
    user_df['group'] = 'group'
    task_df['group'] = 'group'
    resource_df, user_df, task_df = pre_assign(resource_df, user_df, task_df)
    for i in range(TEST_COUNT):
        task_df['scheduler_result'] = SCHEDULER_RESULT.CAN_RUN
        task_df['process_result'] = PROCESS_RESULT.NOT_SURE
        resource_df['nodes'] = 1
        match_result, t = perf_func(lambda: match_func(resource_df=resource_df, task_df=task_df, valid=True))
        total_time += t
        match_result_task_df = match_result[1]
        result.match_startup_result = set(match_result_task_df[match_result_task_df.process_result == PROCESS_RESULT.STARTUP].index.to_list())
        result.match_suspend_result = set(match_result_task_df[match_result_task_df.process_result == PROCESS_RESULT.SUSPEND].index.to_list())
    result.match_time = total_time / TEST_COUNT
    new_schedule = get_new_schedule()
    new_schedule.dfs_writer.put(tick_data)
    new_schedule.beater.tick_process()
    new_schedule.beater.tick_process()
    new_schedule.training_assigner.tick_process()
    new_schedule.training_matcher.tick_process()
    res_df = new_schedule.training_matcher.task_df
    result.schedule_startup_result = set(res_df[res_df.process_result == PROCESS_RESULT.STARTUP].id)
    result.schedule_suspend_result = set(res_df[res_df.process_result == PROCESS_RESULT.SUSPEND].id)
    result.schedule_keep_running_result = set(res_df[res_df.process_result == PROCESS_RESULT.KEEP_RUNNING].id)
    return result


def start_perf():
    with open('/weka-jd/prod/platform_team/tick_data/', 'rb') as f:
        pts = pickle.loads(f.read())
