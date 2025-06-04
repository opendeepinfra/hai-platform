import subprocess
from subprocess import STDOUT, check_output

import hashlib
import os
import asyncio

import conf
import getpass
from datetime import datetime
import time
# 容器化启动 server 的预处理阶段，会用到这里的代码，但是数据库还没有启动
try:
    from db import redis_conn, a_redis
except:
    pass
import json
import inspect
from logm import logger


try:
    from fetion3 import Fetion
    fetion = Fetion('192.168.110.104', 6000)
except:
    fetion = None

is_debug = conf.environ.get('DEBUG', '0') == '1'


# note 发现多进程调用这个，会产生一堆的 Z+ 子进程，没时间去研究这个(有可能是超时后没有取消任务。下面已尝试修复)
async def run_cmd_aio(cmd, timeout=3):
    read_task = None
    try:
        create = asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE,
                                                 stderr=asyncio.subprocess.PIPE)
        proc = await create
        read_task = asyncio.Task(proc.communicate())
        stdout, stderr = await asyncio.wait_for(read_task, timeout)  # timeout=3s
        if proc.returncode != 0:
            logger.error(' '.join([cmd, '->', 'Run Failed']))
            raise subprocess.CalledProcessError(proc.returncode, cmd, stdout,
                                                stderr)
    except asyncio.TimeoutError:
        logger.error(' '.join([cmd, '->', 'Run Timeout']))
        if read_task is not None:
            read_task.cancel()
        raise
    return stdout, stderr


def run_cmd_new(cmd, timeout=3):
    start_time = time.time()
    try:
        output = check_output(cmd, stderr=STDOUT, timeout=timeout, shell=True)
    except subprocess.CalledProcessError as e:
        time_elapsed = time.time() - start_time
        log_str = f'time_elapsed: {time_elapsed}, {cmd} -> Run Failed with exit code {e.returncode}, output: {e.output.decode()}'
        logger.error(log_str)
        raise Exception(log_str)
    except subprocess.TimeoutExpired:
        logger.error(' '.join([cmd, '->', 'Run Timeout']))
        raise
    # print(cmd, '->', 'Run Success')
    return output


def run_cmd(cmd):
    """
        执行命令，按行拆成list然后返回结果
    @param cmd:
    @return:
    """
    # print('running cmd: ', cmd)
    f = os.popen(cmd)
    temp = f.readlines()
    result = list(filter(None, map(lambda x: x.strip(), temp)))
    return result


def fetion_alert(msg, qq=None, stdout=True):
    if stdout:
        print(msg)
    if qq is None:
        qq = '前端' if is_debug else 'alpha集群报警'
    if conf.environ.get('FETION') == '1':
        try:
            fetion.f_alert(3, conf.POLYAXON_SETTING.get('fetion', f'{getpass.getuser()}TestWatchDog'), msg, qq)
        except:
            print('fetion 凉了')


def convert_task_job_to_zwt_key(task, rank: int):
    return f'{hashlib.sha256(f"{task.user_name}{task.nb_name}".encode("utf-8")).hexdigest()[0:50]}-{rank}'


def convert_to_external_node(node, prefix, rank):
    return f'hfai-{prefix}-{rank}'


def convert_to_external_task(task):
    task.assigned_nodes = [convert_to_external_node(n, 'rank', rank) for rank, n in enumerate(task.assigned_nodes)]
    for rank, pod in enumerate(task._pods_):
        pod.node = convert_to_external_node(pod.node, 'rank', rank)
    return task


def monitor_brpop(key, value, process_start_time=None, module_name='Unknown', time_delta_threshold=60):
    try:
        if isinstance(value, str):
            value = json.loads(value)
        if process_start_time is not None:
            value['hf_timestamp'] = max(value['hf_timestamp'], process_start_time)
        time_delta = int(time.time() - value['hf_timestamp'])
        if time_delta >= time_delta_threshold:
            fetion_alert(f'{module_name}订阅的key：{key}在brpop时经过{time_delta}秒才得到结果，超过阈值{time_delta_threshold}秒，请管理员检查！', qq=conf.environ.get('IMPORTANT_QQ', '前端'))
        print(f'{module_name}订阅的key: {key}得到结果{value}，耗时{time_delta}秒，阈值{time_delta_threshold}秒')
    except:
        pass


class Timer:
    def __init__(self, module_name='Unknown', fatal_threshold=10, warning_threshold=10, fetion=False):  # 默认超过10秒报警
        self.record_time = None
        self.fatal_threshold = fatal_threshold
        self.warning_threshold = warning_threshold
        self.module_name = module_name
        self.fetion = fetion

    def record(self, note=None):
        if self.record_time is None:
            self.refresh()
            return
        time_delta = float((datetime.now() - self.record_time).total_seconds())
        log = None

        if time_delta > self.fatal_threshold:  # 两个时间的间隔超过了阈值
            log = f'[FATAL!] {self.module_name}本次耗时{time_delta}秒，请管理员检查！note: {note}'
            logger.error(log)
        elif time_delta > self.warning_threshold:
            log = f'[WARNING!] {self.module_name}本次耗时{time_delta}秒，请管理员检查！note: {note}'
            logger.warning(log)
        if log is not None:
            if self.fetion:
                fetion_alert(log, qq=conf.environ.get('IMPORTANT_QQ', '前端'), stdout=False)
        self.record_time = datetime.now()

    def refresh(self):
        self.record_time = datetime.now()


async def a_is_api_limited(key=None, waiting_seconds: int = 10) -> bool:
    """
    api限流，本次操作后waiting_seconds秒内再次操作会返回True（被限制），否则返回False（未被限制）
    :param key:
    :param waiting_seconds: 单位为秒
    :return:
    """
    key = f'{inspect.getframeinfo(inspect.currentframe().f_back)[2]}{key}'
    exist_key = await a_redis.exists(key)
    if exist_key:
        return True
    else:
        await a_redis.set(key, waiting_seconds)
        await a_redis.expire(key, waiting_seconds)
        return False
