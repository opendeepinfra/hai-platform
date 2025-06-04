import logging
import os
from datetime import datetime

import logzero

import conf
from db import redis_conn
from server_model.selector import BaseTaskSelector
from server_model.auto_task_impl import AutoTaskSchemaImpl
from utils import fetion_alert

k8s_namespace = conf.POLYAXON_SETTING.k8s.namespace
task_id = int(os.environ['TASK_ID'])
task = BaseTaskSelector.find_one(AutoTaskSchemaImpl, id=task_id)
user_name = task.user_name

logger = None
module = None
n_module = 0  # 第几个重启的module
QQ = os.environ['QQ']
QQ1_USERS = {'lwf'}


def save_metric(event):
    """
    manager 会在关键的 event 打点，具体操作就是touch一个文件
    目录结构为
    hw_tests.log
       |- YYYYMMDD
            |- event
                |- task_id
    @param event:
    @return:
    """
    metric_path = os.path.join(conf.POLYAXON_SETTING.hw_tests.log, 'manager',
                               datetime.today().strftime('%Y'),
                               datetime.today().strftime('%Y%m%d'),
                               event)
    os.makedirs(metric_path, exist_ok=True)
    # 每次调用了都要加, 因为 manger 会烂掉，重启
    name = f'{task_id}_{datetime.now().strftime("%H_%M_%S")}'
    if not os.path.exists(os.path.join(metric_path, name)):
        os.mknod(os.path.join(metric_path, name))


def setup_logger(md):
    global logger
    global module
    global n_module
    module = md
    n_module = redis_conn.get(f'module:{task_id}:{module}')
    n_module = int(n_module) + 1 if n_module else 1
    redis_conn.set(f'module:{task_id}:{module}', n_module)
    logger = logzero.setup_logger(
        "log",
        logfile=f"/var/log/experiment_manager_log/{k8s_namespace}_{task_id}/{md.split('.')[0]}_{n_module}",
        maxBytes=1024 * 128,
        backupCount=5,
        level=logging.DEBUG
    )
    debug(f'{module} called')


def record_in_redis_and_fetion(info, pod_id, fetion):
    module_prefix = ('%-25s' % f"{module.split('.')[0]}_{n_module}") if pod_id == 'manager' else ''
    key = f"lifecycle:{task_id}:{pod_id}"
    redis_conn.append(key, f"{datetime.now().strftime('[%Y-%m-%d %H:%M:%S.%f]')} {module_prefix} {info}\n")
    redis_conn.expire(key, 60 * 60 * 24 * 30)
    if fetion:
        fetion_alert(info, QQ if user_name in QQ1_USERS else None, stdout=False)


def debug(info, pod_id='manager', fetion=False):
    if logger:
        logger.debug(info)
    record_in_redis_and_fetion('[D] ' + str(info), pod_id, fetion)


def error(info, pod_id='manager', fetion=False):
    if logger:
        logger.error(info)
    record_in_redis_and_fetion('[E] ' + str(info), pod_id, fetion)
