'''
崩溃恢复（FR-13 / ADR-5 / CON-10）。

ugc-server 有 2 个 uvicorn worker（one/one_etc/core.toml: ugc = 2），
因此「启动即恢复」的逻辑必须互斥，否则同一个任务会被两个进程各恢复一次。

互斥方案（精简版，够 P0 用）：
1. 每个进程启动时把自己的 instance_id 写进 pod 级心跳 hash，并定期续期；
2. 恢复前抢 pod 级 SET NX 锁 `{PROVIDER}:recover:{pod_id}`；
3. 只认领「param 快照里的 instance 已经不在心跳表里」的任务（说明那个进程死了）。

心跳不可用时退化为「快照写入时间超过 recover_stale_seconds」的时间阈值。
'''

import asyncio
import os
import threading
import time
import ujson

from logm import logger

from cloud_storage.utils import status_recorder, PROVIDER
from conf.utils import FileType, FileList, FileInfoList

from .context import (get_instance_id, get_pod_id, get_worker_pools,
                      check_cloud_storage_config)

HEARTBEAT_KEY = 'cloud_storage:instances:{pod_id}'
RECOVER_LOCK_KEY = f'{PROVIDER}:recover:{{pod_id}}'
HEARTBEAT_INTERVAL = 30
HEARTBEAT_EXPIRE = 120
RECOVER_LOCK_TTL = 300


def _heartbeat_key():
    return HEARTBEAT_KEY.format(pod_id=get_pod_id())


def _recover_lock_key():
    return RECOVER_LOCK_KEY.format(pod_id=get_pod_id())


async def heartbeat_once():
    try:
        await status_recorder.a_hset(_heartbeat_key(), get_instance_id(), int(time.time()))
        await status_recorder.a_expire(_heartbeat_key(), HEARTBEAT_EXPIRE)
    except Exception as e:
        logger.warning(f'写实例心跳失败: {str(e)}')


def start_heartbeat_thread():
    def _loop():
        while True:
            try:
                asyncio.run(heartbeat_once())
            except Exception as e:
                logger.warning(f'心跳线程异常: {str(e)}')
            time.sleep(HEARTBEAT_INTERVAL)

    threading.Thread(target=_loop, name='cloud-storage-heartbeat', daemon=True).start()


async def recover_interrupted_tasks():
    '''
    扫描参数快照，认领并重跑未完成任务。多 worker 下只有一个能拿到锁。
    '''
    from .sync_to_cluster import execute_to_cluster, param_key
    from .sync_from_cluster import execute_from_cluster

    try:
        got_lock = await status_recorder.a_set_nx(_recover_lock_key(), get_instance_id(),
                                                  expires=RECOVER_LOCK_TTL)
    except Exception as e:
        logger.warning(f'获取恢复锁失败，跳过本次恢复: {str(e)}')
        return
    if not got_lock:
        logger.info('恢复锁已被其他 worker 持有，跳过本次恢复（多 worker 互斥生效）')
        return

    my_instance = get_instance_id()
    try:
        keys = status_recorder.get_keys(f'{PROVIDER}:*:*:param:*')
    except Exception as e:
        logger.error(f'扫描待恢复任务失败: {str(e)}')
        return

    for k in keys:
        try:
            raw = status_recorder.get(k)
            if not raw:
                continue
            param = ujson.loads(raw)
            # 只认领「属于本实例（自己重启前）」或「原实例已死亡」的任务
            owner_instance = param.get('instance')
            if owner_instance and owner_instance != my_instance:
                try:
                    alive = await status_recorder.a_get_hall(_heartbeat_key())
                except Exception:
                    alive = {}
                if owner_instance in alive:
                    logger.info(f'任务 {k} 的原实例 {owner_instance} 仍存活，跳过')
                    continue
            if not owner_instance and not _stale(param):
                continue

            parts = k.split(':')
            direction = parts[1] if len(parts) > 1 else ''
            param['instance'] = my_instance
            if direction == 'sync_to_cluster':
                files = param.get('file_list') or []
                file_type = FileType(param['file_type'])
                from server_model.selector import AioUserSelector
                user = await AioUserSelector.find_one(user_name=param['username'])
                if user is None:
                    logger.error(f'恢复失败，找不到用户 {param["username"]}')
                    continue
                logger.info(f'恢复 sync_to_cluster 任务: {param["index"]}')
                await execute_to_cluster(user, param['name'], file_type, files,
                                         no_zip=param.get('no_zip', False),
                                         index=param['index'], force=True)
            elif direction == 'sync_from_cluster':
                infos = param.get('file_infos') or []
                if isinstance(infos, dict):
                    infos = infos.get('files') or []
                elif not isinstance(infos, list):
                    infos = []
                file_type = FileType(param['file_type'])
                from server_model.selector import AioUserSelector
                user = await AioUserSelector.find_one(user_name=param['username'])
                if user is None:
                    logger.error(f'恢复失败，找不到用户 {param["username"]}')
                    continue
                logger.info(f'恢复 sync_from_cluster 任务: {param["index"]}')
                await execute_from_cluster(user, param['name'], file_type, infos,
                                           index=param['index'], force=True)
        except Exception as e:
            logger.error(f'恢复任务 {k} 失败: {str(e)}')


def _stale(param, stale_seconds=600):
    created_at = param.get('created_at')
    if not created_at:
        return True
    try:
        return (time.time() - float(created_at)) > stale_seconds
    except Exception:
        return True


async def recover_on_startup():
    '''接入层 startup 钩子调用（ADR-12）；带开关，配置缺失时静默跳过。'''
    from .context import cfg
    if check_cloud_storage_config():
        logger.warning('云存储未配置，跳过崩溃恢复')
        return
    if not bool(cfg('cloud.storage.service.recover_on_startup', default=True)):
        logger.info('recover_on_startup=false，跳过崩溃恢复')
        return
    start_heartbeat_thread()
    await recover_interrupted_tasks()
