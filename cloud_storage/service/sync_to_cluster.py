'''
bucket -> 集群 同步（push 的 stage2）—— API-05（FR-05 / FR-10 / FR-11 / FR-20）。

语义与 cloud_storage/api.py 的 `_sync_to_cluster_impl` 保持一致，但：
- 入参是已解析的 User 对象（身份只来自 token，SEC-01）
- 不再依赖 FastAPI / Body 模型
- 终态 TTL 改为 >= 1800s（ADR-6）
- index 归属写入 Redis（SEC-04）
'''

import asyncio
import os
import threading
import time
import ujson
from concurrent.futures import wait
from functools import partial

from logm import logger
from cloud_storage.metrics import RUNNING_TASKS_GAUGE
from cloud_storage.utils import (get_base_path, get_bucket_name, check_is_subpath,
                                 status_recorder, status_key, record_metrics,
                                 SyncPhase, ClientException)
from conf.utils import FileType, FilePrivacy, DatasetType, SyncDirection, SyncStatus, slice_bytes, hashkey

from .context import (get_worker_pools, get_instance_id, get_pod_id,
                      ensure_cloud_storage_configured, check_feature_enabled,
                      get_max_files_per_request)
from .errors import WorkspaceError, ErrorCode
from .status import set_owner, finalize_status
from .transfer import resumable_download_with_retry, download_callback


def param_key(index: str, is_upload: bool) -> str:
    from cloud_storage.utils import PROVIDER
    top = 'sync_from_cluster' if is_upload else 'sync_to_cluster'
    return f'{PROVIDER}:{top}:{index}:param:{get_instance_id()}'


async def submit_to_cluster(user, name: str, file_type: FileType, files,
                            no_zip: bool = False, force: bool = False) -> dict:
    ensure_cloud_storage_configured()
    check_feature_enabled(user)

    if not name or '/' in name:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, f'工作区名非法: {name}')
    if file_type not in (FileType.WORKSPACE, FileType.ENV):
        raise WorkspaceError(ErrorCode.INVALID_PARAM, f'不支持同步 {file_type} 类型')

    files = list(files or [])
    max_files = get_max_files_per_request()
    if len(files) > max_files:
        raise WorkspaceError(ErrorCode.TOO_MANY_FILES,
                             f'单次请求文件数 {len(files)} 超过上限 {max_files}')

    try:
        cluster_base_path, cloud_base_path = get_base_path(
            user.user_name, user.shared_group, name, file_type, FilePrivacy.GROUP_SHARED, DatasetType.MINI)
    except Exception as e:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, str(e))

    bucket_name = get_bucket_name(file_type, FilePrivacy.GROUP_SHARED)
    index = hashkey(user.token, name, file_type, *files)
    logger.debug(f'hashkey for {name}, {file_type}, {files}: {index}')

    await set_owner(index, False, user)

    if len(files) == 0:
        finalize_status(index, False, SyncPhase.FINISHED)
        return {'index': index, 'dst_path': cluster_base_path, 'accepted': 0, 'skipped': 0,
                'msg': 'files already synced'}

    if not force:
        phase = await status_recorder.a_get(status_key(index, 'status', False))
        if phase == SyncPhase.RUNNING:
            msg = f'上一次同步 {files} 正在进行中, 忽略本次请求'
            logger.warning(msg)
            return {'index': index, 'dst_path': cluster_base_path, 'accepted': 0, 'skipped': 0, 'msg': msg}

    # 参数快照，供崩溃恢复（FR-13）
    index_info = {
        'username': user.user_name,
        'group': user.shared_group,
        'name': name,
        'file_type': file_type.value,
        'no_zip': no_zip,
        'file_list': files,
        'index': index,
        'instance': get_instance_id(),
        'created_at': time.time(),
    }
    await status_recorder.a_set(param_key(index, False), ujson.dumps(index_info))

    with record_metrics('set_sync_status'):
        await user.aio_db.set_sync_status(file_type, name, SyncDirection.PUSH,
                                          SyncStatus.STAGE2_RUNNING, '', cluster_base_path)
    await status_recorder.a_set(status_key(index, 'status', False), SyncPhase.RUNNING)

    if not os.path.exists(cluster_base_path):
        os.makedirs(cluster_base_path, exist_ok=True)
        try:
            os.chown(cluster_base_path, int(user.user_id), int(user.user_id))
        except Exception as e:
            logger.warning(f'chown {cluster_base_path} 失败（忽略）: {str(e)}')

    worker_pools = get_worker_pools()
    subpath_set = set()
    futures = list()
    msg = ''
    accepted, skipped = 0, 0
    for fname in files:
        key = os.path.join(cloud_base_path, fname)
        use_zip = (not no_zip) and fname.endswith('.zip')
        if use_zip:
            local_path = os.path.join(cluster_base_path, '.hfai', fname)
        else:
            local_path = os.path.join(cluster_base_path, fname)
        try:
            check_is_subpath(cluster_base_path, local_path)
        except ClientException as ce:
            raise WorkspaceError(ErrorCode.PATH_ESCAPE, str(ce))

        dirname = os.path.dirname(local_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname, exist_ok=True)
            subs = fname.split('/')
            subpath = cluster_base_path
            for sub in subs[:-1]:
                subpath += f'/{sub}'
                if subpath not in subpath_set:
                    try:
                        os.chown(subpath, int(user.user_id), int(user.user_id))
                    except Exception as e:
                        logger.warning(f'chown {subpath} 失败（忽略）: {str(e)}')
                    subpath_set.add(subpath)

        if not force and key in await status_recorder.a_get_hkeys(status_key(index, 'progress', False)):
            warn_msg = f'{key} 正在下载队列中, 忽略本次请求;'
            logger.warning(warn_msg)
            msg += warn_msg
            skipped += 1
            continue

        logger.debug(f'提交下载文件 {key}')
        loop = asyncio.get_running_loop()
        pool = worker_pools.get(index)
        func_call = partial(pool.submit,
                            resumable_download_with_retry,
                            bucket_name=bucket_name,
                            key=key,
                            filename=local_path,
                            file_type=file_type,
                            multiget_threshold=slice_bytes,
                            part_size=slice_bytes,
                            num_threads=4,
                            index=index,
                            username=user.user_name,
                            userid=user.user_id,
                            use_zip=use_zip,
                            retries=10)
        future = await loop.run_in_executor(None, func_call)
        future.add_done_callback(download_callback)
        futures.append(future)
        RUNNING_TASKS_GAUGE.labels('push', user.user_name, file_type).inc()
        accepted += 1

    threading.Thread(target=wait_to_cluster,
                     name=f'download-{index}',
                     args=(futures, index, user, file_type, name),
                     daemon=True).start()

    logger.info(f'[WORKSPACE] 提交同步任务成功 user={user.user_name} name={name} '
                f'index={index} files={len(files)}')
    return {'index': index, 'dst_path': cluster_base_path, 'accepted': accepted,
            'skipped': skipped, 'msg': '提交同步任务成功' + msg}


async def execute_to_cluster(user, name: str, file_type: FileType, files, no_zip: bool = False,
                             index: str = None, force: bool = True, **kwargs):
    '''崩溃恢复入口：以 force=True 重新提交。'''
    return await submit_to_cluster(user, name, file_type, files, no_zip=no_zip, force=force)


def wait_to_cluster(futures, index, user, file_type, name):
    logger.info(f'开始等待下载任务 {index}..')
    wait(futures)
    msg = ''
    for future in futures:
        e = future.exception()
        if e:
            msg += f'{str(e)};'
    logger.info(f'下载任务 {index} 完成: {msg or SyncPhase.FINISHED}')

    get_worker_pools().finish(index)
    status_recorder.delete(param_key(index, False))
    finalize_status(index, False, msg or SyncPhase.FINISHED)
    # 记录数据库终态；DB 失败不得影响传输结果（FR-12）
    status = SyncPhase.FINISHED if not msg else SyncStatus.STAGE2_FAILED
    try:
        with record_metrics('set_sync_status'):
            user.db.set_sync_status(file_type, name, SyncDirection.PUSH, status)
    except Exception as e:
        logger.error(f'写入同步终态失败（不影响传输）: {str(e)}')
