'''
集群 -> bucket 同步（pull 的 stage1）—— API-07（FR-07 / FR-09 / FR-11 / FR-17）。

语义与 cloud_storage/api.py 的 `_sync_from_cluster_impl` 保持一致，但：
- 入参是已解析的 User 对象
- 配额预检失败返回 QUOTA_EXCEEDED(403) 而不是裸 HTTPException
- 终态 TTL >= 1800s；index 归属写入 Redis
'''

import asyncio
import os
import threading
import time
import ujson
from concurrent.futures import wait
from functools import partial

from logm import logger
from cloud_storage.metrics import RUNNING_TASKS_GAUGE, SYNCING_FILESIZE_GAUGE
from cloud_storage.utils import (get_base_path, get_bucket_name, check_is_subpath,
                                 status_recorder, status_key, record_metrics, SyncPhase,
                                 ClientException, filter_synced_files,
                                 async_list_local_files_inner)
from conf.utils import (FileType, FilePrivacy, DatasetType, SyncDirection, SyncStatus,
                        FileInfo, slice_bytes, hashkey)

from .context import (get_worker_pools, ensure_cloud_storage_configured, check_feature_enabled,
                      get_max_files_per_request)
from .errors import WorkspaceError, ErrorCode
from .status import set_owner, finalize_status
from .sync_to_cluster import param_key
from .transfer import resumable_upload_with_retry, upload_callback, batch_delete_objects_with_retry


def _quota_limit_mb(user, default=102400):
    '''读取 pull（集群->bucket）累积容量限额（FR-17）。读不到时返回默认值并告警。'''
    try:
        limit = user.quota.cloud_storage_quota.download
        return int(limit)
    except Exception as e:
        logger.warning(f'读取 cloud_storage_quota.download 失败，使用默认限额 {default}MB: {str(e)}')
        return default


async def submit_from_cluster(user, name: str, file_type: FileType, file_infos,
                              force: bool = False) -> dict:
    ensure_cloud_storage_configured()
    check_feature_enabled(user)

    if not name or '/' in name:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, f'工作区名非法: {name}')
    if file_type not in (FileType.WORKSPACE, FileType.ENV):
        raise WorkspaceError(ErrorCode.INVALID_PARAM, f'不支持同步 {file_type} 类型')

    file_infos = list(file_infos or [])
    max_files = get_max_files_per_request()
    if len(file_infos) > max_files:
        raise WorkspaceError(ErrorCode.TOO_MANY_FILES,
                             f'单次请求文件数 {len(file_infos)} 超过上限 {max_files}')

    try:
        cluster_base_path, cloud_base_path = get_base_path(
            user.user_name, user.shared_group, name, file_type, FilePrivacy.GROUP_SHARED, DatasetType.MINI)
    except Exception as e:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, str(e))

    file_list = [f.path for f in file_infos]
    index = hashkey(user.token, name, file_type, *file_list)
    logger.debug(f'hashkey for {name}, {file_type}, {file_list}: {index}')
    await set_owner(index, True, user)

    if not force:
        phase = await status_recorder.a_get(status_key(index, 'status', True))
        if phase in (SyncPhase.RUNNING, SyncPhase.INIT):
            msg = f'上一次同步 {file_list} 正在进行中, 忽略本次请求'
            logger.warning(msg)
            return {'index': index, 'accepted': 0, 'skipped': 0, 'msg': msg}

    index_info = {
        'username': user.user_name,
        'group': user.shared_group,
        'name': name,
        'file_type': file_type.value,
        'file_infos': [f.dict() for f in file_infos],
        'index': index,
        'instance': None,
        'created_at': time.time(),
    }
    await status_recorder.a_set(param_key(index, True), ujson.dumps(index_info))

    # 1) 路径与归属校验 + 补齐缺失的 md5/size
    upload_file_infos = []
    with record_metrics('set_sync_status'):
        await user.aio_db.set_sync_status(file_type, name, SyncDirection.PULL,
                                          SyncStatus.STAGE1_RUNNING)
    await status_recorder.a_set(status_key(index, 'status', True), SyncPhase.INIT)

    try:
        for fi in file_infos:
            path = fi.path
            if '../' in path or '..\\' in path or path == '' or path.startswith('/'):
                raise WorkspaceError(ErrorCode.PATH_ESCAPE, f'不支持上传路径 "{path}"')
            src_file = os.path.join(cluster_base_path, path)
            try:
                check_is_subpath(cluster_base_path, src_file)
            except ClientException as ce:
                raise WorkspaceError(ErrorCode.PATH_ESCAPE, str(ce))
            # 符号链接指向工作区之外的必须拒绝（SEC-03）：
            # 用 realpath 之后再走一次 check_is_subpath，防止软链把文件指到工作区外
            if os.path.exists(src_file):
                real_src = os.path.realpath(src_file)
                try:
                    check_is_subpath(os.path.realpath(cluster_base_path), real_src)
                except ClientException:
                    raise WorkspaceError(ErrorCode.PATH_ESCAPE, f'符号链接指向工作区之外: {path}')

            if fi.md5 is None or fi.size is None or fi.last_modified is None:
                files = await async_list_local_files_inner(cluster_base_path, path, False, True)
                if len(files) == 0:
                    logger.warning(f'未找到本地文件{src_file}, 可能被hfignore忽略或被删除')
                upload_file_infos.extend(files)
            else:
                upload_file_infos.append(fi)

        bucket_name = get_bucket_name(file_type, FilePrivacy.GROUP_SHARED)

        with record_metrics('get_usage_in_mb'):
            usage_in_mb = await user.downloaded_files.get_usage_in_mb()
        upload_size = sum([f.size or 0 for f in upload_file_infos])
        filtered = False
        original_upload_file_infos = upload_file_infos.copy()
        if upload_size > 1073741824:
            # 总上传文件大于1G时才过滤已上传文件，避免过度调用 cloud api
            upload_file_infos = await filter_synced_files(bucket_name, index, cloud_base_path,
                                                          upload_file_infos)
            upload_size = sum([f.size or 0 for f in upload_file_infos])
            filtered = True
        upload_mb = upload_size // 1024 // 1024
        limit = _quota_limit_mb(user)
        logger.debug(f'quota校验, request size: {upload_mb}MB, used size: {usage_in_mb}MB, limit size: {limit}MB')
        if usage_in_mb + upload_mb >= limit:
            raise WorkspaceError(
                ErrorCode.QUOTA_EXCEEDED,
                f'请联系管理员提升pull限额, 请求: {upload_mb}MB, 已用: {usage_in_mb}MB, 限额: {limit}MB',
                http_status=403)
    except WorkspaceError as we:
        await status_recorder.a_delete(param_key(index, True))
        await status_recorder.a_set(status_key(index, 'status', True), SyncPhase.FAILED)
        with record_metrics('set_sync_status'):
            await user.aio_db.set_sync_status(file_type, name, SyncDirection.PULL,
                                              SyncStatus.STAGE1_FAILED)
        raise we
    except Exception as e:
        await status_recorder.a_delete(param_key(index, True))
        await status_recorder.a_set(status_key(index, 'status', True), SyncPhase.FAILED)
        with record_metrics('set_sync_status'):
            await user.aio_db.set_sync_status(file_type, name, SyncDirection.PULL,
                                              SyncStatus.STAGE1_FAILED)
        raise WorkspaceError(ErrorCode.INTERNAL_ERROR, str(e), http_status=500)

    worker_pools = get_worker_pools()
    futures = list()
    msg = ''
    src_files = [os.path.join(cluster_base_path, f.path) for f in upload_file_infos]
    dst_files = [os.path.join(cloud_base_path, f.path) for f in upload_file_infos]

    logger.info(f'开始上传本地目录 {src_files} 到远端，总共{upload_mb}MB...')
    await status_recorder.a_set(status_key(index, 'status', True), SyncPhase.RUNNING)
    accepted, skipped = 0, 0
    for i in range(len(src_files)):
        if not force and dst_files[i] in await status_recorder.a_get_hkeys(status_key(index, 'progress', True)):
            warn_msg = f'{dst_files[i]} 正在上传队列中, 忽略本次请求;'
            logger.warning(warn_msg)
            msg += warn_msg
            skipped += 1
            continue

        loop = asyncio.get_running_loop()
        pool = worker_pools.get(index)
        func_call = partial(pool.submit,
                            resumable_upload_with_retry,
                            bucket_name=bucket_name,
                            key=dst_files[i],
                            filename=src_files[i],
                            multipart_threshold=slice_bytes,
                            part_size=slice_bytes,
                            num_threads=4,
                            index=index,
                            user_name=user.user_name,
                            user_role=user.role,
                            file_type=file_type,
                            file_info=upload_file_infos[i],
                            filtered=filtered,
                            retries=10)
        future = await loop.run_in_executor(None, func_call)
        future.add_done_callback(upload_callback)
        futures.append(future)
        RUNNING_TASKS_GAUGE.labels('pull', user.user_name, file_type).inc()
        SYNCING_FILESIZE_GAUGE.labels('pull', user.user_name, file_type).inc(upload_file_infos[i].size or 0)
        accepted += 1

    args = (futures, index, user, file_type, name)
    if file_type in (FileType.DOC, FileType.PYPI):
        args = (futures, index, user, file_type, name, bucket_name,
                [f.path for f in original_upload_file_infos], cloud_base_path)
    threading.Thread(target=wait_from_cluster,
                     name=f'upload-{index}',
                     args=args,
                     daemon=True).start()

    logger.info(f'[WORKSPACE] 提交上传任务成功 user={user.user_name} name={name} '
                f'index={index} files={len(src_files)}')
    return {'index': index, 'accepted': accepted, 'skipped': skipped,
            'upload_mb': upload_mb, 'msg': f'提交同步任务成功, 文件列表: {dst_files}'}


def wait_from_cluster(futures, index, user, file_type, name,
                      bucket_name=None, keys=None, cloud_base_path=None):
    logger.info(f'开始等待上传任务 {index}..')
    wait(futures)
    msg = ''
    for future in futures:
        e = future.exception()
        if e:
            msg += f'{str(e)};'

    # 对于doc/pypi类型，删除 bucket 上的过期文件
    if bucket_name and keys:
        try:
            from cloud_storage.utils import cloud_api
            prefix = cloud_base_path + '/' if cloud_base_path else ''
            file_infos = cloud_api.list_bucket(bucket_name, prefix)[0]
            delete_candidates = [obj.path for obj in file_infos if obj.path[len(prefix):] not in keys]
            for i in range(0, len(delete_candidates), 500):
                batch_delete_objects_with_retry(bucket_name, delete_candidates[i:i + 500])
        except Exception as e:
            msg += f'{str(e)};'

    logger.info(f'上传任务 {index} 完成: {msg or SyncPhase.FINISHED}')
    get_worker_pools().finish(index)
    status_recorder.delete(param_key(index, True))
    finalize_status(index, True, msg or SyncPhase.FINISHED)

    status = SyncPhase.FINISHED if not msg else SyncStatus.STAGE1_FAILED
    try:
        with record_metrics('set_sync_status'):
            user.db.set_sync_status(file_type, name, SyncDirection.PULL, status)
    except Exception as e:
        logger.error(f'写入同步终态失败（不影响传输）: {str(e)}')


async def execute_from_cluster(user, name: str, file_type: FileType, file_infos, index: str = None,
                               force: bool = True, **kwargs):
    '''崩溃恢复入口。file_infos 可能是 dict 列表（来自 Redis 快照）。'''
    infos = []
    for fi in file_infos or []:
        if isinstance(fi, dict):
            infos.append(FileInfo(**fi))
        elif isinstance(fi, FileInfo):
            infos.append(fi)
        else:
            infos.append(FileInfo.parse_obj(fi))
    return await submit_from_cluster(user, name, file_type, infos, force=force)
