'''
/ugc/* 接入层（契约适配 + 鉴权 + 兼容层 + 错误码），业务逻辑全部在 cloud_storage/service。

需求映射：
  API-01 get_sts_token            FR-01
  API-02 set_sync_status          FR-02
  API-03 get_sync_status          FR-03
  API-04 cloud/cluster_files/list FR-04
  API-05 sync_to_cluster          FR-05
  API-06 sync_to_cluster/status   FR-06
  API-07 sync_from_cluster        FR-07
  API-08 sync_from_cluster/status FR-06
  API-09 delete_files             FR-08

说明（ADR-4）：这些函数放在 default.py（而不是 implement.py）里，
部署私有的 api/resource/cloud_storage/custom.py 可以同名覆盖。
'''

from fastapi import Depends, Request

from logm import logger

from api.depends import get_ugc_user
from cloud_storage.service import (WorkspaceError, ErrorCode, cfg,
                                   normalize_enum, normalize_bool, parse_json_body,
                                   unwrap_files, issue_sts_token, list_cluster_files_page,
                                   get_transfer_status, submit_to_cluster, submit_from_cluster,
                                   delete_paths)
from conf.utils import FileType, SyncDirection, SyncStatus, FileInfo


async def _require_config():
    '''配置缺失时返回 CLOUD_STORAGE_NOT_CONFIGURED，而不是抛 500（FR-19 / DEV-02）。'''
    from cloud_storage.service.context import ensure_cloud_storage_configured
    ensure_cloud_storage_configured()


# --------------------------------------------------------------------------- API-01

async def get_sts_token(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    params = request.query_params
    file_type = normalize_enum(params.get('file_type'), FileType, 'file_type')
    if file_type is None:
        file_type = FileType.WORKSPACE
    name = params.get('name') or ''
    ttl_seconds = params.get('ttl_seconds') or 1800
    resp = await issue_sts_token(user, name, file_type, ttl_seconds)
    provider = cfg('cloud.storage.provider', default='oss')
    return {'success': 1, provider: resp}


# --------------------------------------------------------------------------- API-02

async def set_sync_status(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    params = request.query_params
    file_type = normalize_enum(params.get('file_type'), FileType, 'file_type', default=FileType.WORKSPACE)
    direction = normalize_enum(params.get('direction'), SyncDirection, 'direction', default=SyncDirection.PUSH)
    status = normalize_enum(params.get('status'), SyncStatus, 'status', default=SyncStatus.INIT)
    name = params.get('name') or ''
    if not name:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, 'name 不能为空')
    # CON-6：local_path / cluster_path 只是展示性元数据，截断后入库，绝不参与路径推导
    local_path = (params.get('local_path') or '')[:2047]
    cluster_path = (params.get('cluster_path') or '')[:2047]

    await user.aio_db.set_sync_status(file_type, name, direction, status,
                                      local_path, cluster_path)
    logger.debug(f'[WORKSPACE] set_sync_status user={user.user_name} name={name} '
                 f'file_type={file_type.value} direction={direction.value} status={status.value}')
    return {'success': 1}


# --------------------------------------------------------------------------- API-03

async def get_sync_status(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    params = request.query_params
    file_type = normalize_enum(params.get('file_type'), FileType, 'file_type', default=FileType.WORKSPACE)
    name = params.get('name') or '*'
    data = await user.aio_db.get_sync_status(file_type, name)
    # 无记录时必须返回 data: [] + success: 1（客户端据此打印「没找到工作区」）
    return {'success': 1, 'data': data or []}


# --------------------------------------------------------------------------- API-04

async def list_cluster_files(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    params = request.query_params
    name = params.get('name') or ''
    file_type = normalize_enum(params.get('file_type'), FileType, 'file_type', default=FileType.WORKSPACE)
    no_checksum = normalize_bool(params.get('no_checksum'), 'no_checksum')
    no_hfignore = normalize_bool(params.get('no_hfignore'), 'no_hfignore')
    # 客户端固定 recursive=True
    page = params.get('page') or 1
    size = params.get('size') or 100

    body = await parse_json_body(request)
    files = unwrap_files(body, 'file_list')
    subpaths = [f for f in files if isinstance(f, str)]

    result = await list_cluster_files_page(user, name, file_type, subpaths,
                                           no_checksum, no_hfignore, page, size)
    result['success'] = 1
    return result


# --------------------------------------------------------------------------- API-05

async def sync_to_cluster(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    params = request.query_params
    name = params.get('name') or ''
    file_type = normalize_enum(params.get('file_type'), FileType, 'file_type', default=FileType.WORKSPACE)
    no_zip = normalize_bool(params.get('no_zip'), 'no_zip', default=False)

    body = await parse_json_body(request)
    files = [f for f in unwrap_files(body, 'file_list') if isinstance(f, str)]

    result = await submit_to_cluster(user, name, file_type, files, no_zip=no_zip)
    result['success'] = 1
    return result


# --------------------------------------------------------------------------- API-06

async def sync_to_cluster_status(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    index = request.query_params.get('index') or ''
    result = await get_transfer_status(user, index, is_upload=False)
    result['success'] = 1
    return result


# --------------------------------------------------------------------------- API-07

async def sync_from_cluster(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    params = request.query_params
    name = params.get('name') or ''
    file_type = normalize_enum(params.get('file_type'), FileType, 'file_type', default=FileType.WORKSPACE)

    body = await parse_json_body(request)
    raw_files = unwrap_files(body, 'file_infos')
    file_infos = []
    for item in raw_files:
        if isinstance(item, dict):
            file_infos.append(FileInfo(**item))
        else:
            file_infos.append(FileInfo.parse_obj(item))

    result = await submit_from_cluster(user, name, file_type, file_infos)
    result['success'] = 1
    return result


# --------------------------------------------------------------------------- API-08

async def sync_from_cluster_status(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    index = request.query_params.get('index') or ''
    result = await get_transfer_status(user, index, is_upload=True)
    result['success'] = 1
    return result


# --------------------------------------------------------------------------- API-09

async def delete_files(request: Request, user=Depends(get_ugc_user)):
    await _require_config()
    params = request.query_params
    name = params.get('name') or ''
    file_type = normalize_enum(params.get('file_type'), FileType, 'file_type', default=FileType.WORKSPACE)

    body = await parse_json_body(request)
    files = [f for f in unwrap_files(body, 'file_list') if isinstance(f, str)]

    result = await delete_paths(user, name, file_type, files)
    result['success'] = 1
    return result


# --------------------------------------------------------------------------- P1 桩（本期不实现）

async def set_external_user_cloud_storage_quota():
    return {'success': 0, 'msg': 'not implemented', 'code': ErrorCode.INTERNAL_ERROR}


# --------------------------------------------------------------------------- 生命周期钩子

async def startup_recover():
    '''
    ugc-server 启动时：打印配置自检结果 + 恢复崩溃前未完成的同步任务（FR-13 / FR-19）。
    任何异常都不能影响 ugc-server 的其他接口。
    '''
    try:
        from cloud_storage.service.context import log_config_self_check
        log_config_self_check()
    except Exception as e:
        logger.error(f'云存储配置自检异常: {str(e)}')
    try:
        from cloud_storage.service.recovery import recover_on_startup
        await recover_on_startup()
    except Exception as e:
        logger.error(f'崩溃恢复失败（不影响服务启动）: {str(e)}')


async def shutdown_workers():
    '''进程退出时回收进程池，避免 fd / 子进程泄漏。'''
    try:
        from cloud_storage.service.context import get_worker_pools
        pools = get_worker_pools()
        for name in list(pools.pools.keys()):
            try:
                pools.finish(name)
            except Exception:
                pass
    except Exception as e:
        logger.error(f'回收进程池失败: {str(e)}')

