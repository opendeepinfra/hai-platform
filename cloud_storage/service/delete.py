'''
删除集群侧工作区/文件 —— API-09（FR-08 / SEC-03 / SEC-07 / SEC-08）。

- 逐个路径做 check_is_subpath 校验（拒绝穿越）
- 不存在视为成功（幂等）
- 空 file_list 表示「删除整个工作区」：必须显式打审计日志 + 软删 DB 行
'''

import os

import aiofiles.os as asyncos
from logm import logger

from cloud_storage.utils import get_base_path, check_is_subpath, rmtree, ClientException
from conf.utils import FileType, FilePrivacy

from .context import ensure_cloud_storage_configured, check_feature_enabled
from .errors import WorkspaceError, ErrorCode


async def delete_paths(user, name: str, file_type: FileType, files) -> dict:
    ensure_cloud_storage_configured()
    check_feature_enabled(user)

    if file_type not in (FileType.WORKSPACE, FileType.ENV):
        raise WorkspaceError(ErrorCode.INVALID_PARAM, f'不支持删除 {file_type} 类型的文件')
    if not name or '/' in name:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, f'工作区名非法: {name}')

    try:
        cluster_base_path, _ = get_base_path(user.user_name, user.shared_group, name,
                                             file_type, FilePrivacy.GROUP_SHARED)
    except Exception as e:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, str(e))

    files = list(files or [])
    whole_workspace = len(files) == 0
    delete_candidates = []
    if whole_workspace:
        delete_candidates = [cluster_base_path]
    else:
        for f in files:
            if '..' in f:
                raise WorkspaceError(ErrorCode.PATH_ESCAPE, f'文件名中禁止包含父目录: {f}')
            candidate = os.path.normpath(os.path.join(cluster_base_path, f.lstrip('/')))
            try:
                check_is_subpath(cluster_base_path, candidate)
            except ClientException as ce:
                raise WorkspaceError(ErrorCode.PATH_ESCAPE, str(ce))
            delete_candidates.append(candidate)

    # SEC-07：删除整个工作区必须显式审计
    if whole_workspace:
        logger.info(f'[WORKSPACE][AUDIT] 删除整个工作区 user={user.user_name} '
                    f'name={name} file_type={file_type.value} path={cluster_base_path}')

    logger.info(f'开始删除 {delete_candidates}')
    for f in delete_candidates:
        try:
            if os.path.isdir(f):
                await rmtree(f)
            else:
                await asyncos.remove(f)
            logger.info(f'删除成功: {f}')
        except FileNotFoundError:
            logger.warning(f'{f} 不存在，跳过删除')
        except Exception as e:
            logger.error(f'删除失败 {f}: {str(e)}')
            raise WorkspaceError(ErrorCode.INTERNAL_ERROR, f'删除失败: {str(e)}', http_status=500)

    # 软删 DB 行（list 不再显示）；失败不影响删除结果
    try:
        await user.aio_db.soft_delete_sync_status(file_type, name)
    except Exception as e:
        logger.error(f'软删 user_sync_status 失败（不影响删除）: {str(e)}')

    return {'msg': f'删除成功: {delete_candidates}'}
