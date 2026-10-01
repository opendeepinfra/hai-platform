'''
列出集群侧工作区文件（分页）—— API-04（FR-04）。

这是 F1 的直接修复点：**items/total 必须真实**。
桩实现返回空列表会让客户端把集群目录判为空，从而每次 push 都全量重传。
'''

import math
import os

from logm import logger

from cloud_storage.utils import (get_base_path, get_files_cache, cache, check_is_subpath,
                                 ClientException, PROVIDER)
from cloud_storage.utils import status_recorder
from conf.utils import FileType, FilePrivacy, hashkey

from .context import ensure_cloud_storage_configured, get_max_page_size
from .errors import WorkspaceError, ErrorCode

CACHE_TTL_SECONDS = 30


async def list_cluster_files_page(user, name: str, file_type: FileType, subpaths,
                                  no_checksum: bool = False, no_hfignore: bool = False,
                                  page: int = 1, size: int = 100) -> dict:
    ensure_cloud_storage_configured()
    if not name or '/' in name:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, f'工作区名非法: {name}')

    max_page_size = get_max_page_size()
    try:
        page = max(1, int(page))
    except Exception:
        page = 1
    try:
        size = int(size)
    except Exception:
        size = 100
    size = max(1, min(size, max_page_size))

    try:
        cluster_base_path, _ = get_base_path(user.user_name, user.shared_group, name,
                                             file_type, FilePrivacy.GROUP_SHARED)
    except Exception as e:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, str(e))

    path_list = [p for p in (subpaths or []) if p not in (None, '')]
    if not path_list:
        path_list = ['./']
    # 客户端把相对子路径塞进 Body；这里只允许工作区内的相对路径
    normalized = []
    for p in path_list:
        if os.path.isabs(p) or '..' in p.split('/'):
            raise WorkspaceError(ErrorCode.PATH_ESCAPE, f'不支持遍历工作区外的路径: {p}')
        normalized.append(p)
    normalized.sort()

    key = (f'{PROVIDER}:file_cache:'
           f'{hashkey(cluster_base_path, *normalized, str(no_checksum), str(no_hfignore), "True")}')
    try:
        files = await get_files_cache(key, cluster_base_path, normalized,
                                      no_checksum, no_hfignore, True)
    except ClientException as ce:
        # 翻页期间文件被删除：返回可重试错误，而不是错误的空页（FR-04 / FI-12）
        await status_recorder.a_delete(key)
        cache.pop(key, None)
        raise WorkspaceError(ErrorCode.CLIENT_RETRY, str(ce))
    except Exception as e:
        await status_recorder.a_delete(key)
        cache.pop(key, None)
        logger.error(f'列出集群文件失败 {cluster_base_path}/{normalized}: {str(e)}')
        raise WorkspaceError(ErrorCode.INTERNAL_ERROR, f'列出集群文件失败: {str(e)}', http_status=500)

    total = len(files)
    pages = max(1, math.ceil(total / size)) if total else 0
    start = (page - 1) * size
    end = start + size
    page_items = files[start:end]

    items = []
    for fi in page_items:
        item = fi.dict() if hasattr(fi, 'dict') else dict(fi)
        # 一律过滤 .hfai（打包上传的临时 zip），双端约定（FR-10）
        path = item.get('path') or ''
        if path.split('/')[0] == '.hfai':
            continue
        if no_checksum:
            item.pop('md5', None)
        items.append(item)

    # 该页恰好落在被过滤项上时，items 会变少；这里不改 total，保持 total 真实
    logger.debug(f'[WORKSPACE] list_cluster_files user={user.user_name} name={name} '
                 f'page={page} size={size} total={total} returned={len(items)}')
    return {
        'items': items,
        'total': total,
        'page': page,
        'size': size,
        'pages': pages,
    }
