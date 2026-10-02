'''
签发对象存储临时凭证 —— API-01（FR-01 / SEC-02）。

- ttl 夹取到 [900, 43200]
- 授权范围只能是本用户的 workspace 前缀（cloud_base_path）
- provider 键名必须等于客户端配置的 provider（默认 oss）
'''

from logm import logger

from cloud_storage.utils import get_base_path, get_bucket_name
from conf.utils import FileType, FilePrivacy

from .context import get_provider_name, ensure_cloud_storage_configured
from .errors import WorkspaceError, ErrorCode


def _clamp_ttl(ttl_seconds) -> int:
    try:
        ttl = int(ttl_seconds)
    except Exception:
        ttl = 1800
    return max(900, min(ttl, 43200))


async def issue_sts_token(user, name: str, file_type: FileType,
                          ttl_seconds=1800) -> dict:
    ensure_cloud_storage_configured()
    if not name or '/' in name:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, f'工作区名非法: {name}')

    if file_type == FileType.IMAGE:
        # 签发凭证 = 上传通道的入口，必须与 API-05 共用同一个闸门（FR-19 / HC-12）。
        # 否则 upload_enabled=false 时用户仍能拿到凭证把 tar 写进对象存储（一级回滚不成立）。
        from .context import check_image_upload_enabled, image_upload_precheck_required
        check_image_upload_enabled(user)
        if image_upload_precheck_required():
            from .status import image_precheck_done
            if not await image_precheck_done(user, name):
                raise WorkspaceError(ErrorCode.INVALID_PARAM,
                                     f'请先执行 `hai-cli images push` 的预检（name={name}）')

    try:
        _, cloud_base_path = get_base_path(user.user_name, user.shared_group, name,
                                           file_type, FilePrivacy.GROUP_SHARED)
    except Exception as e:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, str(e))

    bucket_name = get_bucket_name(file_type, FilePrivacy.GROUP_SHARED)
    ttl = _clamp_ttl(ttl_seconds)

    # 惰性获取 provider（避免导入期副作用）
    from cloud_storage.utils import cloud_api
    try:
        resp = cloud_api.get_access_token(bucket_name, cloud_base_path, ttl)
    except WorkspaceError:
        raise
    except Exception as e:
        logger.error(f'签发对象存储凭证失败: {str(e)}')
        raise WorkspaceError(ErrorCode.INTERNAL_ERROR, f'签发对象存储凭证失败: {str(e)}', http_status=500)

    logger.info(f'[WORKSPACE] 签发凭证 user={user.user_name} name={name} '
                f'file_type={file_type} bucket={bucket_name} ttl={ttl} prefix={cloud_base_path}')
    return resp


def provider_key() -> str:
    return get_provider_name()
