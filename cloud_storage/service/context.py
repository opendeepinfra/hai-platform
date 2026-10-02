'''
领域层上下文：配置读取、provider 构造、自检、灰度开关、单例。

导入本模块（以及 cloud_storage.service 包）在任何配置环境下都必须成功且无副作用
（设计 ADR-11 / ADR-12 / FR-19）：
- 不注册路由、不注册 on_event
- 不读 Redis / 不建进程池（首次使用时才建）
'''

import os

from conf import CONF
from logm import logger

from .errors import WorkspaceError, ErrorCode

# 启动自检要求的必填键（设计 §9.2）
REQUIRED_KEYS = [
    'cloud.storage.provider',
    'cloud.storage.endpoint',
    'cloud.storage.access_key_id',
    'cloud.storage.access_key_secret',
    'cloud.storage.private_bucket',
    'cloud.storage.service.workspace_path',
    'cloud.storage.service.breakpoint_info_path',
]

_DEFAULT_WORKSPACE_PATH = '/nfs_shared/workspace'
_DEFAULT_BREAKPOINT_PATH = '/var/lib/hai/cloud_storage/breakpoints'


def cfg(path: str, default=None):
    return CONF.try_get(path, default=default)


def get_provider_name() -> str:
    return str(cfg('cloud.storage.provider', default='oss') or 'oss').lower()


def get_workspace_path() -> str:
    return str(cfg('cloud.storage.service.workspace_path', default=_DEFAULT_WORKSPACE_PATH))


def get_breakpoint_info_path() -> str:
    path = str(cfg('cloud.storage.service.breakpoint_info_path', default=_DEFAULT_BREAKPOINT_PATH))
    return path


def get_status_ttl_finished() -> int:
    '''终态 TTL 必须 >= 客户端默认 --sync_timeout（1800），见 ADR-6 / CFG-04。'''
    try:
        ttl = int(cfg('cloud.storage.service.status_ttl_finished', default=1800))
    except Exception:
        ttl = 1800
    return max(ttl, 1800)


def get_max_files_per_request() -> int:
    try:
        return int(cfg('cloud.storage.service.max_files_per_request', default=10000))
    except Exception:
        return 10000


def get_max_page_size() -> int:
    try:
        return int(cfg('cloud.storage.service.max_page_size', default=1000))
    except Exception:
        return 1000


def get_num_workers() -> int:
    try:
        return int(os.environ.get('WORKERS', cfg('cloud.storage.service.workers', default=4)))
    except Exception:
        return 4


def check_cloud_storage_config() -> list:
    '''
    返回缺失的配置键列表（不抛异常），供启动时逐项打印（FR-19）。
    '''
    missing = []
    for key in REQUIRED_KEYS:
        value = cfg(key, default=None)
        if value is None or (isinstance(value, str) and value.strip() == ''):
            missing.append(key)
    return missing


def ensure_cloud_storage_configured():
    missing = check_cloud_storage_config()
    if missing:
        raise WorkspaceError(
            ErrorCode.CLOUD_STORAGE_NOT_CONFIGURED,
            '云存储未配置: 缺少 ' + ', '.join(missing))


def log_config_self_check():
    missing = check_cloud_storage_config()
    if missing:
        logger.error('云存储配置自检未通过，缺少以下配置项（云存储相关接口将返回 '
                     'CLOUD_STORAGE_NOT_CONFIGURED）: ' + ', '.join(missing))
    else:
        logger.info(f'云存储配置自检通过，provider={get_provider_name()}, '
                    f'workspace_path={get_workspace_path()}')
    provider = get_provider_name()
    if provider != 'oss':
        logger.warning(f'当前 cloud.storage.provider={provider}，非生产 provider，仅用于本地/测试环境')
    return missing


def check_feature_enabled(user):
    '''
    灰度开关（OPS-01）：enabled / enabled_users / enabled_groups。
    未开启时抛 FEATURE_DISABLED，客户端会看到 success=0。
    '''
    service = cfg('cloud.storage.service', default=None)
    if service is None:
        ensure_cloud_storage_configured()
    if not bool(cfg('cloud.storage.service.enabled', default=True)):
        raise WorkspaceError(ErrorCode.FEATURE_DISABLED, '工作区同步功能未开启')

    enabled_users = cfg('cloud.storage.service.enabled_users', default=None) or []
    if enabled_users and user.user_name not in list(enabled_users):
        raise WorkspaceError(ErrorCode.FEATURE_DISABLED,
                             f'用户 {user.user_name} 不在工作区同步功能白名单内')

    enabled_groups = cfg('cloud.storage.service.enabled_groups', default=None) or []
    if enabled_groups:
        try:
            allowed = user.in_any_group(list(enabled_groups))
        except Exception:
            allowed = False
        if not allowed:
            raise WorkspaceError(ErrorCode.FEATURE_DISABLED,
                                 f'用户 {user.user_name} 所在用户组不在工作区同步功能白名单内')


# ---------------------------------------------------------------------------
# haienv（`hai-cli env push`）—— 设计 docs/haiplatform/env/env-server-design.md §3.4 / §9
# ---------------------------------------------------------------------------

def get_env_path() -> str:
    '''env 家族父根（配置 [cloud.storage.service] env_path），默认 /hf_shared。'''
    from conf.utils import get_env_path as _get_env_path
    return _get_env_path()


def get_env_root() -> str:
    '''env_root = {env_path}/hfai_envs = dirname(HAIENV_PATH)。'''
    from conf.utils import get_env_root as _get_env_root
    return _get_env_root()


def get_user_env_dir(user) -> str:
    from conf.utils import get_user_env_dir as _get_user_env_dir
    return _get_user_env_dir(getattr(user, 'user_name', user))


def get_env_registry_path(user) -> str:
    '''{user_env_dir}/venv.db —— 用户集群侧注册表路径。'''
    from conf.utils import get_env_registry_path as _get_env_registry_path
    return _get_env_registry_path(getattr(user, 'user_name', user))


def get_env_push_enabled() -> bool:
    return bool(cfg('cloud.storage.service.env_push_enabled', default=True))


def check_env_push_enabled(user):
    '''
    env 灰度开关（FR-12 / OPS-02）：env_push_enabled / env_push_enabled_users /
    env_push_enabled_groups。关闭时两个接口都返回 FEATURE_DISABLED，且不写库、不改文件。
    '''
    if not get_env_push_enabled():
        raise WorkspaceError(ErrorCode.FEATURE_DISABLED, 'env 上传功能未开启')

    enabled_users = cfg('cloud.storage.service.env_push_enabled_users', default=None) or []
    if enabled_users and user.user_name not in list(enabled_users):
        raise WorkspaceError(ErrorCode.FEATURE_DISABLED,
                             f'用户 {user.user_name} 不在 env 上传功能白名单内')

    enabled_groups = cfg('cloud.storage.service.env_push_enabled_groups', default=None) or []
    if enabled_groups:
        try:
            allowed = user.in_any_group(list(enabled_groups))
        except Exception:
            allowed = False
        if not allowed:
            raise WorkspaceError(ErrorCode.FEATURE_DISABLED,
                                 f'用户 {user.user_name} 所在用户组不在 env 上传功能白名单内')


def get_env_name_regex() -> str:
    '''
    名称白名单（FR-08）。配置可收紧，但**不允许**放宽到含 '/'（SEC-04）。
    '''
    from conf.utils import ENV_NAME_RE
    pattern = str(cfg('cloud.storage.service.env_name_regex', default=ENV_NAME_RE.pattern) or '')
    if not pattern or '/' in pattern:
        pattern = ENV_NAME_RE.pattern
    return pattern


def build_cloud_api():
    '''
    按配置构造 provider 实例。惰性调用（不要在导入期调用）。
    '''
    provider = get_provider_name()
    endpoint = cfg('cloud.storage.endpoint')
    access_key_id = cfg('cloud.storage.access_key_id')
    access_key_secret = cfg('cloud.storage.access_key_secret')
    breakpoint_info_path = get_breakpoint_info_path()
    proxy_endpoint = cfg('cloud.storage.service.proxy_endpoint', default='')
    proxies = {'http': proxy_endpoint, 'https': proxy_endpoint} if proxy_endpoint else None

    if provider == 'oss':
        from cloud_storage.provider.oss import OSSApi
        return OSSApi(endpoint,
                      access_key_id,
                      access_key_secret,
                      uid=cfg('cloud.storage.uid'),
                      role_arn=cfg('cloud.storage.role_arn'),
                      breakpoint_info_path=breakpoint_info_path,
                      proxies=proxies)
    if provider in ('s3', 'rustfs'):
        from cloud_storage.provider.s3 import S3Api
        return S3Api(endpoint,
                     access_key_id,
                     access_key_secret,
                     breakpoint_info_path=breakpoint_info_path,
                     proxies=proxies)
    if provider == 'localfs':
        raise WorkspaceError(ErrorCode.CLOUD_STORAGE_NOT_CONFIGURED,
                             'provider=localfs 需要单独的 localfs provider 实现，当前版本未内置')
    # 兜底：保持历史行为（非 oss 配置降级为 Mock），但打印警告
    logger.warning(f'未知的 cloud.storage.provider={provider}，降级为 MockApi（不会真正传输数据）')
    from cloud_storage.provider.mock import MockApi
    return MockApi()


_worker_pools = None
_status_recorder = None


def get_status_recorder():
    global _status_recorder
    if _status_recorder is None:
        from cloud_storage.utils import status_recorder
        _status_recorder = status_recorder
    return _status_recorder


def get_worker_pools():
    global _worker_pools
    if _worker_pools is None:
        from cloud_storage.utils import WorkerPools
        _worker_pools = WorkerPools()
    return _worker_pools


def get_pod_id() -> str:
    return os.environ.get('POD_NAME', 'POD_NAME-0').split('-')[-1]


def get_instance_id() -> str:
    return f'{os.environ.get("POD_NAME", "POD_NAME-0")}-{os.getpid()}'


# ---------------------------------------------------------------------------
# hai-cli images（用户自定义镜像）—— 设计 docs/haiplatform/images/images-server-design.md §3.4 / §9.1
#
# 配置面：
#   [cloud.storage.service] image_path         镜像资产共享根（单点，见 conf.utils.get_image_root）
#   [image] enabled / enabled_users / enabled_groups          灰度开关（OPS-01）
#   [image] registry                           URL 第一段（默认 registry.high-flyer.cn，CMP-04）
#   [image] loader_backend                     register（P0 默认）/ task / registry（§5.3）
#   [image] name_regex                         镜像名白名单（SEC-03）
#   [image] load_helper_image                  initContainer 基础镜像（Q-6 / ADR-I4）
#   [image] data_local_path                    link 脚本可见的宿主目录（Q-5 / ADR-I4）
# ---------------------------------------------------------------------------

#: 合法的数据面后端（设计 §5.3）
IMAGE_LOADER_BACKENDS = ('register', 'task', 'registry')
_DEFAULT_IMAGE_REGISTRY = 'registry.high-flyer.cn'
_DEFAULT_LOAD_HELPER_IMAGE = 'docker.io/library/busybox:latest'
_DEFAULT_DATA_LOCAL_PATH = '/data_local'


def get_image_root() -> str:
    '''image_root = 镜像资产共享根（[cloud.storage.service].image_path，单点定义）。'''
    from conf.utils import get_image_root as _get_image_root
    return _get_image_root()


def get_image_registry() -> str:
    '''镜像 URL 第一段；默认保留 registry.high-flyer.cn，但新代码不依赖它可达（CMP-04）。'''
    value = str(cfg('image.registry', default=_DEFAULT_IMAGE_REGISTRY) or '').strip()
    return value or _DEFAULT_IMAGE_REGISTRY


def get_image_loader_backend() -> str:
    '''数据面后端：非法/缺失一律回退 register（P0 默认，ADR-I2）。'''
    value = str(cfg('image.loader_backend', default='register') or 'register').strip().lower()
    return value if value in IMAGE_LOADER_BACKENDS else 'register'


def get_image_load_helper_image() -> str:
    '''initContainer（load-image）的基础镜像；默认改为各节点已有的 busybox（Q-6 / I17②）。'''
    value = str(cfg('image.load_helper_image', default=_DEFAULT_LOAD_HELPER_IMAGE) or '').strip()
    return value or _DEFAULT_LOAD_HELPER_IMAGE


def get_image_data_local_path() -> str:
    '''link 脚本 initContainer 里 /data_local 对应的宿主路径（Q-5）。'''
    return str(cfg('image.data_local_path', default=_DEFAULT_DATA_LOCAL_PATH) or _DEFAULT_DATA_LOCAL_PATH)


def get_image_name_regex() -> str:
    '''
    镜像名白名单（FR-07 / SEC-03）。配置可收紧，但**不允许**放宽到含 '/'（HC-05）。
    '''
    from conf.utils import IMAGE_NAME_RE
    pattern = str(cfg('image.name_regex', default=IMAGE_NAME_RE.pattern) or '')
    if not pattern or '/' in pattern:
        pattern = IMAGE_NAME_RE.pattern
    return pattern


def image_feature_enabled() -> bool:
    '''总开关（默认 false：未显式开启时零行为变化，OPS-01）。'''
    return bool(cfg('image.enabled', default=False))


def check_image_enabled(user):
    '''
    灰度开关（OPS-01 / SEC-08）：enabled / enabled_users / enabled_groups。
    未开启时抛 FEATURE_DISABLED（HTTP 200 + success=0），由接入层统一处理。
    '''
    if not image_feature_enabled():
        raise WorkspaceError(ErrorCode.FEATURE_DISABLED, '镜像功能未开放')

    enabled_users = cfg('image.enabled_users', default=None) or []
    if enabled_users and user.user_name not in list(enabled_users):
        raise WorkspaceError(ErrorCode.FEATURE_DISABLED,
                             f'用户 {user.user_name} 不在镜像功能白名单内')

    enabled_groups = cfg('image.enabled_groups', default=None) or []
    if enabled_groups:
        try:
            allowed = user.in_any_group(list(enabled_groups))
        except Exception:
            allowed = False
        if not allowed:
            raise WorkspaceError(ErrorCode.FEATURE_DISABLED,
                                 f'用户 {user.user_name} 所在用户组不在镜像功能白名单内')


def image_self_check() -> dict:
    '''
    启动自检（CFG-04）：image_root 存在且可写、registry 已配置、loader_backend 合法、
    load_helper_image 非空。**失败只告警不阻断**（对齐 env_registry_self_check）。
    '''
    problems = []
    image_root = get_image_root()
    try:
        if not os.path.isdir(image_root):
            problems.append(f'image_root 不存在: {image_root}')
        elif not os.access(image_root, os.W_OK):
            problems.append(f'image_root 不可写: {image_root}')
    except Exception as e:  # pragma: no cover - 自检自身不得抛异常
        problems.append(f'image_root 检查异常: {e}')

    if not get_image_registry():
        problems.append('image.registry 为空')
    if str(cfg('image.loader_backend', default='register') or 'register').strip().lower() not in IMAGE_LOADER_BACKENDS:
        problems.append(f'image.loader_backend 取值非法: {cfg("image.loader_backend", default=None)}')
    if not str(cfg('image.load_helper_image', default=_DEFAULT_LOAD_HELPER_IMAGE) or '').strip():
        problems.append('image.load_helper_image 为空')

    result = {
        'ok': len(problems) == 0,
        'problems': problems,
        'image_root': image_root,
        'enabled': image_feature_enabled(),
        'loader_backend': get_image_loader_backend(),
        'registry': get_image_registry(),
    }
    if problems:
        logger.warning('image path check: WARN - ' + '; '.join(problems))
    else:
        logger.info(f'image path check: OK (image_root={image_root}, '
                    f'loader_backend={result["loader_backend"]}, enabled={result["enabled"]})')
    return result
