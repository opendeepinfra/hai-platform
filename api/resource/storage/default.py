'''
hai-cli env（haienv）服务端接入层 —— 设计 docs/haiplatform/env/env-server-design.md §4 / §5.3。

  API-11 POST /ugc/update_cluster_venv     预检：名称校验 + 路径推导 + 写权限探测
  API-13 POST /ugc/register_cluster_venv   注册：写集群侧 {user_env_dir}/venv.db

说明（ADR-E7 / HC-08）：这些函数放在 default.py（而不是 implement.py）里，
部署私有的 api/resource/storage/custom.py 可以同名覆盖。
业务逻辑全部在 cloud_storage/service/env_registry.py（领域层，HC-06）。
'''

from __future__ import annotations
from typing import TYPE_CHECKING

from fastapi import Depends, Request

from logm import logger

from api.depends import get_ugc_user
from cloud_storage.metrics import env_push_requests_total, env_register_duration_seconds
from cloud_storage.service import (WorkspaceError, parse_json_body,
                                   derive_env_path, register_env, env_registry_self_check)

if TYPE_CHECKING:
    from .implement import MountPoint


async def _require_config():
    '''配置缺失时返回 CLOUD_STORAGE_NOT_CONFIGURED，而不是抛 500。'''
    from cloud_storage.service.context import ensure_cloud_storage_configured
    ensure_cloud_storage_configured()


def _as_str_list(value) -> list:
    '''
    归一化 extra_search_dir / extra_search_bin_dir / extra_environment。
    列表原样保留（TC-A18：不得字符串化）；单值字符串包成单元素列表。
    '''
    if value is None or value == '':
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    if isinstance(value, str):
        return [value]
    return [str(value)]


# --------------------------------------------------------------------------- API-11

async def update_cluster_venv(request: Request, user=Depends(get_ugc_user)):
    '''
    预检 + 路径推导（FR-03 / FR-07 / FR-08 / SEC-03）。

    出参：{'success': 1, 'path': '<集群 env 目录绝对路径>', 'exists': <bool>}
    '''
    await _require_config()
    params = request.query_params
    venv_name = params.get('venv_name') or ''
    py = params.get('py') or ''
    extend = params.get('extend')
    try:
        result = await derive_env_path(user, venv_name, py, extend)
    except WorkspaceError as e:
        env_push_requests_total.labels(api='update_cluster_venv', result='fail', code=e.code).inc()
        logger.warning(f'[ENV] update_cluster_venv 失败 user={user.user_name} env={venv_name} '
                       f'code={e.code} msg={e.msg}')
        raise
    except Exception as e:
        env_push_requests_total.labels(api='update_cluster_venv', result='fail',
                                       code='INTERNAL_ERROR').inc()
        logger.error(f'[ENV] update_cluster_venv 异常 user={user.user_name} env={venv_name}: {e}')
        raise WorkspaceError('INTERNAL_ERROR', f'预检失败: {e}')
    env_push_requests_total.labels(api='update_cluster_venv', result='ok', code='OK').inc()
    result['success'] = 1
    logger.debug(f'[ENV] update_cluster_venv user={user.user_name} env={venv_name} '
                 f'path={result.get("path")} exists={result.get("exists")}')
    return result


# --------------------------------------------------------------------------- API-13

async def register_cluster_venv(request: Request, user=Depends(get_ugc_user)):
    '''
    把已上传成功的 env 写入集群侧注册表（FR-04 / FR-09 / SEC-02）。

    出参：{'success': 1, 'registered': true, 'path': ..., 'db': ...}
    '''
    import time as _time

    await _require_config()
    body = await parse_json_body(request)
    venv_name = body.get('venv_name')
    path = body.get('path')
    py = body.get('py')
    try:
        started = _time.time()
        result = await register_env(user, venv_name, path, py,
                                    _as_str_list(body.get('extra_search_dir')),
                                    _as_str_list(body.get('extra_search_bin_dir')),
                                    _as_str_list(body.get('extra_environment')))
        env_register_duration_seconds.labels(result='ok').observe(_time.time() - started)
    except WorkspaceError as e:
        env_push_requests_total.labels(api='register_cluster_venv', result='fail', code=e.code).inc()
        logger.warning(f'[ENV] register_cluster_venv 失败 user={user.user_name} env={venv_name} '
                       f'path={path} code={e.code} msg={e.msg}')
        raise
    except Exception as e:
        env_push_requests_total.labels(api='register_cluster_venv', result='fail',
                                       code='INTERNAL_ERROR').inc()
        logger.error(f'[ENV] register_cluster_venv 异常 user={user.user_name} env={venv_name} '
                     f'path={path}: {e}')
        raise WorkspaceError('ENV_REGISTRY_WRITE_FAILED',
                             f'写入集群侧注册表失败: {path}，上传的文件已保留，可直接重试 `env push` 补登记')
    env_push_requests_total.labels(api='register_cluster_venv', result='ok', code='OK').inc()
    result['success'] = 1
    return result


# --------------------------------------------------------------------------- OPS-01 启动自检

async def startup_env_check():
    '''
    ugc-server 启动时校验 env 路径三方同源（OPS-01）。
    任何异常都不得影响服务启动。
    '''
    try:
        env_registry_self_check()
    except Exception as e:
        logger.error(f'env 路径自检异常（不影响服务启动）: {e}')


async def get_user_weka_usage():
    return {'success': 1, 'result': {}, 'msg': 'not implemented'}


async def get_3fs_monitor_dir_api():
    return []


async def get_external_user_storage_usage():
    return {'success': 1, 'result': {}, 'msg': 'not implemented'}


async def security_check(mount_point: MountPoint):
    """ 添加挂载点时的安全校验 """
    pass
