'''
hai-cli images（用户自定义镜像）服务端接入层 —— 设计 docs/haiplatform/images/images-server-design.md §4 / §5.5。

  API-15 POST /ugc/user/train_image/load           加载登记
  API-16 POST /ugc/user/train_image/update_status  状态回报（数据面执行方）
  API-17 POST /ugc/user/train_image/list           列表（路由已存在，走 api/query/optimized/resource.py）
  API-18 POST /ugc/user/train_image/delete         删除

说明（ADR-I1 / ADR-I7 / HC-07）：这些函数放在 default.py（而不是 implement.py）里，
部署私有的 api/resource/image/custom.py 可以同名覆盖。
业务逻辑全部在 server_model/user_impl/user_image/（领域层，HC-07）。
'''

from __future__ import annotations

import time

from fastapi import Depends, Request

from logm import logger

from api.depends import get_ugc_user
from cloud_storage.service import WorkspaceError, parse_json_body
from image_metrics import (image_load_total, image_load_duration_seconds,
                           image_list_rows, image_update_status_total)


async def _params(request: Request) -> dict:
    ''' 兼容 query string 与 text/plain 内 JSON（§4.0 统一约定）。 '''
    merged = dict(request.query_params)
    body = await parse_json_body(request)
    if body:
        merged.update(body)
    return merged


def _as_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'y', 'on')


# --------------------------------------------------------------------------- API-15

async def hfai_image_load(request: Request, user=Depends(get_ugc_user)):
    '''
    加载登记（FR-03 / FR-06 / FR-07 / SEC-01 / SEC-02）。

    出参：{'success': 1, 'msg': '镜像已登记，状态：loaded', 'image': ..., 'image_tar': ...,
           'status': ..., 'task_id': ...}
    '''
    params = await _params(request)
    started = time.time()
    try:
        result = await user.image.async_load(image_tar=params.get('image_tar'),
                                             image=params.get('image'),
                                             force=_as_bool(params.get('force')))
    except WorkspaceError as e:
        image_load_total.labels(result='fail', code=e.code).inc()
        logger.warning(f'[IMAGE] load 失败 user={user.user_name} '
                       f'image_tar={params.get("image_tar")} code={e.code} '
                       f'elapsed_ms={int((time.time() - started) * 1000)} msg={e.msg}')
        raise  # api/app.py 已注册 WorkspaceError 全局处理器 → {'success':0,'code','msg'}
    except Exception as e:
        image_load_total.labels(result='fail', code='INTERNAL_ERROR').inc()
        logger.error(f'[IMAGE] load 异常 user={user.user_name} '
                     f'image_tar={params.get("image_tar")} '
                     f'elapsed_ms={int((time.time() - started) * 1000)}: {e}')
        raise WorkspaceError('INTERNAL_ERROR', f'加载镜像失败: {e}')
    image_load_total.labels(result='ok', code='').inc()
    image_load_duration_seconds.labels(backend=result.get('backend', 'register')).observe(
        time.time() - started)
    logger.info(f'[IMAGE] load 成功 user={user.user_name} image_tar={result.get("image_tar")} '
                f'image={result.get("image")} status={result.get("status")} '
                f'task_id={result.get("task_id")} backend={result.get("backend")} '
                f'reused={result.get("reused")} cost_ms={int((time.time() - started) * 1000)}')
    return {'success': 1, 'msg': f'镜像已登记，状态：{result["status"]}', **result}


# --------------------------------------------------------------------------- API-16

async def hfai_image_update_status(request: Request, user=Depends(get_ugc_user)):
    ''' 状态回报（FR-10 / SEC-04）；仅 task/registry 后端使用。 '''
    params = await _params(request)
    started = time.time()
    try:
        result = await user.image.async_report_image_status(
            image_tar=params.get('image_tar'), status=params.get('status'),
            path=params.get('path'), message=params.get('message') or '',
            task_id=params.get('task_id'))
    except WorkspaceError as e:
        image_update_status_total.labels(result='fail', code=e.code).inc()
        logger.warning(f'[IMAGE] update_status 失败 user={user.user_name} '
                       f'image_tar={params.get("image_tar")} status={params.get("status")} '
                       f'code={e.code} msg={e.msg}')
        raise
    except Exception as e:
        image_update_status_total.labels(result='fail', code='INTERNAL_ERROR').inc()
        logger.error(f'[IMAGE] update_status 异常 user={user.user_name} '
                     f'image_tar={params.get("image_tar")}: {e}')
        raise WorkspaceError('INTERNAL_ERROR', f'状态回报失败: {e}')
    image_update_status_total.labels(result='ok', code='').inc()
    logger.info(f'[IMAGE] update_status 成功 user={user.user_name} '
                f'image_tar={result.get("image_tar")} from_status={result.get("from_status")} '
                f'status={result.get("status")} cost_ms={int((time.time() - started) * 1000)}')
    return {'success': 1, 'msg': '状态已更新', **result}


# --------------------------------------------------------------------------- 列表（P2 独立端点预留）

async def hfai_image_list(request: Request, user=Depends(get_ugc_user)):
    ''' 保留桩名；列表主路径仍是既有 API-17（本函数为 P2 独立列表端点预留）。 '''
    rows = await user.image.async_get_user_images()
    try:
        image_list_rows.labels(shared_group=user.shared_group).set(len(rows))
    except Exception:
        pass
    return {'success': 1, 'data': rows}


# --------------------------------------------------------------------------- API-18

async def hfai_image_delete(request: Request, user=Depends(get_ugc_user)):
    ''' 删除（FR-05 / SEC-02 / SEC-05）；组校验在领域层，接入层不做业务判断。 '''
    params = await _params(request)
    try:
        result = await user.image.async_delete(image=params.get('image'))
    except WorkspaceError as e:
        logger.warning(f'[IMAGE] delete 失败 user={user.user_name} '
                       f'image={params.get("image")} code={e.code} msg={e.msg}')
        raise
    except Exception as e:
        logger.error(f'[IMAGE] delete 异常 user={user.user_name} image={params.get("image")}: {e}')
        raise WorkspaceError('INTERNAL_ERROR', f'删除镜像失败: {e}')
    logger.info(f'[IMAGE] delete 成功 user={user.user_name} image={params.get("image")} '
                f'deleted={result.get("deleted")}')
    return {'success': 1, 'msg': f'已删除 {result["deleted"]} 个镜像记录', **result}


# --------------------------------------------------------------------------- OPS-01 启动自检

async def startup_image_check():
    '''
    ugc-server 启动时执行镜像路径自检（CFG-04）：失败只告警，不影响服务启动。
    '''
    try:
        from cloud_storage.service.context import image_self_check
        image_self_check()
    except Exception as e:
        logger.error(f'image 路径自检异常（不影响服务启动）: {e}')
