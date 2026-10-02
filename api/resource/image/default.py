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

import os
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


# --------------------------------------------------------------------------- API-19（S8-5 上传预检）

async def hfai_image_push_precheck(request: Request, user=Depends(get_ugc_user)):
    '''
    上传预检（API-19，FR-18 / Q-11）：**只读**接口，用于落点协商 + 幂等判定 + 容量上限。

    出参：{'success': 1, 'name', 'image', 'file', 'image_tar', 'cloud_path', 'cluster_path',
           'exists', 'registered', 'max_tar_bytes', 'msg'}

    - `cloud_path` 必须原样作为客户端 push 的远端 key 前缀（stage1 写对象存储的 key 前缀
      与服务端 stage2 读取时的 `cloud_base_path` 必须逐字节一致，否则 stage2 找不到对象）；
    - `cluster_path` 是共享盘落点目录，最终文件 = `{cluster_path}/{file}`；
    - `exists` / `registered` 供客户端跳过重复上传（`--force` 可覆盖）；
    - `file_size`（或 `size`）声明本地 tar 字节数时，超限在此**快速失败**（OPS-07 / TC-UP-10）。
    '''
    params = await _params(request)

    from cloud_storage.service.context import (check_image_upload_enabled, check_image_max_tar_bytes,
                                               get_image_max_tar_bytes, get_provider_name)
    from cloud_storage.service.status import set_image_precheck
    from cloud_storage.utils import get_base_path, check_is_subpath
    from conf.utils import (FileType, FilePrivacy, derive_image_name, is_valid_image_name)

    try:
        check_image_upload_enabled(user)

        filename = str(params.get('file') or '').strip()
        if (not filename or filename in ('.', '..') or '/' in filename or '\\' in filename
                or os.path.basename(filename) != filename):
            raise WorkspaceError('INVALID_PARAM', f'非法的镜像文件名: {params.get("file")}')

        image = str(params.get('image') or '').strip() or derive_image_name(filename)
        if not is_valid_image_name(image):
            raise WorkspaceError('INVALID_PARAM', f'非法的镜像名: {image}')

        # 落点目录名 = 镜像条目名（Q-9「目录 + tar」/ Q-10）
        name = image

        check_image_max_tar_bytes(params.get('file_size') or params.get('size'))

        try:
            cluster_base_path, cloud_base_path = get_base_path(
                user.user_name, user.shared_group, name, FileType.IMAGE, FilePrivacy.GROUP_SHARED)
        except Exception as e:
            raise WorkspaceError('INVALID_PARAM', str(e))

        image_tar = os.path.join(cluster_base_path, filename)
        check_is_subpath(cluster_base_path, image_tar)   # SEC-09 / HC-13 兜底

        exists = os.path.isfile(image_tar)
        registered = False
        try:
            rows = await user.image.async_get_user_images()
            registered = any(r.get('image') == image and r.get('status') == 'loaded'
                             for r in (rows or []))
        except Exception as e:
            # 预检是只读的，查不到注册状态不应阻断上传（按未注册处理，客户端会继续传）
            logger.warning(f'[IMAGE] push_precheck 查询注册状态失败（按未注册处理）: {e}')

        # `upload_require_precheck=true` 时的凭证（默认关闭，见 cloud_storage/service/status.py）
        await set_image_precheck(user, name)

        msg = ('该 tar 已在集群且已登记，可直接提交任务'
               if (exists and registered) else
               ('该 tar 已在集群，上传可跳过' if exists else '需要上传'))
        # provider 必须回给客户端：客户端上传时要按**同一个 provider** 去要 STS 凭证。
        # 103 上服务端配置的是 s3/rustfs，客户端默认值 oss 会拿到
        # 「get_sts_token returns non oss data」而直接失败（实测踩过，D11）。
        provider_name = get_provider_name()
        logger.info(f'[IMAGE] push_precheck user={user.user_name} name={name} image={image} '
                    f'file={filename} exists={exists} registered={registered} '
                    f'provider={provider_name} cluster={cluster_base_path} cloud={cloud_base_path}')
        return {'success': 1, 'name': name, 'image': image, 'file': filename,
                'image_tar': image_tar, 'cloud_path': cloud_base_path,
                'cluster_path': cluster_base_path, 'exists': exists,
                'registered': registered, 'provider': provider_name,
                'max_tar_bytes': get_image_max_tar_bytes(), 'msg': msg}
    except WorkspaceError as e:
        logger.warning(f'[IMAGE] push_precheck 失败 user={user.user_name} '
                       f'file={params.get("file")} code={e.code} msg={e.msg}')
        raise


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
