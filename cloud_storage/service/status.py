'''
同步任务的「过程态」查询（Redis）—— 对应 API-06 / API-08（FR-06 / FR-12 / FR-20）。

返回体必须能被客户端 `_do_poll_status` 直接消费：
  - running: msg 必须是可 int() 的数字（已传字节数）
  - finished: msg 为 ''
  - failed: msg 为可读原因
  - 不存在的 index: NOT_FOUND_INDEX（客户端会看到 success=0）
  - index 归属校验：非本人任务返回 FORBIDDEN（SEC-04）
'''

from logm import logger

from cloud_storage.utils import status_recorder, status_key, SyncPhase, PROVIDER

from .context import get_status_ttl_finished
from .errors import WorkspaceError, ErrorCode


def owner_key(index: str, is_upload: bool) -> str:
    return f'{PROVIDER}:{"sync_from_cluster" if is_upload else "sync_to_cluster"}:{index}:owner'


async def set_owner(index: str, is_upload: bool, user):
    await status_recorder.a_set(owner_key(index, is_upload), user.user_name, 604800)


async def check_owner(index: str, is_upload: bool, user):
    '''
    SEC-04：用他人 index 访问时返回 403。
    owner 键不存在时（历史任务 / 已过期）不做拦截，避免误伤。
    '''
    owner = await status_recorder.a_get(owner_key(index, is_upload))
    if owner is not None and owner != user.user_name:
        raise WorkspaceError(ErrorCode.FORBIDDEN,
                             f'index {index[:10]} 不属于当前用户', http_status=403)


async def get_transfer_status(user, index: str, is_upload: bool) -> dict:
    if not index:
        raise WorkspaceError(ErrorCode.INVALID_PARAM, '必须指定 index')

    phase = await status_recorder.a_get(status_key(index, 'status', is_upload))
    logger.debug(f'---- transfer status: {index[:10]} {phase} is_upload={is_upload} ----')
    if phase is None:
        raise WorkspaceError(ErrorCode.NOT_FOUND_INDEX, '不存在的index', http_status=400)

    await check_owner(index, is_upload, user)

    if phase == SyncPhase.RUNNING:
        progress = await status_recorder.a_get_hvalues(status_key(index, 'progress', is_upload))
        total = None
        if 'dataset_total_size' in progress:
            total = int(progress['dataset_total_size'])
            msg = sum(v for k, v in progress.items() if k != 'dataset_total_size')
        else:
            msg = sum(progress.values())
        ret = {'status': SyncPhase.RUNNING, 'msg': msg}
        if total is not None:
            ret['total'] = total
        return ret
    if phase == SyncPhase.FINISHED:
        return {'status': SyncPhase.FINISHED, 'msg': ''}
    if phase == SyncPhase.INIT:
        return {'status': SyncPhase.INIT, 'msg': ''}
    # 其余情况：phase 里存的是失败原因（见 wait_* 的写法）
    return {'status': SyncPhase.FAILED, 'msg': phase}


def finalize_status(index: str, is_upload: bool, phase_or_msg):
    '''
    写终态：TTL 必须 >= 客户端 --sync_timeout（默认 1800），否则客户端会看到 NOT_FOUND_INDEX（ADR-6）。

    注意：这里必须是**同步**函数 —— 它同时被 `wait_to_cluster` / `wait_from_cluster`
    这两个跑在后台线程里的函数调用（线程里没有事件循环，写成 async 会导致
    「coroutine was never awaited」，终态永远写不进去，状态一直悬挂在 running）。
    '''
    status_recorder.delete(status_key(index, 'progress', is_upload))
    status_recorder.set(status_key(index, 'status', is_upload), phase_or_msg,
                        expires=get_status_ttl_finished())
