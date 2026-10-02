'''
客户端兼容层（需求 FR-14 / COMP-01 / COMP-02）。

必须吸收三种「客户端实际形态」：
1. 枚举串：file_type=FileType.WORKSPACE / direction=SyncDirection.PUSH / status=SyncStatus.STAGE1_RUNNING
2. Content-Type: text/plain 的 JSON Body（客户端用 aiohttp data=<json str> 发送）
3. Body 外壳：{"file_list": {"files": [...]}} / {"file_infos": {"files": [...]}}
'''

import json
import re

from conf import CONF

from .errors import WorkspaceError, ErrorCode

# FileType.WORKSPACE -> 去掉所有 "Xxx." 前缀
_ENUM_PREFIX = re.compile(r'^(?:[A-Za-z_]\w*\.)+')


def legacy_compat_enabled() -> bool:
    return bool(CONF.try_get('cloud.storage.service.legacy_param_compat', default=True))


def normalize_enum(raw, enum_cls, field: str, default=None, enabled: bool = None):
    '''
    把客户端的枚举串归一化成 enum 成员。

    - None/'' -> default（default 也是 None 时返回 None）
    - 'FileType.WORKSPACE' / 'filetype.workspace' / 'WORKSPACE' / 'workspace' -> FileType.WORKSPACE
    - 非法值 -> WorkspaceError(INVALID_PARAM)，消息里带上合法取值列表
    '''
    if enabled is None:
        enabled = legacy_compat_enabled()

    if raw is None or (isinstance(raw, str) and raw.strip() == ''):
        return default

    if isinstance(raw, enum_cls):
        return raw

    text = str(raw).strip()
    if enabled:
        text = _ENUM_PREFIX.sub('', text)

    candidate = text.strip().lower()
    for member in enum_cls:
        if member.value.lower() == candidate:
            return member

    valid = ', '.join(m.value for m in enum_cls)
    raise WorkspaceError(ErrorCode.INVALID_PARAM,
                         f'{field} 取值非法: {raw}，合法取值为 [{valid}]')


def normalize_bool(raw, field: str, default: bool = False) -> bool:
    if raw is None or (isinstance(raw, str) and raw.strip() == ''):
        return default
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in ('1', 'true', 'yes', 'y', 'on'):
        return True
    if text in ('0', 'false', 'no', 'n', 'off'):
        return False
    raise WorkspaceError(ErrorCode.INVALID_PARAM, f'{field} 取值非法: {raw}，应为 true/false')


def unwrap_files(body, wrapper: str = None):
    '''
    兼容 Body 外壳，返回 files 列表（list[dict] 或 list[str]，原样返回元素）。

    支持：
      {"file_list": {"files": [...]}, ...}   # 客户端实际形态（wrapper='file_list'）
      {"file_infos": {"files": [...]}}       # 客户端实际形态（wrapper='file_infos'）
      {"files": [...]}                       # 规范形态（裸体）
      {} / None                              # 空体 -> []
    '''
    if not body:
        return []
    if not isinstance(body, dict):
        raise WorkspaceError(ErrorCode.INVALID_BODY, '请求体必须是 JSON 对象')

    payload = body
    if wrapper and isinstance(body.get(wrapper), dict):
        payload = body[wrapper]
    elif wrapper and isinstance(body.get(wrapper), list):
        payload = body[wrapper]

    if isinstance(payload, dict):
        files = payload.get('files', [])
    elif isinstance(payload, list):
        files = payload
    else:
        raise WorkspaceError(ErrorCode.INVALID_BODY, '请求体缺少 files 字段')

    if files is None:
        return []
    if not isinstance(files, list):
        raise WorkspaceError(ErrorCode.INVALID_BODY, 'files 字段必须是列表')
    return files


def parse_json_body_sync(raw_body: bytes, wrapper: str = None) -> dict:
    '''
    手工解析原始 Body（忽略 Content-Type）——CON-4。
    空 body -> {}；非法 JSON / 非对象 -> INVALID_BODY。
    '''
    if raw_body is None:
        return {}
    if isinstance(raw_body, bytes):
        text = raw_body.decode('utf-8', errors='replace').strip()
    else:
        text = str(raw_body).strip()
    if text == '':
        return {}
    try:
        body = json.loads(text)
    except Exception:
        raise WorkspaceError(ErrorCode.INVALID_BODY, '请求体不是合法 JSON')
    if not isinstance(body, dict):
        raise WorkspaceError(ErrorCode.INVALID_BODY, '请求体必须是 JSON 对象')
    return body


async def parse_json_body(request, wrapper: str = None) -> dict:
    return parse_json_body_sync(await request.body(), wrapper)
