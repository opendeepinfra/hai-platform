'''
workspace（/ugc/*）领域层统一错误。

约定（需求 §4 统一约定 / CON-3）：
- 业务失败一律返回 {'success': 0, 'msg': ..., 'code': ...}
- HTTP 状态码由 WorkspaceError.http_status 决定，默认 200
- 任何响应体都必须带 success 字段，客户端会先断言 'success' in result
'''


class ErrorCode:
    INVALID_PARAM = 'INVALID_PARAM'
    INVALID_BODY = 'INVALID_BODY'
    UNAUTHORIZED = 'UNAUTHORIZED'
    FORBIDDEN = 'FORBIDDEN'
    PATH_ESCAPE = 'PATH_ESCAPE'
    QUOTA_EXCEEDED = 'QUOTA_EXCEEDED'
    CLOUD_STORAGE_NOT_CONFIGURED = 'CLOUD_STORAGE_NOT_CONFIGURED'
    FEATURE_DISABLED = 'FEATURE_DISABLED'
    NOT_FOUND_INDEX = 'NOT_FOUND_INDEX'
    TOO_MANY_FILES = 'TOO_MANY_FILES'
    PAYLOAD_TOO_LARGE = 'PAYLOAD_TOO_LARGE'
    CLIENT_RETRY = 'CLIENT_RETRY'
    INTERNAL_ERROR = 'INTERNAL_ERROR'
    # haienv（`hai-cli env`）新增错误码 —— 设计 docs/haiplatform/env/env-server-design.md §4
    ENV_ALREADY_EXISTS = 'ENV_ALREADY_EXISTS'
    ENV_REGISTRY_NOT_WRITABLE = 'ENV_REGISTRY_NOT_WRITABLE'
    ENV_REGISTRY_WRITE_FAILED = 'ENV_REGISTRY_WRITE_FAILED'
    # 注册表**读**失败（fail-closed）：读不出来 ≠ 没注册过，否则会产生重复环境（N3）
    ENV_REGISTRY_READ_FAILED = 'ENV_REGISTRY_READ_FAILED'
    ENV_PATH_MISMATCH = 'ENV_PATH_MISMATCH'


class WorkspaceError(Exception):
    '''
    领域层唯一的业务异常。接入层把它改写成带 success 的响应。
    '''

    def __init__(self, code: str, msg: str, http_status: int = 200):
        self.code = code
        self.msg = msg
        self.http_status = http_status
        super().__init__(msg)

    def to_response(self) -> dict:
        return {'success': 0, 'code': self.code, 'msg': self.msg}
