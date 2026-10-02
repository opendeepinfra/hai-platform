'''
cloud_storage 领域层。

约束（设计 §3.3 / ADR-11 / ADR-12）：
- **禁止**在本包内 import fastapi / 注册路由 / 注册 on_event
- `CONF.cloud.storage.*`、provider、Redis、进程池一律惰性获取
- 入参是已解析的 User 与归一化后的枚举；返回纯 dict（不含 success），失败抛 WorkspaceError
'''

from .errors import WorkspaceError, ErrorCode
from .compat import normalize_enum, normalize_bool, parse_json_body, parse_json_body_sync, unwrap_files
from .context import (check_cloud_storage_config, check_feature_enabled, cfg,
                      get_workspace_path, get_provider_name, log_config_self_check,
                      get_env_root, get_user_env_dir, get_env_registry_path,
                      check_env_push_enabled)
from .sts import issue_sts_token
from .cluster_files import list_cluster_files_page
from .status import get_transfer_status
from .sync_to_cluster import submit_to_cluster
from .sync_from_cluster import submit_from_cluster
from .delete import delete_paths
from .env_registry import (validate_env_name, derive_env_path, register_env,
                           env_registry_self_check)

__all__ = [
    'WorkspaceError', 'ErrorCode',
    'normalize_enum', 'normalize_bool', 'parse_json_body', 'parse_json_body_sync', 'unwrap_files',
    'check_cloud_storage_config', 'check_feature_enabled', 'cfg',
    'get_workspace_path', 'get_provider_name', 'log_config_self_check',
    'get_env_root', 'get_user_env_dir', 'get_env_registry_path', 'check_env_push_enabled',
    'issue_sts_token', 'list_cluster_files_page', 'get_transfer_status',
    'submit_to_cluster', 'submit_from_cluster', 'delete_paths',
    'validate_env_name', 'derive_env_path', 'register_env', 'env_registry_self_check',
]
