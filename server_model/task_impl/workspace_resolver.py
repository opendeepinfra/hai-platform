"""
FR-15：把任务 schema 中的 `oss://<shared_group>/<user_name>/workspaces/<name>`
解析为集群共享盘上的真实路径。

本模块是**纯函数**模块（设计 §3.3 / ADR-11 / ADR-12）：

- 不做 Redis / OSS / 网络调用，不写文件系统（只允许 `os.path.isdir` 这类只读检查）；
- 模块顶层只依赖 `conf`，**不导入** `cloud_storage.*`（该包 `__init__` 会拉起
  FastAPI app / provider / Redis 连接）；`check_is_subpath` 在真正需要防穿越校验时
  才惰性导入，且导入失败一律 fail-closed（宁可不解析，也不能跳过校验）；
- 非 URI 的 workspace（集群本地路径）**原样返回**，行为与改动前完全一致（回归保护 K-05）。

路径约定见设计 §10.1：`<workspace_path>/<shared_group>/<user_name>/workspaces/<name>`。
"""

import os
import re

from conf import CONF


URI_RE = re.compile(r'^(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*)://(?P<remote>.+)$')


class TaskSchemaError(Exception):
    """
    任务 schema 非法（workspace 无法解析）。

    说明：设计文档 §10.1 引用的 `TaskSchemaError` 在仓库中原本并不存在
    （`api/task_schema.py` 只有 pydantic 模型 `TaskSchema` / `TaskSpec`），
    因此在解析器模块内定义，作为 workspace 解析的唯一对外异常类型。
    """


def _load_check_is_subpath():
    """
    惰性复用 `cloud_storage.utils.check_is_subpath`（不重新实现安全校验）。

    之所以不在模块顶层 import：`cloud_storage.utils` 属于云存储传输层，
    launcher / manager 进程不应在导入期被其污染（设计 §3.3 导入纪律）。
    """
    try:
        from cloud_storage.utils import check_is_subpath
    except Exception as e:  # pragma: no cover - 取决于部署环境
        raise TaskSchemaError(
            '服务端无法加载路径校验工具 cloud_storage.utils.check_is_subpath，'
            '拒绝解析 workspace URI') from e
    return check_is_subpath


def workspace_uri_to_cluster_path(workspace: str):
    """
    **纯字符串**解析：把 `<provider>://<group>/<user>/workspaces/<name>` 映射为
    `<workspace_path>/<group>/<user>/workspaces/<name>`，不做任何归属校验（调用方负责）。

    为什么单独提供这个函数：`add_runtime_mounts` 在 `ITaskImpl.__init__` 里被调用，
    那个时机**不能访问 `task_impl.user`**（会递归构造 TaskImpl 导致栈溢出），
    所以挂载阶段只能用这个不依赖 user 的版本。

    :return: 集群绝对路径；不是本 provider 的 URI / 形状非法 / 缺配置时返回 None
    """
    if not workspace:
        return None
    m = URI_RE.match(workspace)
    if m is None:
        return None

    provider = CONF.try_get('cloud.storage.provider', default=None)
    root = CONF.try_get('cloud.storage.service.workspace_path', default=None)
    if not provider or not root:
        return None
    if m.group('scheme').lower() != str(provider).lower():
        return None

    remote = m.group('remote')
    parts = remote.split('/')
    # 必须是 <group>/<user>/workspaces/<name> 四段，且每段都是普通名字（拒绝 . / .. / 空段）
    if len(parts) != 4 or parts[2] != 'workspaces':
        return None
    if any(p in ('', '.', '..') for p in parts):
        return None

    root = str(root).rstrip('/') or '/'
    return f'{root}/{remote}'


def resolve_workspace_path(user, workspace: str, *, check_exists: bool = False) -> str:
    """
    把 `oss://<shared_group>/<user_name>/workspaces/<name>` 解析为集群绝对路径。

    :param user: 任务提交人（需要 `shared_group` / `user_name` 两个属性）
    :param workspace: 任务 schema 的 `spec.workspace`
    :param check_exists: 是否校验集群路径已存在。**只允许在能看见共享盘的进程里打开**：
        目前仅提交接口 `api/operation/implement.py`（hai-platform pod）使用；
        `parse_code_cmd`（task manager pod）与 `add_runtime_mounts` 都必须保持 False——
        manager pod 不挂载 `cloud.storage.service.workspace_path`，打开它会把已同步的
        工作区误判为「尚未同步」（实测任务 7 `ugc_e2e3`）
    :return: 集群绝对路径（无尾部斜杠）；非 URI 原样返回
    """
    # 1) 空 / None：原样返回（不改动旧行为）
    if not workspace:
        return workspace

    m = URI_RE.match(workspace)
    # 2) 不是 URI：集群本地路径，透明透传（回归要求 K-05；无 cloud.storage 配置也必须能走这里）
    if m is None:
        return workspace

    scheme, remote = m.group('scheme').lower(), m.group('remote')

    provider = CONF.try_get('cloud.storage.provider', default=None)
    root = CONF.try_get('cloud.storage.service.workspace_path', default=None)
    if not provider or not root:
        # URI 分支缺配置：给出可读异常，而不是 AttributeError / 拼出错误路径
        raise TaskSchemaError(
            '服务端未配置 cloud.storage（cloud.storage.provider / '
            'cloud.storage.service.workspace_path），无法解析 workspace URI')

    # 3) scheme 必须与 provider 一致（大小写不敏感）
    if scheme != str(provider).lower():
        raise TaskSchemaError(f'不支持的 workspace scheme: {scheme}')

    # 4) 归属校验：只能引用自己的工作区（防越权）
    shared_group = getattr(user, 'shared_group', None)
    user_name = getattr(user, 'user_name', None)
    if not shared_group or not user_name:
        raise TaskSchemaError(f'无法确定当前用户的 shared_group/user_name，workspace 不属于当前用户: {remote}')

    prefix = f'{shared_group}/{user_name}/workspaces/'
    if not remote.startswith(prefix):
        raise TaskSchemaError(f'workspace 不属于当前用户: {remote}')

    name = remote[len(prefix):]
    # 名称不能为空、不能含 '/'（'..'/'.' 也一并拒绝，避免拼出上跳路径）
    if not name or '/' in name or name in ('.', '..'):
        raise TaskSchemaError(f'workspace 名称非法: {name}')

    # 5) 集群真实路径 + 防目录穿越（group/user 亦可能被污染，故统一走 check_is_subpath）
    root = str(root).rstrip('/') or '/'
    path = f'{root}/{remote}'
    check_is_subpath = _load_check_is_subpath()
    try:
        check_is_subpath(root, path)
    except Exception as e:
        raise TaskSchemaError(f'workspace 路径非法: {remote}') from e

    # 6) 可选的存在性校验（只在提交/构建阶段做，挂载阶段不做）
    if check_exists and not os.path.isdir(path):
        raise TaskSchemaError(
            f'workspace 尚未同步到集群，请先执行 hai-cli workspace push'
            f'（workspace 名称: {name}）')

    return path
