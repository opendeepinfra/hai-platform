from base_model.base_task import ITaskImpl
from conf import CONF
from ..workspace_resolver import resolve_workspace_path


WORKSPACE_MOUNT_NAME = 'workspace-path'


def _schema_workspace(task_impl: ITaskImpl):
    """
    直接读 `task.schema` 的 `spec.workspace`（不做 pydantic 校验）：
    本函数在 `SingleTaskImpl.__init__`（single_task_impl.py:32）被调用，
    早于任务创建，任何多余的失败都可能让任务创建流程崩溃。
    """
    schema = getattr(task_impl.task, 'schema', None) or {}
    spec = schema.get('spec') or {}
    return spec.get('workspace') or ''


def _mounted_paths(task_impl: ITaskImpl):
    """
    已经存在的挂载 mount_path 集合：`_runtime_mounts`（本次已追加的）+ storage 表（既有挂载）。

    去重是必要的：k8s 的 pod spec 不允许重复的 mountPath。
    `personal_storage()` 会把 storage_df（`cached_property`）取一次，之后
    `build_schemas`（single_task_impl.py:283）再次调用即为缓存命中，
    因此单任务的 SQL 次数不变（只是提前到 __init__）；任何失败都忽略，不影响任务创建。
    """
    paths = {m.get('mount_path') for m in (getattr(task_impl, '_runtime_mounts', None) or [])}
    try:
        paths |= {m.get('mount_path') for m in task_impl.user.storage.personal_storage(task_impl.task)}
    except Exception:
        pass
    return paths


def _add_workspace_mount(task_impl: ITaskImpl):
    workspace = _schema_workspace(task_impl)
    if not workspace:
        return

    # 挂载发生在任务创建早期（不校验存在性、不做任何 I/O）
    cluster_path = resolve_workspace_path(task_impl.task.user, workspace, check_exists=False)

    workspace_path = CONF.try_get('cloud.storage.service.workspace_path', default=None)
    if not workspace_path:
        # 没有 cloud.storage 配置：普通本地路径不需要额外挂载
        return
    if not cluster_path.startswith(str(workspace_path).rstrip('/')):
        # 非云存储工作区（普通本地/集群路径），保持改动前的行为不变（K-05）
        return
    if cluster_path in _mounted_paths(task_impl):
        return

    task_impl._runtime_mounts.append({
        'host_path': cluster_path,
        'mount_path': cluster_path,          # 与解析路径完全一致，保证 cd / MARSV2_TASK_WORKSPACE 语义不变
        'mount_type': 'DirectoryOrCreate',   # 路径缺失也能起 pod（存在性由 parse_code_cmd 提前拦截）
        'read_only': False,                  # 工作区必须可写（训练要写 checkpoint）
        'name': WORKSPACE_MOUNT_NAME,
    })


def add_runtime_mounts(task_impl: ITaskImpl):
    """
    在 task_impl 中添加动态的挂载点，这样可以在 schema 的时候调用进去
    # 一般而言，挂载点是在 storage 表中的，但是会有在运行环境中指定的额外挂载点

    举例：
    task.runtime_mounts.append({
        'host_path': mount_src_path,
        'mount_path': mountpath,
        'mount_type': 'DirectoryOrCreate',
        'read_only': False,
        'name': 'workspace-path'
    })

    本函数在 `ITaskImpl.__init__` 中被调用（early init，早于任务创建）：不做存在性校验
    （不碰文件系统）、没有 cloud.storage 配置时静默返回、任何异常都不向上抛（非法的 workspace
    URI 由 `parse_code_cmd` 抛出可读的 TaskSchemaError）。唯一一次 storage 表读取在
    `_mounted_paths()`（用于 mount_path 去重，build_schemas 会复用同一份缓存）。

    :param task_impl:
    :return:
    """
    try:
        _add_workspace_mount(task_impl)
    except Exception:
        pass
