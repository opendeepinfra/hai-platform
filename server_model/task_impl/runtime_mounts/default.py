from base_model.base_task import ITaskImpl

from ..workspace_resolver import workspace_uri_to_cluster_path


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


def _add_workspace_mount(task_impl: ITaskImpl):
    """
    为云存储工作区追加一个 pod 挂载点。

    ⚠️ 这里**绝对不能访问 `task_impl.task.user`**：`task_impl.user` 是
    `base_task.user`（base_task.py:262）这样的延迟属性，它的求值会再次构造
    TaskImpl（`AutoTaskImpl.__new__` → `SingleTaskImpl.__init__` → `add_runtime_mounts`），
    在本函数里取 user 会造成**无限递归 → 栈溢出 SIGABRT**（manager 容器崩溃重启）。

    因此这里只做「纯字符串」解析：`<provider>://<group>/<user>/workspaces/<name>`
    → `<workspace_path>/<group>/<user>/workspaces/<name>`；
    归属校验（group/user 是否属于当前用户）由 `parse_code_cmd`（build_schemas 阶段，
    user 已就绪）与任务提交接口负责，两处都会再次解析并校验。
    """
    workspace = _schema_workspace(task_impl)
    if not workspace:
        return

    cluster_path = workspace_uri_to_cluster_path(workspace)
    if not cluster_path:
        # 非 URI / scheme 不匹配 / 缺配置：普通本地或集群路径，保持改动前行为（K-05）
        return

    # 去重只看本次已追加的 runtime mounts（storage 表挂载由 build_schemas 统一处理，
    # 那里访问 user 是安全的）
    mounted = {m.get('mount_path') for m in (getattr(task_impl, '_runtime_mounts', None) or [])}
    if cluster_path in mounted:
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

    本函数在 `ITaskImpl.__init__` 中被调用（early init，早于任务创建）：
    只做纯字符串解析，**不访问 user、不查库、不做 I/O**；没有 cloud.storage 配置或
    workspace 不是云存储 URI 时静默返回；任何异常都不向上抛
    （非法的 workspace URI 由提交接口与 `parse_code_cmd` 抛出可读的 TaskSchemaError）。

    :param task_impl:
    :return:
    """
    try:
        _add_workspace_mount(task_impl)
    except Exception:
        pass
