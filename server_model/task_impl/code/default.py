from api.task_schema import TaskSchema
from base_model.base_task import ITaskImpl
from ..workspace_resolver import resolve_workspace_path


def parse_code_cmd(task_impl: ITaskImpl):
    """
    # 可以自定义在用户的代码中注入一些东西

    这里会把 `spec.workspace` 中的 `oss://<group>/<user>/workspaces/<name>` 解析为集群真实路径
    （FR-15），返回的 code_dir 会用于 `cd {code_dir}` 与 `MARSV2_TASK_WORKSPACE`；
    非 URI 的本地/集群路径原样返回（回归保护 K-05）。

    :param task_impl:
    :return: code_dir, code_file, code_params
    """
    task_schema: TaskSchema = TaskSchema.parse_obj(task_impl.task.schema)
    workspace = resolve_workspace_path(task_impl.task.user, task_schema.spec.workspace,
                                       check_exists=True)
    return workspace, task_schema.spec.entrypoint, task_schema.spec.parameters
