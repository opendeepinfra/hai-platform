from api.task_schema import TaskSchema
from base_model.base_task import ITaskImpl
from ..workspace_resolver import resolve_workspace_path


def parse_code_cmd(task_impl: ITaskImpl):
    """
    # 可以自定义在用户的代码中注入一些东西

    这里会把 `spec.workspace` 中的 `oss://<group>/<user>/workspaces/<name>` 解析为集群真实路径
    （FR-15），返回的 code_dir 会用于 `cd {code_dir}` 与 `MARSV2_TASK_WORKSPACE`；
    非 URI 的本地/集群路径原样返回（回归保护 K-05）。

    ⚠️ 这里**必须保持 `check_exists=False`（默认值）**，只做纯字符串解析：

    本函数在 task manager pod 内执行，而 manager pod 只挂载 kubeconfig
    （`launcher.manager_mounts`），**看不到共享盘上的 `cloud.storage.service.workspace_path`**，
    因此 `os.path.isdir(集群路径)` 恒为 False，会把**已经 push 好的工作区**误判成
    「尚未同步到集群」——实测任务 7 `ugc_e2e3`：21:51:04 工作区已落盘、21:51:11 提交接口
    校验通过，21:51:27 manager 仍报错并把任务判死（worker pod 从未创建）。

    存在性/已同步校验由**提交接口**在 hai-platform pod 内完成
    （`api/operation/implement.py` 的 `resolve_workspace_path(..., check_exists=True)`），
    那里确实能看到共享盘；错误文案与「不产生任务已创建但立即失败」的语义（FR-15 / TC-K06）
    都由提交接口保证。若日后要让 manager 侧也校验，必须先把工作区根目录挂进 manager pod。

    :param task_impl:
    :return: code_dir, code_file, code_params
    """
    task_schema: TaskSchema = TaskSchema.parse_obj(task_impl.task.schema)
    workspace = resolve_workspace_path(task_impl.task.user, task_schema.spec.workspace)
    return workspace, task_schema.spec.entrypoint, task_schema.spec.parameters
