

from fastapi import Depends

from api.app import app
from api.depends import get_task_no_check
from api.depends import request_limitation
from base_model.training_task import TrainingTask
from utils import run_cmd_aio


async def swap_memory(swap_limit: int, task: TrainingTask = Depends(get_task_no_check)):
    """
    设置容器 memsw_limit
    """
    try:
        container_name = f"{task.user_name}-{task.id}-0"
        await run_cmd_aio(
            f'ssh -o StrictHostKeyChecking=no -i /home/hpp/.ssh/id_rsa hpp@{task.assigned_nodes[0]} '  # 用hpp这个用户去ssh
            f""" "if docker ps | grep {container_name } > /dev/null; then """
            f""" docker ps | grep {container_name } | awk '{{system(\\\"docker update --memory-swap '{swap_limit}g' \\\" \$1)}}'; """
            f""" else sudo crictl pods --no-trunc -q --name {container_name} | xargs sudo crictl ps --no-trunc -q -p | xargs sudo runc --root /run/containerd/runc/k8s.io/ update --memory-swap {swap_limit}g; fi" """,
            timeout=10)
        return {
            'success': 1,
            'result': "success"
        }
    except Exception as e:
        return {
            'success': 0,
            'result': "failed",
            'msg': str(e)
        }
