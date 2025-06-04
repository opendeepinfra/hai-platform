
import re
from abc import ABC

from real_time_logs.api import get_task_node_idx_log
from server_model.training_task_impl.task_api_impl import TaskApiImpl


class ServiceTaskApiImpl(TaskApiImpl, ABC):
    async def log(self, rank: int, just_error=False, last_seen=None, service=None, **kwargs):
        """
        获取 ServiceTask 的日志，支持指定 service 查看指定服务的日志
        注意即使指定了 service，返回信息中的 stop_code, exit_code, error_msg 信息仍是 task 信息（即整个容器的信息），单个服务无此项数据
        @param service: 指定的服务名，值为 None 时查看所有日志
        @return:
        """
        suffix = f'{service}.service_log' if service else f'#{rank}'
        return await super().log(rank=rank, just_error=just_error, last_seen=last_seen, suffix_filter=suffix)
