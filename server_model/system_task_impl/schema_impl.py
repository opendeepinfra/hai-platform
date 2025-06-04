

import os
from abc import ABC
from cached_property import cached_property

import conf
from server_model.task_impl.single_task_impl import SingleTaskImpl


class SystemTaskSchemaImpl(SingleTaskImpl, ABC):

    @cached_property
    def train_environment(self, *args, **kwargs):
        env = super(SystemTaskSchemaImpl, self).train_environment
        # 如果 train_environment 说要 system，就用当前 manager 的镜像
        if env.env_name == 'system':
            env.image = os.environ['CURRENT_POD_IMAGE']
        return env
