

import os
from abc import ABC
from cached_property import cached_property
from k8s.v1_api import networkv1beta1 as k8s_networkv1beta1_api

import conf
from server_model.task_impl.single_task_impl import SingleTaskImpl


class ServiceTaskSchemaImpl(SingleTaskImpl, ABC):

    @cached_property
    def train_environment(self, *args, **kwargs):
        env = super(ServiceTaskSchemaImpl, self).train_environment
        if env.env_name == 'system':
            default_python_ver = '3.8'
            env.image = os.environ['CURRENT_POD_IMAGE']
        else:
            default_python_ver = '3.6'
        restored_image = self.user.image.find_one(self.task.nb_name)
        if restored_image:
            env.image = restored_image['image_ref']
        env.config['environments'] = {
            **env.config.get('environments', {}),
            "USER_BIN_PYTHON": env.config.get('python', f'/usr/bin/python{default_python_ver}')
        }
        return env

    def get_service(self, rank):
        all_service = {'nodeports': [], 'headless_services': [], 'ingress_rules':[]}
        if rank == 0:
            services = self.task.config_json.get('services', {})
            # 为保证与现有容器的 jupyter path 兼容, 调整 jupyter 的 ingress path
            transform_service_name = lambda x : '' if x == 'jupyter' else f'/{x}'
            for service_name, service in services.items():
                if service['type'] == 'tcp':
                    all_service['nodeports'].append({'name': service_name, 'port': service['port']})
                elif service['type'] == 'http':
                    all_service['headless_services'].append({'name': service_name, 'port': service['port']})
                    all_service['ingress_rules'].append({
                        'path': f'/{self.task.user_name}/{self.task.nb_name}' + f'{transform_service_name(service_name)}',
                        'port': service['port']
                    })
        return all_service

    def task_run_script(self, *args, **kwargs):
        log_pipe_chain = "ts '[%Y-%m-%d %H:%M:%.S]' | pv -L ${{MAX_OPS}}" \
                         " | rotatelogs -n ${{NUMBER_OF_FILES}}" \
                         " ${{MARSV2_LOG_FILE_PATH}}.{service_name}.service_log ${{MAX_FILESIZE}}"
        service_scripts = '\n'.join([
            f"""
            export MARSV2_SERVICE_NAME={service_name} MARSV2_SERVICE_PORT={service['port']}
            (
                ({service['startup_script']}) 2>&1 | {log_pipe_chain.format(service_name=service_name)}; 
                echo exit code $?
            ) 2>&1 | ts [{service_name}] &
            service_pids=$!,{service_name},{1 if service.get('watch_state') else 0}" "$service_pids
            """
            for service_name, service in self.task.config_json.get('services', {}).items() if service['startup_script']
        ])
        service_pids_file = f'/tmp/.service_pids'
        return self.fix_script(f"""
        set +e
        set -o pipefail
        ulimit -n 204800
        python3 -c "import hf_env; hf_env.set_env('202111'); import hfai; hfai.client.api.set_swap_memory(10000)"
        # service scripts
        {service_scripts}
        echo $service_pids > {service_pids_file}
        unset MARSV2_SERVICE_NAME MARSV2_SERVICE_PORT
        sleep infinity
        """)
