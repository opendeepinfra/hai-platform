from collections import defaultdict

from kubernetes.client import ApiException

from base_model.base_user import BaseUser
from base_model.training_task import TrainingTask
from k8s.v1_api import create_nodeport_service
from k8s.v1_api import corev1 as k8s_corev1_api
from server_model.user_impl import UserQuota, UserDb
from utils import convert_task_job_to_key


class UserNodePort:
    def __init__(self, user: BaseUser):
        self.user = user
        self.user_quota: UserQuota = self.user.quota
        self.user_db: UserDb = self.user.db

    def find_all(self, alias=None, nb_name=None, rank=None, dist_port=None):
        any_str, any_str_or_empty, any_num = ".+?", ".*?", "\\d+?"
        pattern = f'port:{alias or any_str_or_empty}:{nb_name or any_str}:{rank or any_num}:{dist_port or any_num}'
        quota_df = self.user_quota.quota_df
        return quota_df[quota_df.index.str.fullmatch(pattern)]

    async def async_get_all(self):
        await self.user_quota.prefetch_quota_df()
        port_df = self.find_all()
        results = defaultdict(list)
        for resource, row in port_df.iterrows():
            # resource 格式 port:{usage}:{nb_name}:{rank}:{dist_port}
            try:
                alias, nb_name, rank, dist_port = resource.split(':')[1:]
                results[nb_name].append({
                    'alias': alias, 'rank': int(rank), 'dist_port': int(dist_port), 'src_port': int(row.quota)
                })
            except ValueError as err:
                print(f'解析 {self.user.user_name} 的端口信息失败: resource={resource} ({err})')
        return results

    def create(self, task: TrainingTask, namespace: str, alias: str, dist_port: int, rank: int = 0):
        if ':' in alias or len(alias) > 32:
            raise Exception(f'alias(即usage) 不能包含 ":" 且长度不能超过32')

        try:
            node_name = task.assigned_nodes[rank]
            node = k8s_corev1_api.read_node(node_name)
            node_ip = next(data.address for data in node.status.addresses if data.type=='InternalIP')
        except ApiException as ae:
            raise Exception(f'根据rank：{rank}没能查询到任务所在节点') from ae

        task_key = convert_task_job_to_key(task, rank)
        service_name = f's{task_key}-{dist_port}'
        port_quota = self.user_quota.port_quota

        if port_quota == 0:
            raise Exception(f'用户没有端口暴露的 quota，不能申请端口')
        existed_port = self.find_all(nb_name=task.nb_name, rank=rank, dist_port=dist_port)
        if len(existed_port) != 0:
            src_port = int(existed_port.quota[0])
        else:
            # 新建记录
            src_port = None
            if len(self.find_all()) == port_quota:  # 没有这个端口的 quota 了
                raise Exception(f'用户不能申请更多的端口了，最多 {port_quota} 个，可以释放不常用的任务端口')
        result = create_nodeport_service(service_name=service_name,
                                         namespace=namespace,
                                         dist_port=dist_port,
                                         selector={'task_key': task_key},
                                         src_port=src_port)
        if src_port is None:
            src_port = result['port']
            port_name = f'port:{alias}:{task.nb_name}:{rank}:{dist_port}'
            self.user_db.insert_quota(port_name, src_port)
        return {
            'ip': node_ip,
            'port': int(src_port),
            'existed': result['existed'],
        }

    def delete(self, task: TrainingTask, namespace: str, dist_port: int, rank: int = 0):
        port_df = self.find_all(nb_name=task.nb_name, rank=rank, dist_port=dist_port)
        if len(port_df) == 0:
            raise Exception(f'{task.nb_name} 容器不存在该端口，请检查 dist_port, rank 是否正确')
        task_key = convert_task_job_to_key(task, rank)
        service_name = f's{task_key}-{dist_port}'
        try:
            k8s_corev1_api.delete_namespaced_service(name=service_name, namespace=namespace)
        except ApiException as ae:
            # 如果要删除的 nodeport 已经不存在于 k8s 集群上了, 继续删除 DB 记录
            if ae.status != 404:
                raise ae
        self.user_db.delete_quota(port_df.index[0])

    def quota_info(self):
        return {
            'quota': self.user_quota.port_quota,
            'used_quota': len(self.find_all())
        }
