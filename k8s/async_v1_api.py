import asyncio
import pandas as pd
from conf import MARS_GROUP_FLAG
from k8s_worker import k8s_worker_node_gpu_num, k8s_worker_node_cpu_num, k8s_worker_node_memory_num, \
    k8s_worker_get_nodes_df, k8s_worker_set_node_label, k8s_worker_cordon_node, k8s_worker_uncordon_node, \
    k8s_worker_get_pod, k8s_worker_set_pod_annotations, k8s_worker_get_nodes_leaf, k8s_worker_get_nodes_spine, \
    k8s_worker_get_storage_quota


async def waiting_result(x):
    while True:
        if x.ready():
            return x.get()
        await asyncio.sleep(0.01)


async def async_node_gpu_num(node):
    x = k8s_worker_node_gpu_num.delay(node)
    return await waiting_result(x)


async def async_node_cpu_num(node):
    x = k8s_worker_node_cpu_num.delay(node)
    return await waiting_result(x)


async def async_node_memory_num(node):
    x = k8s_worker_node_memory_num.delay(node)
    return await waiting_result(x)


async def async_get_nodes_df():
    while True:
        try:
            x = k8s_worker_get_nodes_df.delay()
            nodes_df = pd.DataFrame(await waiting_result(x))
            nodes_leaf = k8s_worker_get_nodes_leaf()
            nodes_spine = k8s_worker_get_nodes_spine()
            nodes_df['LEAF'] = nodes_df.NAME.apply(lambda x: nodes_leaf.get(x, 'NO_LEAF'))
            nodes_df['SPINE'] = nodes_df.NAME.apply(lambda x: nodes_spine.get(x, 'NO_SPINE'))
            return nodes_df
        except:
            await asyncio.sleep(0.01)


async def get_storage_quota():
    while True:
        try:
            x = k8s_worker_get_storage_quota.delay()
            return await waiting_result(x)
        except:
            await asyncio.sleep(0.01)


async def async_set_node_label(node: str, key: str, value: str):
    x = k8s_worker_set_node_label.delay(node, key, value)
    return await waiting_result(x)


async def async_set_node_groups(node: str, *groups) -> bool:
    """

    @param node:
    @param groups: 多层 group 如，*[‘jd_all’, 'jd_a100', 'jd_a200'] 等等
    @return:
    """
    assert len(groups) >= 2, '必须设置 lv0 和 lv1 的 group'
    return await async_set_node_label(node, MARS_GROUP_FLAG, '.'.join(groups))


async def async_move_group_to_debug_monitor(node):
    node_type = 'dev' if node.endswith('dev') else 'train'
    return await async_set_node_groups(node, f'jd_debug_{node_type}', 'jd_debug_monitor')


async def async_cordon_node(node):
    x = k8s_worker_cordon_node.delay(node)
    return await waiting_result(x)


async def async_uncordon_node(node):
    x = k8s_worker_uncordon_node.delay(node)
    return await waiting_result(x)


# commit by wenjun
async def async_get_pod(namespace, name):
    x = k8s_worker_get_pod.delay(namespace, name)
    return await waiting_result(x)


# commit by wenjun
async def async_set_pod_annotations(namespace, name, annotations):
    x = k8s_worker_set_pod_annotations.delay(namespace, name, annotations)
    return await waiting_result(x)
