import asyncio
import datetime
import time

from abc import ABC
from typing import List, Union
from aioinflux import InfluxDBClient
from dateutil.parser import parse

from db import a_db_engine, sql_params
import sqlalchemy
from conf import DB_CONF, CONTAINER_NAME
from conf.flags import QUE_STATUS
from server_model.training_task_impl.additional_property_impl import \
    AdditionalPropertyImpl

from ._dashboard_api_series_influx_query import query_func_map

def nodes_list_query_string(task, rank: int = -1):
    return "kubernetes_node=~/" + "|".join(
        task.assigned_nodes[rank:rank + 1] if 0 <= rank < len(
            task.assigned_nodes) else task.assigned_nodes) + "/"


def pod_list_query_string(task, rank: int = -1):
    return f"pod='{task.user_name}-{task.id}-{rank}'" if rank != -1 else f"pod=~/{task.user_name}-{task.id}-d*/"


class DashboardApiImpl(AdditionalPropertyImpl, ABC):
    async def get_latest_point(self):
        task = self.task
        if task.queue_status != QUE_STATUS.SCHEDULED:
            return {
                'gpu_util': None,
                'ib_recv': None,
                'ib_trans': None,
            }
        nodes_qstr = nodes_list_query_string(task)
        query_gpu = f'''
        SELECT mean("value") FROM DCGM_FI_DEV_GPU_UTIL
        WHERE ({nodes_qstr}) AND time>now()-2m
        '''
        # ib_recv 即时值取法解释
        # 对每个节点的ib_recv_count，最近3分钟内的点取导数，然后取最后一个结果。因为倒序之后求导，所以结果是负的
        # 拿到数据后需要手动求平均。使用influxdb自带的嵌套查询聚合数据也可以，但是性能会下降若干倍，体验极差。
        # 所以此处手动处理

        # {'results': [{'series': [{'columns': ['time', 'derivative'],
        #                       'name': 'node_infiniband_port_data_received_bytes_total',
        #                       'tags': {'kubernetes_node': 'jd-f0403-dl'},
        #                       'values': [[1617792123638000000,
        #                                   -1.4267205166940888]]},
        #                      {'columns': ['time', 'derivative'],
        #                       'name': 'node_infiniband_port_data_received_bytes_total',
        #                       'tags': {'kubernetes_node': 'jd-f0402-dl'},
        #                       'values': [[1617792152038000000,
        #                                   -8.280078570048015e-07]]}],
        #           'statement_id': 0}]}

        query_ibrecv = f'''
        SELECT DERIVATIVE("value",1s)/1024/1024/1024
        FROM "node_infiniband_port_data_received_bytes_total"
        WHERE  ({nodes_qstr}) and time>now()-3m
        GROUP BY "kubernetes_node" ORDER BY time desc limit 1
        '''

        query_ibtrans = f'''
        SELECT DERIVATIVE("value",1s)/1024/1024/1024
        FROM "node_infiniband_port_data_transmitted_bytes_total"
        WHERE  ({nodes_qstr}) and time>now()-3m
        GROUP BY "kubernetes_node" ORDER BY time desc limit 1
        '''
        try:
            async with InfluxDBClient(**DB_CONF.influxdb) as client:
                res_gpu, res_ibrecv, res_ibtrans = await asyncio.gather(
                    client.query(query_gpu),
                    client.query(query_ibrecv),
                    client.query(query_ibtrans)
                    )

                res_gpu = res_gpu['results'][0]['series']
                res_ibrecv = res_ibrecv['results'][0]['series']
                res_ibtrans = res_ibtrans['results'][0]['series']

                ib_recv_value = [n['values'][0][1] for n in res_ibrecv]
                ib_recv_value = -1 * sum(ib_recv_value) / len(ib_recv_value)

                ib_trans_value = [n['values'][0][1] for n in res_ibtrans]
                ib_trans_value = -1 * sum(ib_trans_value) / len(ib_trans_value)


            ret = {
                'gpu_util': res_gpu[-1]['values'][-1][1] if len(res_gpu) else None,
                'ib_recv': ib_recv_value,
                'ib_trans': ib_trans_value
            }
        except Exception:
            # 用来前端处理错误
            ret = {
                'gpu_util': -1,
                'ib_recv': -1,
                'ib_trans': -1
            }
        return ret

    
    async def get_chain_time_series(self, query_type: str, rank: int, *args, **kwargs):
        '''
        获取整条chain的数据
        '''

        data_interval = kwargs.get('data_interval', '5min')

        async def _get_single_series(node_list:List[str], time_begin_sec_ts:int, time_end_sec_ts:int, query_type:Union['gpu','cpu','every_card', 'mem'], user_name:str, task_id:int, rank:int, data_interval='5min'):
            # 返回数据 [[timestamp:NanoSecond],[a_values],[b_values]...]
            
            # timestamp in second
            assert len(str(int(time_begin_sec_ts))) == 10
            assert len(str(int(time_end_sec_ts))) == 10

            SEC_TO_NANO = 1000000000 # 秒级别时间戳换算到influxdb的纳秒级

            time_end = time_end_sec_ts * SEC_TO_NANO
            time_begin = time_begin_sec_ts * SEC_TO_NANO

            try:
                func = query_func_map[query_type][data_interval]
                return await func(
                    user_name,
                    task_id,
                    rank, 
                    node_list,
                    time_begin,
                    time_end,
                    DB_CONF.influxdb
                )
            except KeyError:
                # 没有返回有效的数据时,取series会抛出keyError
                return None

        async def _get_all_nodes(chain_id: str):
            SQL = """
            select
                   "t"."user_name" as "user_name",
                   "t"."id" as "id", "t"."assigned_nodes" as "assigned_nodes",
                   "t"."queue_status" as "queue_status",
                   "p"."begin_at" as "begin_at",
                   "t"."end_at" as "end_at"
            from "task_ng" as "t" left join "pod_ng" as "p" on "t"."id"= "p"."task_id"
            where array_length("t"."assigned_nodes", 1) != 0
                and ("t"."queue_status" = 'finished' or "t"."queue_status"='scheduled')
                and "t"."chain_id" = %s
                and "p"."job_id" = 0
            order by "t"."id"
            """
            sql, params = sql_params.format(SQL, (chain_id,))
            async with a_db_engine.begin() as conn:
                res = await conn.execute(sqlalchemy.text(sql), params)
                res = res.all()

            now = datetime.datetime.now()
            res = [dict(row) for row in res]

            for t in res:
                t['end'] = now if t['queue_status'] == 'scheduled' else t['end_at']
            return res

        async def make_obj(t, rank):
            start = int(t['begin_at'].timestamp())
            end = int(t['end'].timestamp())
            ret = {
                "start": start,
                "end": end,
                "series": await _get_single_series(
                    [t['node_list'][rank]],
                    start,
                    end,
                    query_type, t['user_name'], t['id'], rank, data_interval),
                "id": t['id'],
                "rank": rank,
                "type": query_type
            }
            node = t['node_list'][rank]
            if node:
                ret['node'] = node
            return ret
        

        perf_task_list = await _get_all_nodes(self.task.chain_id)

        if len(perf_task_list) == 0:
            # return {
            #     'success':0,
            #     'msg':'no_matched_tasks'    
            # }
            raise ValueError('no_matched_tasks')

        for t in perf_task_list:
            t['node_list'] = t['assigned_nodes']

        max_rank = len(perf_task_list[0]['node_list']) - 1

        if rank < 0 or rank > max_rank:
            raise ValueError(f"rank {rank} out of max:{max_rank}")

        try:
            res = await asyncio.gather(*[make_obj(t, rank) for t in perf_task_list])
            return res

        except:
            return None

            

