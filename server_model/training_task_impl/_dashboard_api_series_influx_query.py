import asyncio
import numpy as np
import pandas as pd
from aioinflux import InfluxDBClient

# 20220217: 因为线上influxdb里只有这个container_name被采集，在此成固定的
CONTAINER_NAME="hf-experiment"

def _make_node_query_string(node_list):
    return "kubernetes_node=~/" + "|".join(node_list)+ "/"

def pod_list_query_string(user_name, task_id, rank: int = -1):
    return f"pod='{user_name}-{task_id}-{rank}'" if rank != -1 else f"pod=~/{user_name}-{task_id}-d*/"


# ========== 5min 查询，使用降采样聚合过的数据 ==========

async def query_gpu_all_5min(user_name, task_id, rank, node_list, time_begin, time_end, influxdb_conf):
    node_list_string = _make_node_query_string(node_list)
    sql = f'''
    SELECT mean("gpuutil"), mean("fb") FROM "a_year"."downsampled_node_gpu"
    WHERE ({node_list_string}) AND time>{time_begin} AND time<={time_end} group by time(5m)
    '''
    async with InfluxDBClient(**influxdb_conf) as client:
            g_res = await client.query(sql)
            res_all = g_res['results'][0]['series'][-1]['values']
            # [[t1,va1,vb1]...] -> [[t1,t2..],[va1,va2...][vb1,vb2...]]
            res_all = [ [x[0] for x in res_all],
                    [round(x[1],2) if x[1] is not None else None for x in res_all],
                    [round(x[2],0) if x[2] is not None else None for x in res_all] ]
    return res_all

async def query_cpu_io_5min(user_name, task_id, rank, node_list, time_begin, time_end, influxdb_conf):
    pods_qstr = pod_list_query_string(user_name, task_id, rank)
    node_list_string = _make_node_query_string(node_list)
    query_cpu_util = f'''
    SELECT  mean("cpumean") FROM "a_year"."downsampled_ctn_cpu"
    WHERE {pods_qstr} 
    AND container='{CONTAINER_NAME}' 
    AND time>{time_begin} AND time<={time_end} group by time(5m)
    '''
    query_ib = f'''
    SELECT mean(ibrecv)/1024/1024/1024 as ibrecv, mean(ibtrans)/1024/1024/1024 as ibtrans 
    FROM  "a_year"."downsampled_node_ib"
    WHERE ({node_list_string}) AND time>{time_begin} AND time<={time_end} group by time(5m)
    '''

    async with InfluxDBClient(**influxdb_conf) as client:
        a_res, b_res = await asyncio.gather(client.query(query_cpu_util), client.query(query_ib))
    a_res = a_res['results'][0]['series'][-1]['values']
    b_res = b_res['results'][0]['series'][-1]['values']
    # 对齐两个序列,有可能差若干时间戳
    df1 = pd.DataFrame(a_res, columns=['ts', 'cpu'])
    df2 = pd.DataFrame(b_res, columns=['ts', 'ibr', 'ibt'])
    df_merged = pd.merge(df1, df2, how="outer", on="ts", sort=True)
    df_merged = df_merged.round(2).replace({np.nan: None}) # 节省传输量，降低精度
    res_all = tuple(df_merged.T.to_records(index=False).tolist()) # .T 行列转换 to_records 按行输出
    return res_all


async def query_mem_5min(user_name, task_id, rank, node_list, time_begin, time_end, influxdb_conf):
    pods_qstr = pod_list_query_string(user_name, task_id, rank)
    query_mem = f'''
    SELECT mean(rss)/1024/1024/1024 as "rss", mean(cache)/1024/1024/1024 as "cache", 
    mean(memlimit)/1024/1024/1024 as "limit" FROM "a_year"."downsampled_ctn_mem"
    WHERE {pods_qstr} 
    AND time>{time_begin} AND time<={time_end} group by time(5m)
    '''
    async with InfluxDBClient(**influxdb_conf) as client:
        mem_res = await client.query(query_mem)
    res_all = mem_res['results'][0]['series'][-1]['values']
    # [[t1,va1,vb1]...] -> [[t1,t2..],[va1,va2...][vb1,vb2...]]
    res_all = [[x[0] for x in res_all],
            [round(x[1], 2) if x[1] is not None else None for x in res_all], # rss
            [round(x[2] + x[1], 2) if (x[2] is not None and x[1] is not None) else None for x in res_all], # rss + cache
            [round(x[3], 2) if x[3] is not None else None for x in res_all]] # limit
    return res_all

async def query_every_card_5min(user_name, task_id, rank, node_list, time_begin, time_end, influxdb_conf):
    node_list_string = _make_node_query_string(node_list)
    query_gpu_by_card = f'''
    SELECT mean("gpuutil") FROM "a_year"."downsampled_node_gpu"
    WHERE ({node_list_string})
    AND time>{time_begin} AND time<={time_end} group by time(5m),"gpu"
    '''
    async with InfluxDBClient(**influxdb_conf) as client:
        every_card_res = await client.query(query_gpu_by_card)
    every_card_res = every_card_res['results'][0]['series']
    time_series = [x[0] for x in every_card_res[0]['values']]
    card_points = {}
    for card_series in every_card_res:
        key = int(card_series['tags']['gpu'])
        card_points[key] = [round(x[1],2) if x[1] is not None else None for x in card_series['values']]

    res_all  = [time_series]
    for i in range(len(card_points.keys())):
        res_all.append(card_points.get(i, [None for x in time_series]))
    return res_all


# ========== 1min 查询，使用降采样聚合过的数据 ==========

def align_timestamp_to_min(t):
    '''
    对齐纳秒级时间戳到整分钟
    '''
    align = 1000000000 * 60 # NANOSECOND
    return (t // align) * align


async def query_gpu_all_1min(user_name, task_id, rank, node_list, time_begin, time_end, influxdb_conf):
    node_list_string = _make_node_query_string(node_list)
    gpu_util_sql = f'''
    SELECT mean("value") FROM "DCGM_FI_DEV_GPU_UTIL"
    WHERE ({node_list_string})
    AND time>{time_begin} AND time<={time_end} group by time(1m)
    '''
    gpu_fb_used_sql = f'''
    SELECT mean("value") FROM "DCGM_FI_DEV_FB_USED"
    WHERE ({node_list_string})
    AND time>{time_begin} AND time<={time_end} group by time(1m)
    '''
    async with InfluxDBClient(**influxdb_conf) as client:
        a_res, b_res = await asyncio.gather(
                        client.query(gpu_util_sql),
                        client.query(gpu_fb_used_sql))
    a_res = a_res['results'][0]['series'][-1]['values']
    b_res = b_res['results'][0]['series'][-1]['values']
    # 对齐两个序列,有可能差若干时间戳
    dfa = pd.DataFrame(a_res, columns=['ts', 'util'])
    dfb = pd.DataFrame(b_res, columns=['ts', 'fb'])
    df_merged = pd.merge(dfa, dfb, how="outer", on="ts", sort=True)
    df_merged.dropna(how='any', inplace=True)
    df_merged.drop_duplicates(subset=['ts'], keep='first',inplace=True)
    df_merged = df_merged.round(2).replace({np.nan: None}) # 节省传输量，降低精度
    res_all = tuple(df_merged.T.to_records(index=False).tolist()) # .T 行列转换 to_records 按行输出
            # [[t1,va1,vb1]...] -> [[t1,t2..],[va1,va2...][vb1,vb2...]]
    return res_all

async def query_cpu_io_1min(user_name, task_id, rank, node_list, time_begin, time_end, influxdb_conf):
    # 字段: CPU / IBR /IBT
    pods_qstr = pod_list_query_string(user_name, task_id, rank)
    node_list_string = _make_node_query_string(node_list)
    query_cpu_util = f'''
    SELECT derivative(value, 1s) AS cpu 
    FROM prometheus.a_week.container_cpu_usage_seconds_total 
    WHERE {pods_qstr}
    AND container='{CONTAINER_NAME}' 
    AND time>{time_begin} AND time<={time_end}
    '''
    query_ib_recv = f'''
    SELECT derivative(value, 1s)/1024/1024/1024 AS recv 
    FROM prometheus.a_week.node_infiniband_port_data_received_bytes_total 
    WHERE time>{time_begin} AND time<={time_end}
    AND {node_list_string}
    '''
    query_ib_trans = f'''
    SELECT derivative(value, 1s)/1024/1024/1024 AS recv 
    FROM prometheus.a_week.node_infiniband_port_data_transmitted_bytes_total 
    WHERE time>{time_begin} AND time<={time_end}
    AND {node_list_string}
    '''
    async with InfluxDBClient(**influxdb_conf) as client:
        cpu_res, ibr_res, ibt_res = await asyncio.gather(
            client.query(query_cpu_util),
            client.query(query_ib_recv),
            client.query(query_ib_trans))
    cpu_res = cpu_res['results'][0]['series'][-1]['values']
    ibr_res = ibr_res['results'][0]['series'][-1]['values']
    ibt_res = ibt_res['results'][0]['series'][-1]['values']
    # 对齐两个序列,有可能差若干时间戳
    df_cpu = pd.DataFrame(cpu_res, columns=['ts', 'cpu'])
    df_ibr = pd.DataFrame(ibr_res, columns=['ts', 'ibr'])
    df_ibt = pd.DataFrame(ibt_res,columns=['ts', 'ibt'])
    
    # 对齐时间戳到分
    for df in (df_cpu, df_ibr, df_ibt):
        df['ts'] = df['ts'].apply(align_timestamp_to_min)

    df_ib = pd.merge(df_ibr, df_ibt, how="outer", on="ts", sort=True)
    df_merged = pd.merge(df_cpu, df_ib, how="outer", on="ts", sort=True)
    df_merged.dropna(how='any', inplace=True)
    df_merged.drop_duplicates(subset=['ts'], keep='first',inplace=True)
    df_merged = df_merged.round(2).replace({np.nan: None}) # 节省传输量，降低精度
    res_all = tuple(df_merged.T.to_records(index=False).tolist()) # .T 行列转换 to_records 按行输出
    return res_all


async def query_every_card_1min(user_name, task_id, rank, node_list, time_begin, time_end, influxdb_conf):
    node_list_string = _make_node_query_string(node_list)
    query_gpu_by_card = f'''
    SELECT "value" FROM "DCGM_FI_DEV_GPU_UTIL"
    WHERE ({node_list_string})
    AND time>{time_begin} AND time<={time_end} group by "gpu"
    '''
    async with InfluxDBClient(**influxdb_conf) as client:
        every_card_res = await client.query(query_gpu_by_card)
    every_card_res = every_card_res['results'][0]['series']
    time_series = [x[0] for x in every_card_res[0]['values']]
    card_points = {}
    for card_series in every_card_res:
        key = int(card_series['tags']['gpu'])
        card_points[key] = [round(x[1],2) if x[1] is not None else None for x in card_series['values']]

    res_all  = [time_series]
    for i in range(len(card_points.keys())):
        res_all.append(card_points.get(i, [None for x in time_series]))
    return res_all

async def query_mem_1min(user_name, task_id, rank, node_list, time_begin, time_end, influxdb_conf):
    pods_qstr = pod_list_query_string(user_name, task_id, rank)
    
    query_cache = f'''
    SELECT value /1024/1024/1024
    FROM prometheus.a_week.container_memory_cache
    WHERE container = '{CONTAINER_NAME}'
    AND {pods_qstr}
    AND time>{time_begin} AND time<={time_end}
    '''

    query_rss = f'''
    SELECT value /1024/1024/1024
    FROM prometheus.a_week.container_memory_rss
    WHERE container = '{CONTAINER_NAME}'
    AND {pods_qstr}
    AND time>{time_begin} AND time<={time_end}
    '''

    query_limit = f'''
    SELECT value /1024/1024/1024
    FROM prometheus.a_week.container_spec_memory_limit_bytes
    WHERE container = '{CONTAINER_NAME}'
    AND {pods_qstr}
    AND time>{time_begin} AND time<={time_end}
    '''

    async with InfluxDBClient(**influxdb_conf) as client:
        cache_res, rss_res, limit_res = await asyncio.gather(
            client.query(query_cache),
            client.query(query_rss),
            client.query(query_limit))
    rss_res = rss_res['results'][0]['series'][-1]['values']
    cache_res = cache_res['results'][0]['series'][-1]['values']
    limit_res = limit_res['results'][0]['series'][-1]['values']

    df_rss = pd.DataFrame(rss_res, columns=['ts', 'rss'])
    df_cache = pd.DataFrame(cache_res, columns=['ts', 'cache'])
    df_limit = pd.DataFrame(limit_res,columns=['ts', 'limit'])

    for df in (df_cache, df_rss, df_limit):
        df['ts'] = df['ts'].apply(align_timestamp_to_min)

    df_t1 = pd.merge(df_rss, df_cache, how="outer", on="ts", sort=True)
    df_merged = pd.merge(df_t1, df_limit, how="outer", on="ts", sort=True)
    column_used = df_merged['rss'] + df_merged['cache']
    df_merged = df_merged.drop('cache' ,axis=1)
    df_merged.insert(2, 'used', column_used)
    df_merged.dropna(how='any', inplace=True)
    df_merged.drop_duplicates(subset=['ts'], keep='first',inplace=True)
    df_merged = df_merged.round(2).replace({np.nan: None})
    res_all = tuple(df_merged.T.to_records(index=False).tolist()) # .T 行列转换 to_records 按行输出
    # [[t1,va1,vb1]...] -> [[t1,t2..],[va1,va2...][vb1,vb2...]]
    return res_all

query_func_map = {
    'gpu':{
        '5min': query_gpu_all_5min,
        '1min': query_gpu_all_1min,
    },
    'cpu':{
        '5min': query_cpu_io_5min,
        '1min': query_cpu_io_1min,
    },
    'every_card':{
        '5min': query_every_card_5min,
        '1min': query_every_card_1min,
    },
    'mem':{
        '5min': query_mem_5min,
        '1min': query_mem_1min,
    }
}