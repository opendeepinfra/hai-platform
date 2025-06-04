

from fastapi import Depends

from base_model.training_task import TrainingTask
from conf.flags import TASK_FLAG, QUE_STATUS, CHAIN_STATUS
from server_model.pod import Pod
from server_model.user import User
from utils import convert_to_external_task
from api.query.depends import get_api_user_in_query_db
from api.query.query_db import QueryDB


async def get_tasks_in_query_db(user: User, sql_where, sql_where_args, page: int = None, page_size: int = None):
    sql = f"""
    select 
        "t".*, "task".*, "task"."suspend_code" & {TASK_FLAG.STAR} = {TASK_FLAG.STAR} as "star",
        case 
            when "task"."queue_status" = '{QUE_STATUS.FINISHED}' then '{CHAIN_STATUS.FINISHED}'
            when "task"."queue_status" = '{QUE_STATUS.QUEUED}' and "task"."id" != "task"."first_id" then '{CHAIN_STATUS.SUSPENDED}'
            when "task"."queue_status" = '{QUE_STATUS.QUEUED}' and "task"."id" = "task"."first_id" then '{CHAIN_STATUS.WAITING_INIT}'
            when "task"."queue_status" = '{QUE_STATUS.SCHEDULED}' then '{CHAIN_STATUS.RUNNING}'
        end as "chain_status"
    from "task"
    inner join (
        select
            max("id") as "max_id",
            array_agg("id" order by "id") as "id_list",
            array_agg("queue_status" order by "id") as "queue_status_list",
            array_agg("begin_at" order by "id") as "begin_at_list",
            array_agg("end_at" order by "id") as "end_at_list",
            array_agg("created_at" order by "id") as "created_at_list",
            array_agg("stop_code" order by "id") as "stop_code_list",
            array_agg("suspend_code" order by "id") as "suspend_code_list",
            array_agg("whole_life_state" order by "id") as "whole_life_state_list",
            array_agg("worker_status" order by "id") as "worker_status_list",
            (array_agg("pods" order by "id" desc) filter (where "pods" != '[]'))[1] as "last_pods"
        from "task"
        where "chain_id" in (
            select distinct on ("chain_id", "first_id") "chain_id"
            from "task"
            where "user_name" = '{user.user_name}' {sql_where}
            order by "first_id" desc
            {'' if page is None else f'limit {page_size} offset {page_size * (page - 1)}'}
        )
        group by "chain_id"
    ) as "t" on "t"."max_id" = "task"."id"
    order by "first_id" desc
    """
    count_sql = f"""
    select count(distinct "chain_id") as "count"
    from "task"
    where "user_name" = '{user.user_name}' {sql_where}
    """
    results = await QueryDB.a_execute(sql, sql_where_args)
    total_count = (await QueryDB.a_execute(count_sql, sql_where_args))[0]['count']
    res = []
    for r in results:
        task = TrainingTask(**r)
        task._pods_ = [Pod(**p) for p in r['pods']]
        del task._trait_values['config']
        if not user.is_internal:
            task = convert_to_external_task(task)
        res.append(task._trait_values)
    return res, total_count


async def get_tasks_api(
        page: int,
        page_size: int,
        task_type_list: str,
        nb_name_pattern: str = None,
        worker_status_list: str = None,
        queue_status_list: str = None,
        only_star: bool = False,
        user: User = Depends(get_api_user_in_query_db)
):
    if page <= 0:
        return {
            'success': 0,
            'msg': 'page 必须大于等于 1'
        }
    if page_size <= 0 or page_size > 100:
        return {
            'success': 0,
            'msg': 'page_size 需要为 1 ~ 100'
        }
    task_type_list = task_type_list.split(',')
    sql_where = f''' and "task_type" in ({",".join("%s" for _ in task_type_list)}) '''
    sql_where_args = tuple(task_type_list)
    if nb_name_pattern is not None:
        sql_where += ' and "nb_name" like %s '
        sql_where_args += (f'%{nb_name_pattern}%', )
    if worker_status_list is not None:
        worker_status_list = worker_status_list.split(',')
        sql_where += f''' and "worker_status_list" = ({",".join("%s" for _ in worker_status_list)}) '''
        sql_where_args += tuple(worker_status_list)
    if queue_status_list is not None:
        queue_status_list = queue_status_list.split(',')
        sql_where += f''' and "queue_status" = ({",".join("%s" for _ in queue_status_list)}) '''
        sql_where_args += tuple(queue_status_list)
    if only_star:
        sql_where += f' and "suspend_code" & {TASK_FLAG.STAR} = {TASK_FLAG.STAR} '
        sql_where_args += (f'%{nb_name_pattern}%', )
    results, total_count = await get_tasks_in_query_db(user=user, sql_where=sql_where, sql_where_args=sql_where_args, page=page, page_size=page_size)
    return {
        'success': 1,
        'chain_tasks': results,
        'total': total_count
    }


async def get_task_api(
        id: int = None,
        chain_id: str = None,
        nb_name: str = None,
        user: User = Depends(get_api_user_in_query_db)
):
    if id is not None:
        sql_where = ' and "id" = %s '
        sql_where_args = (id, )
    elif chain_id is not None:
        sql_where = ' and "chain_id" = %s '
        sql_where_args = (chain_id, )
    elif nb_name is not None:
        sql_where = ' and "nb_name" = %s '
        sql_where_args = (nb_name, )
    else:
        return {
            'success': 0,
            'msg': '必须指定 id, chain_id 或者 nb_name'
        }
    results, _ = await get_tasks_in_query_db(user=user, sql_where=sql_where, sql_where_args=sql_where_args, page=1, page_size=1)
    if len(results) == 0:
        return {
            'success': 0,
            'msg': '没有符合条件的任务'
        }
    return {
            'success': 1,
            'task': results[0]
        }
