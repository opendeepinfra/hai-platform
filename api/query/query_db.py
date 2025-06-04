

import os
import asyncio
import time
from typing import List
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

import conf
from utils import fetion_alert
from conf.flags import QUE_STATUS
from db import db_engine
from db import sql_params
from k8s.async_v1_api import async_get_nodes_df
from api.query.query_db_schema import SCHEMA
from logm import logger


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


class QueryDB(object):
    UPDATE_QUERY_DATA_INTERVAL = 1
    STEP_SIZE = 10000
    query_engine = create_engine("postgresql://postgres:root@127.0.0.1:5432/query_db", pool_pre_ping=True, pool_size=100)
    a_query_engine = create_async_engine("postgresql+asyncpg://postgres:root@127.0.0.1:5432/query_db", pool_pre_ping=True, pool_size=100)

    @classmethod
    def insert_records(cls, records: list, table_name: str, columns: List[str], conflict_columns: List[str]):
        if len(records) == 0:
            return
        sql_prefix = f"""
        insert into "{table_name}" ({','.join(f'"{c}"' for c in columns)}) values 
        """
        sql_suffix = f"""
        on conflict ({','.join(f'"{c}"' for c in conflict_columns)}) do update set 
        ({','.join(f'"{c}"' for c in columns)}) 
        = 
        ({','.join(f'excluded."{c}"' for c in columns)})
        """
        # 一次最多插入几个
        sub_list_len = 1000
        for sub_records in ([records[i:i+sub_list_len] for i in range(0, len(records), sub_list_len)]):
            sql = sql_prefix + ','.join('(' + ','.join('%s' for _ in record) + ')' for record in sub_records) + sql_suffix
            cls.query_engine.execute(sql, [attr for record in sub_records for attr in record])

    @classmethod
    def update_task(cls, where):
        # todo: 从库的 timezone 不是北京时间，需要统一改一下
        sql = f"""
        set timezone to 'Asia/Shanghai';
        select
            "task_ng"."id",
            "task_ng"."nb_name",
            "task_ng"."user_name",
            "task_ng"."code_file",
            "task_ng"."workspace",
            "task_ng"."config_json"::text,
            "task_ng"."group",
            "task_ng"."nodes",
            array_to_json("task_ng"."assigned_nodes")::text,
            "task_ng"."restart_count",
            "task_ng"."whole_life_state",
            "task_ng"."first_id",
            "task_ng"."backend",
            "task_ng"."task_type",
            "task_ng"."queue_status",
            "task_ng"."notes",
            "task_ng"."priority",
            "task_ng"."chain_id",
            "task_ng"."stop_code",
            "task_ng"."suspend_code",
            "task_ng"."mount_code",
            "task_ng"."suspend_updated_at",
            "task_ng"."begin_at",
            "task_ng"."end_at",
            "task_ng"."created_at",
            "task_ng"."worker_status",
            extract(year from "task_ng"."created_at"),
            extract(month from "task_ng"."created_at"),
            extract(day from "task_ng"."created_at"),
            extract(hour from "task_ng"."created_at"),
            coalesce(json_agg(pod_ng) filter (where "pod_ng"."task_id" is not null), '[]'::json)::text,
            extract(epoch from (current_timestamp - "task_ng"."created_at"))::integer,
            coalesce(extract(epoch from (
                   case
                       when "task_ng"."queue_status" = '{QUE_STATUS.FINISHED}' then "task_ng"."end_at"
                       else current_timestamp end
                   - "task_ng"."begin_at"
               )), 0)::integer,
            coalesce(extract(epoch from (
                   (case
                        when "task_ng"."queue_status" = '{QUE_STATUS.FINISHED}' then "task_ng"."end_at"
                        else current_timestamp end
                       - "task_ng"."begin_at") * "host"."gpu_num" * "task_ng"."nodes"
               )), 0)::integer,
            "host"."room",
            "user"."shared_group",
            "user"."role"
        from "task_ng"
        left join "host" on "task_ng"."assigned_nodes"[1] = "host"."node"
        inner join "user" on "task_ng"."user_name" = "user"."user_name"
        left join "pod_ng" on "pod_ng"."task_id" = "task_ng"."id"
        {where}
        group by "task_ng"."id", "user"."user_id", "host"."node"
        """
        cls.insert_records(
            db_engine.execute(sql).fetchall(),
            'task',
            [
                "id", "nb_name", "user_name", "code_file", "workspace", "config_json", "group", "nodes", "assigned_nodes",
                "restart_count", "whole_life_state", "first_id", "backend", "task_type", "queue_status", "notes",
                "priority", "chain_id", "stop_code", "suspend_code", "mount_code", "suspend_updated_at", "begin_at", "end_at",
                "created_at", "worker_status", "created_year", "created_month", "created_day", "created_hour", "pods", "created_seconds",
                "running_seconds", "gpu_seconds", "schedule_zone", "shared_group", "user_role"
            ],
            ["id"]
        )

    @classmethod
    def update_user(cls):
        sql = """
        select
            "user"."user_id",
            "user"."active",
            "user"."user_name",
            "user"."nick_name",
            "user"."role",
            "user"."shared_group",
            "user"."last_activity",
            "quota"."resource",
            coalesce(
                max("quota") filter (where "user"."user_name" = "quota"."user_name"),
                max("quota") filter (where "user"."user_name" != "quota"."user_name")
            ) as "quota",
            array_to_json("ugs"."user_groups")::text,
            "user"."token"
        from
        "user"
        left join "user_group" on "user_group"."user_name" = "user"."user_name"
        inner join "quota" on "quota"."user_name" in ("user"."user_name", "user"."role"::varchar, "user_group"."group", 'public')
        inner join (
            select
                "user"."user_name",
                array_cat(
                    coalesce(array_agg("user_group"."group") filter (where "user_group"."group" is not null), '{}'),
                    array["user"."shared_group", "user"."role"::varchar, 'public']
                ) as "user_groups"
            from "user"
            left join "user_group" on "user"."user_name" = "user_group"."user_name"
            group by "user"."user_id"
        ) "ugs" on "ugs"."user_name" = "user"."user_name"
        group by "user"."user_id", "quota"."resource", "user"."role", "ugs"."user_groups"
        """
        cls.insert_records(
            db_engine.execute(sql).fetchall(),
            'user',
            ["user_id", "active", "user_name", "nick_name", "role", "shared_group", "last_activity", "resource", "quota", "user_groups", "token"],
            ["user_id", "resource"]
        )

    @classmethod
    def update_resource(cls):
        resource_df = loop.run_until_complete(async_get_nodes_df())
        cls.insert_records(
            [[
                r.NAME, r.mars_group, r.type, r.GPU_NUM, r.STATUS, r.origin_group, r.schedule_zone, r.memory, r.nodes,
                r.cpu, r.use, r.internal_ip, r.CLUSTER, r.group, r.working, r.working_user, r.LEAF, r.SPINE
            ] for _, r in resource_df.iterrows()],
            'resource',
            [
                "name", "mars_group", "type", "gpu_num", "status", "origin_group", "schedule_zone", "memory", "nodes",
                "cpu", "use", "internal_ip", "cluster", "group", "working", "working_user", "leaf", "spine"
            ],
            ["name"]
        )

    @classmethod
    def init_db(cls):
        print('init db', flush=True)
        try:
            res = cls.query_engine.execute('select count(*) from task').fetchall()[0][0]
            if res >= 0:
                print('已经有数据了，update 就行')
                cls.update()
                return
        except Exception as e:
            pass
        print('进行数据库初始化')
        cls.query_engine.execute(SCHEMA)
        # 需要分多次，因为 maxstandby_streaming_delay 有限制
        db_max_id = db_engine.execute('select max("id") from "task_ng"').fetchall()[0][0]
        # 可能数据库没有数据
        if isinstance(db_max_id, int):
            for step in range(int(db_max_id / cls.STEP_SIZE) + 1):
                cls.update_task(f"""where "id" > {step * cls.STEP_SIZE} and "id" <= {(step + 1) * cls.STEP_SIZE} """)

    @classmethod
    def update(cls):
        # 拿这些任务
        # 当前最大 id 之后的
        # 自己认为没有结束的
        current_max_id = cls.query_engine.execute('select max("id") from "task"').fetchall()[0][0]
        current_non_finished_id = cls.query_engine.execute('''select "id" from "task" where "queue_status" != 'finished' ''').fetchall()
        task_where = ''
        if isinstance(current_max_id, int):
            task_where = f""" where "task_ng"."id" > {current_max_id} """
        if len(current_non_finished_id) > 0:
            task_where += f"""
             or "task_ng"."id" in ({','.join(str(i[0]) for i in current_non_finished_id)})
            """
        cls.update_task(task_where)
        cls.update_user()
        cls.update_resource()

    @classmethod
    async def a_execute(cls, sql, args=()):
        sql, args = sql_params.format(sql, args)
        async with QueryDB.a_query_engine.begin() as conn:
            return [{**r} for r in await conn.execute(sqlalchemy.text(sql), args)]


if __name__ == '__main__':
    logger.info('init query db')
    t1 = time.time()
    try:
        QueryDB.init_db()
    except Exception as e:
        msg = f"query db 初始化失败：{e}"
        logger.error(msg)
        fetion_alert(msg, qq=conf.environ.get('IMPORTANT_QQ', '前端'), stdout=False)
        os._exit(1)
    t2 = time.time()
    logger.info('init db finished in ', round(t2 - t1, 2), 'seconds')
    logger.info('start updating')
    count = 0
    while True:
        t1 = time.time()
        logger.info(f'第 {count} 次更新 QueryData', flush=True)
        try:
            QueryDB.update()
        except Exception as e:
            msg = f"query db update 失败：{e}"
            logger.error(msg)
            fetion_alert(msg, qq=conf.environ.get('IMPORTANT_QQ', '前端'), stdout=False)
        logger.info(f'第 {count} 次更新 QueryData 结束，耗时 {round(time.time() - t1, 2)} s', flush=True)
        count += 1
        time.sleep(QueryDB.UPDATE_QUERY_DATA_INTERVAL)
