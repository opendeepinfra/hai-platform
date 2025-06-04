

import datetime

from server_model.user import User
from .query_db import QueryDB


def replace_unsafe_char(s: str):
    return s.replace(',', '').replace('-', '').replace('"', '').replace("'", '').replace(' ', '')


# 过滤函数，返回一个 tuple (sql, args)
FILTER_FUNC = {
    'eq': lambda *args: (f' "{replace_unsafe_char(args[0])}" = %s ', (args[1], )),
    'ne': lambda *args: (f' "{replace_unsafe_char(args[0])}" != %s ', (args[1], )),
    'gt': lambda *args: (f' "{replace_unsafe_char(args[0])}" > %s ', (args[1], )),
    'ge': lambda *args: (f' "{replace_unsafe_char(args[0])}" >= %s ', (args[1], )),
    'lt': lambda *args: (f' "{replace_unsafe_char(args[0])}" < %s ', (args[1], )),
    'le': lambda *args: (f' "{replace_unsafe_char(args[0])}" <= %s ', (args[1], )),
    'in': lambda *args: (f''' "{replace_unsafe_char(args[0])}" in ({','.join('%s' for _ in args[1])}) ''', (args[1], )),
    'not_in': lambda *args: (f''' "{replace_unsafe_char(args[0])}" not in ({','.join('%s' for _ in args[1])}) ''', (args[1], )),
    'like': lambda *args: (f' "{replace_unsafe_char(args[0])}" like %s ', (args[1], )),
    'ilike': lambda *args: (f' "{replace_unsafe_char(args[0])}" ilike %s ', (args[1], )),
    'not_like': lambda *args: (f' "{replace_unsafe_char(args[0])}" not like %s ', (args[1], )),
    'not_ilike': lambda *args: (f' "{replace_unsafe_char(args[0])}" not ilike %s ', (args[1], )),
    'is_null': lambda *args: (f' "{replace_unsafe_char(args[0])}" is null ', ()),
    'is_not_null': lambda *args: (f' "{replace_unsafe_char(args[0])}" is not null ', ()),
    'match': lambda *args: (f' "{replace_unsafe_char(args[0])}" ~ %s ', (args[1], )),
    '&=': lambda *args: (f' "{replace_unsafe_char(args[0])}" & %s = %s ', (args[1], args[1])),
    'time_range': lambda *args: (
        f' "{replace_unsafe_char(args[0])}" >= %s and "{replace_unsafe_char(args[0])}" <= %s ',
        (datetime.datetime.fromisoformat(args[1]), datetime.datetime.fromisoformat(args[2]))
    )
}


# 非聚合函数
CALC_FUNC = {}


AGG_FUNC = {
    'array_agg': lambda *args: (
        f' array_agg("{replace_unsafe_char(args[0])}" '
        f'''order by {','.join(f'"{replace_unsafe_char(_field)}"' for _field in args[1])} {replace_unsafe_char(args[2])}) ''',
        ()
    ),
    'sum': lambda *args: (f' sum("{replace_unsafe_char(args[0])}") ', ()),
    'max': lambda *args: (f' max("{replace_unsafe_char(args[0])}") ', ()),
    'min': lambda *args: (f' min("{replace_unsafe_char(args[0])}") ', ()),
    'count': lambda *args: (f' count("{replace_unsafe_char(args[0])}") ', ()),
    'distinct_count': lambda *args: (f' count(distinct "{replace_unsafe_char(args[0])}") ', ()),
    'nth': lambda *args: (
        f' (array_agg("{replace_unsafe_char(args[0])}" '
        f'''order by {','.join(f'"{replace_unsafe_char(_field)}"' for _field in args[2])} {replace_unsafe_char(args[3])}))'''
        f'[{int(args[1])}] ',
        ()
    ),
    'last_by_id': lambda *args: (
        f' (array_agg("{replace_unsafe_char(args[0])}" order by "id" desc))[1] ',
        ()
    ),
    'last_not_empty_by_id': lambda *args: (
        f' (array_agg("{replace_unsafe_char(args[0])}" order by "id" desc) '
        f'''filter (where "{replace_unsafe_char(args[0])}" != '[]' and "{replace_unsafe_char(args[0])}" != '{{}}' and "{replace_unsafe_char(args[0])}" is not null))'''
        '[1] ',
        ()
    ),
}


def to_sql(query_schema, table_name):
    groupby_fields = []
    fields = {}
    sql = "select "
    args = ()
    # 处理字段
    assert len(query_schema.get('fields', {})) > 0, "至少要指定一个字段"
    # 是否为聚合查询
    is_agg_query = False
    for field_name, field_config in query_schema['fields'].items():
        assert isinstance(field_config, str) or isinstance(field_config, list), 'field 内容只能是 str 或 list'
        # case "source" as 'field_name'
        if isinstance(field_config, str):
            field = f' "{replace_unsafe_char(field_config)}" '
            fields[field_name] = [field, ()]
            groupby_fields.append(field_name)
        # case func("source") as 'field_name'
        elif isinstance(field_config, list):
            field_func = field_config[0]
            if field_func in AGG_FUNC:
                is_agg_query = True
                field_func = AGG_FUNC[field_func]
            elif field_func in CALC_FUNC:
                field_func = CALC_FUNC[field_func]
                groupby_fields.append(field_name)
            else:
                raise Exception(f"不存在的 function：{field_func}")
            field, field_args = field_func(*field_config[1:])
            fields[field_name] = [field, field_args]
    sql += ' , '.join(f' {fields[field_name][0]} as "{replace_unsafe_char(field_name)}" ' for field_name in fields)
    for field_name in fields:
        args += fields[field_name][1]
    if not is_agg_query:
        groupby_fields = []
    # table name
    sql += f' from "{table_name}" '
    # 处理 filters
    if len(query_schema.get('filters', [])) > 0:
        sql += " where "
        filters = []
        for filter_config in query_schema['filters']:
            sub_filters = []
            for sub_filter_config in filter_config:
                assert sub_filter_config[0] in FILTER_FUNC, f'不存在的 filter 函数：{sub_filter_config[0]}'
                sub_filter, sub_filter_args = FILTER_FUNC[sub_filter_config[0]](*sub_filter_config[1:])
                sub_filters.append(sub_filter)
                args += sub_filter_args
            filters.append('(' + ' or '.join(sub_filters) + ')')
        sql += ' and '.join(filters)
    # 处理 group by
    if is_agg_query:
        sql += " group by " + ' , '.join(fields[field_name][0] for field_name in groupby_fields)
        for field_name in groupby_fields:
            args += fields[field_name][1]
    # 处理 order by
    if query_schema.get('order') is not None:
        sql += " order by " + ' , '.join(fields[field_name][0] for field_name in query_schema['order']['by'])
        if not query_schema['order'].get('ascending', True):
            sql += ' desc '
    # limit offset
    if query_schema.get('limit') is not None:
        sql += f' limit {int(query_schema["limit"])} '
    if query_schema.get('offset') is not None:
        sql += f' offset {int(query_schema["offset"])} '
    return sql, args


async def query(query_body, user: User = None):
    """
    query body 是一个 dict，以下是 yaml 示例
    ```yaml
    task:
        fields:                                        # 不能留空，必须指定需要的字段
            field_1: source / ["func", "arg1", ...]    # func 可以是计算函数，也可以是聚合函数，如果有聚合函数，其余非聚合字段自动作为 group by
        filters:                                       # 可以留空，不 filter
            - [["filter_func", "arg1", ...], ...]      # 同级 or
            - [[], ...]                                # 并列的 and
        order:                                         # 可以留空，不 sort
            by: [field1, ...]
            ascending: false                           # 默认 true
        offset: 10                                     # 可以留空，默认 0
        limit: 100                                     # 可以留空，返回全量
    resource: {}                                       # 可以不写，不进行查询
    user: {}                                           # 可以不写，不进行查询
    ```
    example1:
    task:
        fields:
            chain_id: chain_id,                                     # -> "chain_id" as "chain_id"
            first_id: ["min", "id"],                                # -> min("id") as "first_id" 同时因为是一个聚合函数，别的非聚合字段自动进入 group by
            created_at: ["min", "created_at"]                       # -> min("created_at") as "created_at"
            nodes: ["min", "nodes"]                                 # -> min("nodes") as "nodes"
        filters:
            - [["eq", "user_name", "zwt"]]                          # -> "user_name" = 'zwt'
            - [["eq", "task_type", "training"]]                     # -> "task_type" = 'training'
        order:
            by: ["first_id"]
            ascending: false
        offset: 150
        limit: 15
    这个查询等于 按照提交的 id 倒序查任务，从 offset 150 开始拿，拿 15 个
    select
        "chain_id" as "chain_id", min("id") as "first_id", min("created_at") as "created_at", min("nodes") as "nodes"
    from
        "task"
    where
        "user_name" = 'zwt' and "task_type" = 'training'
    group by "chain_id"
    order by min("id") desc
    limit 15 offset 150

    example2:
    task:
        fields:
            chain_id: chain_id,                                     # -> "chain_id" as "chain_id"
            pods: ["array_agg", "pods"],                            # -> "array_agg("pods") as "pods" 同时因为是一个聚合函数，别的非聚合字段自动进入 group by
            last_id: ["max", "id"]                                  # -> max("id") as "last_id"
        filters:
            - [["eq", "chain_id", "TASK_CHAIN_ID"]]                 # -> "chain_id" = 'TASK_CHAIN_ID'
            - [["eq", "user_name", "zwt"]]                          # -> "user_name" = 'zwt'
    这个查询等于 查 TASK_CHAIN_ID 这个 chain_id 的所有 pods
    select
        "chain_id" as "chain_id", array_agg("pods") as "pods", max("id") as "last_id"
    from
        "task"
    where
        "chain_id" = 'TASK_CHAIN_ID' and "user_name" = 'zwt'
    group by "chain_id"
    """
    results = {}
    if query_body.get('task') is not None:
        task_query = query_body['task']
        if user is not None and not user.quota.api_get_all_user:
            task_query['filters'] = task_query.get('filters', []) + [[['eq', 'user_name', user.user_name]]]
        sql, args = to_sql(task_query, 'task')
        results['task'] = QueryDB.a_execute(sql, args)
    if query_body.get('user') is not None:
        user_query = query_body['user']
        if user is not None and not user.quota.api_get_all_user:
            user_query['filters'] = user_query.get('filters', []) + [[['eq', 'user_name', user.user_name]]]
        for field_config in user_query['fields'].values():
            assert 'token' not in field_config, "这个接口不允许查询 token"
        sql, args = to_sql(user_query, 'user')
        results['user'] = QueryDB.a_execute(sql, args)
    if query_body.get('resource') is not None:
        resource_query = query_body['resource']
        sql, args = to_sql(resource_query, 'resource')
        results['resource'] = QueryDB.a_execute(sql, args)
    return results
