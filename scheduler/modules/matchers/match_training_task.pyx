# @Time    : 2022/6/8 9:15
# @Author  : Wentao Zhang
# @Email   : wt.zhang@high-flyer.cn


import conf
import pandas as pd
from collections import defaultdict
from scheduler.base_model import modify_task_df_safely, SCHEDULER_RESULT, PROCESS_RESULT
try:
    from conf.flags import CHAIN_STATUS, UPGRADE_FAILED_VERSION, QUE_STATUS
except Exception as e:
    print(e)


MATCH_BY = 'LEAF'


def get_match_task_func():
    # task_ids_to_interrupt = set()
    def match_task(resource_df: pd.DataFrame, task_df: pd.DataFrame, valid: bool):
        # nonlocal task_ids_to_interrupt
        # 记录 node 的 room
        node_room_series = resource_df.room.copy()
        node_room_series.index = resource_df.NAME
        node_room_mapping = node_room_series.to_dict()
        all_rooms = set(resource_df.room)
        all_users = set(task_df.user_name)
        # 只要 Ready 的节点
        resource_df = resource_df[
            (resource_df.STATUS == 'Ready') &
            (resource_df.nodes == 1) &
            ((resource_df.working == 'not_working') | (resource_df.working == 'training'))
            ].copy()
        # 运行态任务需要一个 dict 来查找对应的 room
        task_room_mapping = {}
        # 用户目前在各个 room 的占用情况
        user_room_nodes = defaultdict(lambda: defaultdict(int))
        for _, task in task_df[task_df.queue_status == QUE_STATUS.SCHEDULED].iterrows():
            room = 'NO_ROOM'
            if len(task.assigned_nodes) > 0:
                room = node_room_mapping.get(task.assigned_nodes[0], 'NO_ROOM')
            user_room_nodes[task.user_name][room] += task.nodes
            task_room_mapping[task.id] = room
        # 记录各个 room 需要分配节点（要启动）的任务
        room_startup_tasks = defaultdict(list)
        # 记录各个 room 各个 group 的剩余节点
        room_group_rest_nodes = resource_df.groupby('room').NAME.count().to_dict()
        # 记录各个 room 最大剩余节点数
        max_rest_room_modes = max(room_group_rest_nodes.values()) if len(room_group_rest_nodes.values()) > 0 else 0
        # 所有 quota 内的任务都拿出来，按照 match_rank 依次 match
        tmp_task_df = task_df[task_df.scheduler_result == SCHEDULER_RESULT.CAN_RUN] \
            .sort_values('match_rank')\
            .sort_values('schedule_zone', key=lambda s: s.apply(lambda v: v is None), kind='stable')\
            .sort_values('priority', kind='stable', ascending=False)\
            .copy()
        resource_dict = {n: 1 for n in resource_df.NAME}
        while len(tmp_task_df) > 0:
            task = tmp_task_df.iloc[0]
            tmp_task_df = tmp_task_df[1:]
            startup = True
            # 用户指定要调度的 zone
            schedule_zone = task.schedule_zone
            if task.queue_status == QUE_STATUS.SCHEDULED:
                # 在运行的任务，分配原有节点，把资源删掉
                current_assigned_resource = {n: resource_dict[n] for n in task.assigned_nodes if n in resource_dict}
                task_room = task_room_mapping[task.id] if schedule_zone is None else schedule_zone
                # 要的节点不在可分配节点中
                no_such_nodes = len(current_assigned_resource) != task.nodes
                # 要的节点，被分配给了其他人的情况
                lack_nodes = set(current_assigned_resource.values()) != {1} or room_group_rest_nodes.get(task_room, 0) < task.nodes
                # room 变化了
                room_changed = {node_room_mapping.get(n) for n in task.assigned_nodes} != {task_room}
                if not (no_such_nodes or lack_nodes or room_changed):
                    room_group_rest_nodes[task_room] -= task.nodes
                    for n in task.assigned_nodes:
                        resource_dict[n] -= 1
                    task_df.loc[task.id, 'process_result'] = PROCESS_RESULT.KEEP_RUNNING
                    continue
                else:
                    # 不能分配原有节点，room 的占用分布需要更新
                    user_room_nodes[task.user_name][task_room] -= task.nodes
                    # 这些任务是要打断的，这里直接记录一下
                    startup = False
                    if lack_nodes:
                        # 节点够，但这个任务的原有节点被分走了
                        if max_rest_room_modes >= task.nodes:
                            task_df.loc[task.id, ['scheduler_result', 'process_result', 'scheduler_msg']] = \
                                [SCHEDULER_RESULT.CAN_NOT_RUN, PROCESS_RESULT.SUSPEND, '原有节点被分配给其他任务了，打断重启']
                        # 超卖的任务，分到这里节点已经不够了
                        else:
                            task_df.loc[task.id, ['scheduler_result', 'process_result', 'scheduler_msg']] = \
                                [SCHEDULER_RESULT.CAN_NOT_RUN, PROCESS_RESULT.SUSPEND, '没权利继续运行了']
                    elif room_changed:
                        task_df.loc[task.id, ['scheduler_result', 'process_result', 'scheduler_msg']] = \
                            [SCHEDULER_RESULT.CAN_NOT_RUN, PROCESS_RESULT.SUSPEND, '用户切换了 schedule_zone']
                    elif no_such_nodes:
                        task_df.loc[task.id, ['scheduler_result', 'process_result', 'scheduler_msg']] = \
                            [
                                SCHEDULER_RESULT.NODE_ERROR,
                                PROCESS_RESULT.SUSPEND,
                                f'因为节点 {set(task.assigned_nodes) - set(current_assigned_resource.keys())} 无法分配，即将被打断'
                            ]
            # 分配不了原有节点的运行态任务、要启动的任务，当作要启动的任务，从这个用户占用最少的 room 开始分配
            schedule_zones = all_rooms if schedule_zone is None else all_rooms & {schedule_zone}
            for room in sorted(schedule_zones, key=lambda r: (user_room_nodes[task.user_name][r], -room_group_rest_nodes.get(r, 0))):
                if room_group_rest_nodes.get(room, 0) >= task.nodes:
                    # 这个 room 可以分配，把资源删掉，更新 room 占用分布
                    if startup:
                        room_startup_tasks[room].append(task)
                    room_group_rest_nodes[room] -= task.nodes
                    user_room_nodes[task.user_name][room] += task.nodes
                    break
            else:
                # 用户没指定 zone 才考虑剔除任务
                if schedule_zone is None:
                    # 怎么都没有办法运行这个任务
                    if task.match_rank & (15 << 19) != 0:
                        # 如果不是权利数内的任务，这个用户的后续任务就都不考虑了
                        tmp_task_df = tmp_task_df[tmp_task_df.user_name != task.user_name].sort_values('match_rank')
                    else:
                        # 如果是权利数内的任务，这个用户的后续任务节点数小于自己的依然考虑
                        tmp_task_df = tmp_task_df[(tmp_task_df.user_name != task.user_name) | (tmp_task_df.nodes < task.nodes)].sort_values('match_rank')
                else:
                    # 用户指定的 zone 分配不出节点
                    task_df.loc[task.id, 'scheduler_msg'] = '用户指定的 schedule_zone 分配不出节点'
                # 如果所有的任务节点数都大于等于最多剩余的 room，就不用继续调度了
                if len(tmp_task_df) == len(tmp_task_df[tmp_task_df.nodes > max_rest_room_modes]):
                    break
        # 给要启动的任务分配节点，分配之前把要继续运行的节点删了
        resource_df.loc[resource_df.NAME.isin([k for k, v in resource_dict.items() if v == 0]), 'nodes'] = 0
        for room, room_tasks in room_startup_tasks.items():
            for task in room_tasks:
                this_task_resource_df = resource_df[(resource_df.room == room) & (resource_df.group == task.group) & (resource_df.nodes == 1)]
                if len(this_task_resource_df) >= task.nodes:
                    try:
                        # 节点够用，可以分配，先尝试从 spine 分配
                        match_by_node_count = this_task_resource_df.groupby(MATCH_BY).NAME.count().sort_values()
                        required_nodes = task.nodes
                        match_bys = []
                        while True:
                            more_than_required = match_by_node_count[match_by_node_count >= required_nodes]
                            if len(more_than_required):
                                # 所需节点数量可以满足，选择满足的 spine 中节点最少的那个
                                match_bys.append(more_than_required.index[0])
                                break
                            else:
                                # 所需节点数不能直接满足，选择所有 spine 中节点最多的那个
                                required_nodes -= match_by_node_count[-1]
                                match_bys.append(match_by_node_count.index[-1])
                            # 把选过的删了
                            match_by_node_count = match_by_node_count[~match_by_node_count.index.isin(match_bys)]
                        this_task_assigned_nodes = this_task_resource_df[this_task_resource_df[MATCH_BY].isin(match_bys)][0:task.nodes]
                        assert len(this_task_assigned_nodes) == task.nodes, "按照 spine 分节点的逻辑出现了问题"
                    except:
                        # 出现了异常，选择随机分配
                        this_task_assigned_nodes = this_task_resource_df[0:task.nodes]
                    task_df.loc[task.id, 'process_result'] = PROCESS_RESULT.STARTUP
                    task_df = modify_task_df_safely(
                        task_df,
                        task_id=task.id,
                        assigned_nodes=this_task_assigned_nodes.NAME.to_list(),
                        memory=(this_task_assigned_nodes.memory - (23 << 30)).to_list(),
                        cpu=[0] * len(this_task_assigned_nodes) if conf.environ.get('DEBUG', '0') == '1' else (
                                    this_task_assigned_nodes.cpu - 10).to_list(),
                        assigned_gpus=[[i for i in range(gpus)] for gpus in this_task_assigned_nodes.GPU_NUM.to_list()]
                    )
                    resource_df.loc[resource_df.NAME.isin(this_task_assigned_nodes.NAME), 'nodes'] = 0
                else:
                    print('出错了，分配的时候节点不够了')
        # 还没有操作的任务都是节点不够了，在运行的就打断，不在运行的就继续排队
        task_df.loc[(task_df.process_result == PROCESS_RESULT.NOT_SURE) & (task_df.queue_status == QUE_STATUS.SCHEDULED),
                    ['process_result', 'scheduler_msg']] = [PROCESS_RESULT.SUSPEND, '没权利继续运行了']
        task_df.loc[(task_df.process_result == PROCESS_RESULT.NOT_SURE) & (task_df.queue_status != QUE_STATUS.SCHEDULED),
                    ['process_result', 'scheduler_msg']] = [PROCESS_RESULT.DO_NOTHING, '继续排队']
        # # 在最后把之前发出过打断的任务都标记为打断
        # task_df.loc[task_df.index.isin(task_ids_to_interrupt), ['process_result', 'scheduler_result', 'scheduler_msg']] = \
        #     [PROCESS_RESULT.SUSPEND, SCHEDULER_RESULT.CAN_NOT_RUN, '之前发起了打断的，继续发送打断指令']
        # # 如果这次是有效的 match，更新打断表
        # if valid:
        #     task_ids_to_interrupt |= set(task_df[task_df.process_result == PROCESS_RESULT.SUSPEND].id)
        return resource_df, task_df
    return match_task
