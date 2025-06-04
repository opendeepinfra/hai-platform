import asyncclick as click
from rich.console import Console
from .utils import HandleHfaiCommandArgs
from hfai.client.api.monitor_api import get_tasks_overview, get_cluster_overview
from hfai.client.api.storage_api import get_user_personal_stroage
from hfai.client.api.user_api import get_worker_user_info
from rich import box
from rich.table import Table
from rich.tree import Tree
from collections import defaultdict
from hfai.conf.flags import EXP_PRIORITY
from hfai.conf.utils import bytes_to_human

@click.group()
def monitor():
    """
    获取当前任务列表相关信息
    """

def task_table(df):
    resource_table = Table(show_header=True, box=box.SQUARE_DOUBLE_HEAD)
    resource_columns = ['priority', 'queue_status', 'count']
    for k in resource_columns:
        resource_table.add_column(k)

    for _, row in df.astype(str).iterrows():
        values = []
        for k in resource_columns:
            if k == 'priority':
                values.append(EXP_PRIORITY.get_name_by_value(int(row[k])))
            else:
                values.append(row[k])
        resource_table.add_row(*values)
    return resource_table


@click.command(cls=HandleHfaiCommandArgs)
async def task_overview():
    """
    查看当前集群任务概况
    """
    console = Console()
    overview_data = await get_tasks_overview(data_type='df')
    console.print(task_table(overview_data.rename(columns=str.lower)))


def cluster_table(overview_data):
    resource_table = Table(show_header=True, box=box.SQUARE_DOUBLE_HEAD)
    resource_columns = ['Status', 'Number']
    for k in resource_columns:
        resource_table.add_column(k)

    resource_table.add_row(*['Total', str(overview_data['total'])])
    resource_table.add_row(*['Working', str(overview_data['working'])])
    resource_table.add_row(*['Free', str(overview_data['free'])])
    resource_table.add_row(*['Others', str(overview_data['other'])])
    return resource_table


@click.command(cls=HandleHfaiCommandArgs)
async def cluster_overview():
    """
    查看当前集群节点概况
    """
    console = Console()

    overview_data = await get_cluster_overview()
    usage_rate = overview_data['usage_rate']
    usage_rate_text = f"{round(usage_rate * 100, 2)} %"
    if usage_rate < 0.85:
        console.print(f'Worker Usage Ratio: [green] {usage_rate_text} [/green]')
    else:
        console.print(f'Worker Usage Ratio: [red] {usage_rate_text} [/red]')
    console.print('Overview:')
    console.print(cluster_table(overview_data))


@click.command(cls=HandleHfaiCommandArgs)
async def storage_overview():
    """
    查看 storage 状态
    """
    storage_data = await get_user_personal_stroage()
    console = Console()
    storage_table = Table(show_header=True, box=box.ASCII_DOUBLE_HEAD)
    for k in ['Path', 'ReadOnly', 'Type', 'Quota (Used / Total)']:
        storage_table.add_column(k)
    for item in storage_data:
        path = item.get('mount_path', '')
        read_only = str(item.get('read_only', ''))
        tp = item.get('mount_type', '')
        quota = item.get('quota', {})
        quota = f'{bytes_to_human(quota.get("used_bytes", "-"))} / {bytes_to_human(quota.get("limit_bytes", "-"))}'
        storage_table.add_row(*[path, read_only, tp, quota])
    console = Console()
    console.print('Overview:')
    console.print(storage_table)


@click.command(cls=HandleHfaiCommandArgs)
async def quota_overview():
    """
    查看 quota
    """
    all_quota = (await get_worker_user_info()).get('all_quota', {})
    used_quota = (await get_worker_user_info()).get('already_used_quota', {})
    quota_dict = defaultdict(lambda: defaultdict(lambda: {'used': 0, 'total': 0}))
    for name, quota in all_quota.items():
        try:
            group, priority = name.split('-')
            quota_dict[group][priority]['total'] = quota
        except:
            pass
    for name, quota in used_quota.items():
        try:
            group, priority = name.split('-')
            quota_dict[group][priority]['used'] = quota
        except:
            pass
    console = Console()
    tree = Tree('Overview: (Used / Total)')
    for group, group_value in quota_dict.items():
        g = tree.add(f'[red] {group} [/red]')
        for priority in ['EXTREME_HIGH', 'VERY_HIGH', 'HIGH', 'ABOVE_NORMAL', 'NORMAL', 'LOW']:
            if priority in group_value:
                g.add(f'{priority}: {group_value[priority]["used"]} / {group_value[priority]["total"]}')
    console.print(tree)


monitor.add_command(task_overview)
monitor.add_command(cluster_overview)
monitor.add_command(storage_overview)
monitor.add_command(quota_overview)
