
import munch

from .storage_api import get_user_group
from .workspace_util import *
from hfai.conf.utils import FileType

current_hf_run_path = os.getcwd()
workspace_config_file = './.hfai/workspace.yml'


def update_current_hf_run_path(new_path):
    global current_hf_run_path
    current_hf_run_path = new_path


async def init(workspace=None):
    """
    这里的设计思路，会和 git 一样
        1. python xxx.py 的时候，会一层一层找到 workspace
        2. 如果有两个同名的 workspace，会覆盖，这边我们是不处理这个逻辑的
        3. 用户可以把本地多个文件夹都作为一个 workspace
    @param str workspace: 本地工作区路径
    @return:
    """

    os.makedirs(os.path.join(current_hf_run_path, './.hfai'), exist_ok=True)
    wcf = os.path.join(current_hf_run_path, workspace_config_file)
    if os.path.exists(wcf):
        wc = munch.Munch.fromYAML(open(wcf))
        if wc.workspace == workspace:
            print('已经创建了这个 workspace')
        else:
            print(f'当前目录已经配置为 workspace: {wc.workspace}，请确认')
        return

    print('查询用户 group 信息...')
    user_info = await get_user_group()
    cwd = current_hf_run_path
    # 转换windows路径格式为unix格式
    drive, cwd = os.path.splitdrive(cwd)
    if drive != '':
        cwd = cwd.replace('\\', '/')

    if not workspace:
        workspace = os.path.basename(cwd)
    exclude_path = ['/ceph-jd', '/weka-jd', '/opt/hf_venvs', '/hf_shared']
    if any(cwd.startswith(p) for p in exclude_path):
        print('检测到是在萤火集群的代码, 忽略本次操作，请在集群外使用 workspace 功能')
        return

    provider = 'oss'
    remote_path = f'{user_info["user_shared_group"]}/{user_info["user_name"]}/workspaces/{workspace}'
    workspace_config = munch.Munch.fromDict({
        "workspace": workspace,
        'local': cwd,
        'remote': remote_path,
    })

    await set_sync_status(FileType.WORKSPACE, workspace, SyncDirection.PUSH, SyncStatus.INIT, cwd, remote_path)
    with open(os.path.join(current_hf_run_path, workspace_config_file),
              encoding='utf8', mode='w') as f:
        f.write(workspace_config.toYAML())
    print(
        f'初始化 workspace [{workspace_config.local}]->[{provider}://{workspace_config.remote}] 成功')


def get_workspace_conf():
    """
    获取这个目录的 workspace 配置文件，返回 workspace config 的文件
    @return: workspace_config_file
    """
    cwd = current_hf_run_path
    subs = []
    while cwd != '/':
        wcf = os.path.join(cwd, workspace_config_file)
        if os.path.exists(wcf):
            return wcf, subs
        subs.insert(0, os.path.basename(cwd))
        cwd = os.path.dirname(cwd)
    return None, None


async def get_wc_with_check():
    wcf, _ = get_workspace_conf()
    if not wcf:
        print('workspace未初始化')
        return None
    wc = munch.Munch.fromYAML(open(wcf))
    result = await get_sync_status(FileType.WORKSPACE, wc.workspace)
    if len(result) == 0:
        print(f"没找到工作区 {wc.workspace}")
        return None
    return wc


async def push(force: bool = False, no_checksum: bool = False, no_zip: bool = False, no_diff: bool = False, list_timeout: int = 300, sync_timeout: int = 300, token_expires: int = 1800):
    """
    推送本地workspace到集群
    @param force: 是否强制推送
    @param no_checksum: 是否禁用checksum
    @return bool: 标识push是否成功
    """
    # 一层一层往上面找，看看有没有 ./hfai/workspace.yml
    wc = await get_wc_with_check()
    if wc is None:
        return False
    return await push_to_cluster(wc.local, wc.remote, wc.workspace, FileType.WORKSPACE, force=force,
        no_checksum=no_checksum, no_zip=no_zip, no_diff=no_diff, list_timeout=list_timeout, sync_timeout=sync_timeout, token_expires=token_expires)


async def pull(force: bool = False, no_checksum: bool = False, subpath: str = '', list_timeout: int = 300, sync_timeout: int = 300, token_expires: int = 1800):
    """
    从集群下载workspace数据到本地
    @param force: 是否覆盖本地文件
    @param no_checksum: 是否禁用checksum
    @param subpath: pull的子目录
    @return bool: 标识pull是否成功
    """
    # 一层一层往上面找，看看有没有 ./hfai/workspace.yml
    wc = await get_wc_with_check()
    if wc is None:
        return False
    while subpath.startswith('.' + os.path.sep):  # 为了在ignore的时候不出问题
        subpath = subpath[len('.' + os.path.sep):]
    return await pull_from_cluster(wc.local, wc.remote, wc.workspace, FileType.WORKSPACE, force=force,
        no_checksum=no_checksum, subpath=subpath, list_timeout=list_timeout, sync_timeout=sync_timeout, token_expires=token_expires)


async def diff(no_checksum: bool = False, list_timeout: int = 3600):
    """
    比较本地和集群目录的diff
    @param no_checksum: 是否禁用checksum
    """
    wc = await get_wc_with_check()
    if wc is None:
        return False
    local_only_files, cluster_only_files, _, changed_files = await diff_local_cluster(wc.local, wc.workspace, FileType.WORKSPACE, ['./'], no_checksum=no_checksum, timeout=list_timeout)
    print_diff(local_only_files, cluster_only_files, changed_files)


async def list():
    current_ws = ''
    wcf, _ = get_workspace_conf()
    if wcf:
        wc = munch.Munch.fromYAML(open(wcf))
        current_ws = wc.workspace

    result = await get_sync_status(FileType.WORKSPACE)
    console = Console()
    table = Table(box=ASCII2, style='dim', show_header=True)
    for column in ['workspace', 'local_path', 'cluster_path', 'push status', 'last push', 'pull status', 'last pull']:
        table.add_column(column)
    for f in result:
        table.add_row(f['name'], f['local_path'], f['cluster_path'], f['push_status'], f['last_push'], f['pull_status'], f['last_pull'], 
            style=('bold' if f['name'] == current_ws else None), end_section=True)
    console.print(table)


async def delete(workspace_name):
    current_ws = ''
    wcf, _ = get_workspace_conf()
    if wcf:
        wc = munch.Munch.fromYAML(open(wcf))
        current_ws = wc.workspace

    result = await get_sync_status(FileType.WORKSPACE, workspace_name)
    if len(result) == 0:
        print(f"没找到工作区 {workspace_name}")
        return
    await delete_workspace(workspace_name)
    if current_ws == workspace_name:
        os.remove(wcf)
    else:
        print(f'请手动删除本地工作区{workspace_name}目录下 .hfai/workspace.yml 文件！')
