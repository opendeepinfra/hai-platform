import asyncclick as click
from .utils import HandleHfaiGroupArgs, HandleHfaiCommandArgs
from hfai.client.api import workspace_api


@click.group(cls=HandleHfaiGroupArgs)
def workspace():
    """
    构造工作区，把本地代码推送到萤火二号上跑
    """
    pass


class WorkspaceHandleHfaiCommandArgs(HandleHfaiCommandArgs):
    def format_options(self, ctx, formatter):
        pieces = self.collect_usage_pieces(ctx)
        with formatter.section("Arguments"):
            if 'workspace_name' in pieces:
                formatter.write_dl(rows=[('workspace_name', '工作区的名字')])
            if 'remote_path' in pieces:
                formatter.write_dl(rows=[('remote_path', '远端文件路径，远端目录请通过diff获取，如 checkpoint/model.pt，默认: checkpoint')])
        super(WorkspaceHandleHfaiCommandArgs, self).format_options(ctx, formatter)


@workspace.command(cls=WorkspaceHandleHfaiCommandArgs)
@click.argument('workspace_name', required=True, metavar='workspace_name')
async def init(workspace_name):
    """
    初始化本地工作区
    """
    try:
        await workspace_api.init(workspace_name)
    except Exception as e:
        print(f'初始化失败，错误信息：{e}')
        return


@workspace.command(cls=HandleHfaiCommandArgs)
@click.option('-f', '--force', required=False, is_flag=True, default=False, help='是否强制推送并覆盖远端目录, 默认值为False')
@click.option('-n', '--no_checksum', required=False, is_flag=True, default=False, help='是否对文件禁用checksum, 默认值为False')
@click.option('-z', '--no_zip', required=False, is_flag=True, default=False, help='是否禁用workspace打包上传, 默认值为False')
@click.option('-d', '--no_diff', required=False, is_flag=True, default=False, help='是否禁用差量上传, 如是, 本地和远端不一致文件将被强制覆盖, 默认值为False')
@click.option('-l', '--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='遍历集群工作区的超时时间, 单位(s)')
@click.option('-s', '--sync_timeout', required=False, is_flag=False, type=click.IntRange(5, 21600), default=1800, show_default=True, help='从oss同步到集群的超时时间, 单位(s)')
@click.option('-t', '--token_expires', required=False, is_flag=False, type=click.IntRange(900, 21600), default=1800, show_default=True, help='从本地上传到oss的sts token有效时间, 单位(s)')
async def push(force, no_checksum, no_zip, no_diff, list_timeout, sync_timeout, token_expires):
    """
    推送本地workspace到萤火二号
    """
    try:
        pushed = await workspace_api.push(force=force, no_checksum=no_checksum, no_zip=no_zip, no_diff=no_diff, list_timeout=list_timeout, sync_timeout=sync_timeout, token_expires=token_expires)
        if not pushed:
            print('推送失败，请稍后重试，或者联系管理员...')
        else:
            print('推送成功')
    except Exception as e:
        print(f'推送失败，错误信息：{e}')
        return


@workspace.command(cls=WorkspaceHandleHfaiCommandArgs)
@click.option('-f', '--force', required=False, is_flag=True, default=False, help='是否强制覆盖本地目录，默认值为False')
@click.option('-n', '--no_checksum', required=False, is_flag=True, default=False, help='是否对文件禁用checksum，默认值为False')
@click.option('-l', '--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='遍历集群工作区的超时时间, 单位(s)')
@click.option('-s', '--sync_timeout', required=False, is_flag=False, type=click.IntRange(5, 21600), default=1800, show_default=True, help='从集群同步到oss的超时时间, 单位(s)')
@click.option('-t', '--token_expires', required=False, is_flag=False, type=click.IntRange(900, 21600), default=1800, show_default=True, help='从oss下载到本地的sts token有效时间, 单位(s)')
async def pull(force, no_checksum, list_timeout, sync_timeout, token_expires):
    """
    下载萤火二号目录到本地workspace
    """
    try:
        pulled = await workspace_api.pull(force=force, no_checksum=no_checksum, list_timeout=list_timeout, sync_timeout=sync_timeout, token_expires=token_expires)
        if not pulled:
            print('下载失败，请稍后重试，或者联系管理员...')
        else:
            print('下载成功')
    except Exception as e:
        print(f'下载失败，错误信息：{e}')
        return


@workspace.command(cls=WorkspaceHandleHfaiCommandArgs)
@click.argument('remote_path', required=True, default='checkpoint', metavar='remote_path')
@click.option('-f', '--force', required=False, is_flag=True, default=False, help='是否强制覆盖本地目录，默认值为False')
@click.option('-n', '--no_checksum', required=False, is_flag=True, default=False, help='是否对文件禁用checksum，默认值为False')
@click.option('-l', '--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='遍历集群工作区的超时时间，单位(s)')
@click.option('-s', '--sync_timeout', required=False, is_flag=False, type=click.IntRange(5, 21600), default=1800, show_default=True, help='从集群同步到oss的超时时间, 单位(s)')
@click.option('-t', '--token_expires', required=False, is_flag=False, type=click.IntRange(900, 21600), default=1800, show_default=True, help='从oss下载到本地的sts token有效时间, 单位(s)')
async def download(remote_path, force, no_checksum, list_timeout, sync_timeout, token_expires):
    """
    下载萤火二号目录中指定文件到本地，
    远端目录请通过diff获取，如 checkpoint/model.pt
    """
    if remote_path == '':
        print('请指定远端文件路径')
        return
    try:
        pulled = await workspace_api.pull(force=force, no_checksum=no_checksum, subpath=remote_path, list_timeout=list_timeout, sync_timeout=sync_timeout, token_expires=token_expires)
        if not pulled:
            print('下载失败，请稍后重试，或者联系管理员...')
        else:
            print('下载成功')
    except Exception as e:
        print(f'下载失败，错误信息：{e}')
        return


@workspace.command(cls=WorkspaceHandleHfaiCommandArgs)
@click.option('-n', '--no_checksum', required=False, is_flag=True, default=False, help='是否对文件禁用checksum，默认值为False')
@click.option('-l', '--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='遍历集群工作区的超时时间，单位(s)')
async def diff(no_checksum, list_timeout):
    """
    比较本地workspace和萤火二号目录diff，默认比较文件md5\n
    如耗时较长，可通过'--no_checksum'参数禁用md5计算，只通过文件size比较，该方法不可靠
    """
    try:
        await workspace_api.diff(no_checksum, list_timeout)
    except Exception as e:
        print(f'diff失败，错误信息：{e}')
        return


@workspace.command(cls=WorkspaceHandleHfaiCommandArgs)
async def list():
    """
    列举所有萤火二号workspace，加粗表示为当前运行目录所在workspace
    """
    try:
        await workspace_api.list()
    except Exception as e:
        print(f'list失败，错误信息：{e}')
        return


@workspace.command(cls=WorkspaceHandleHfaiCommandArgs)
@click.argument('workspace_name', required=True, metavar='workspace_name')
@click.confirmation_option(prompt='请确认是否删除 workspace, 此操作将删除集群侧工作目录且无法复原！')
async def remove(workspace_name):
    """
    删除集群工作区
    """
    if not workspace_name:
        print("请输入工作区名字")
        return
    try:
        await workspace_api.delete(workspace_name)
        print(f'remove 完成')
    except Exception as e:
        print(f'remove 失败，错误信息：{e}')
        return
