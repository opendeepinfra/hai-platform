import getpass
import os

import asyncclick as click
from .utils import HandleHfaiCommandArgs, HandleHfaiGroupArgs
from hfai.client.api.venv_api import create_venv, list_venv, remove_venv, push_venv
from rich import box
from rich.console import Console
from rich.table import Table
import sys
from hfai.base_model.venv import get_db_path


@click.group(cls=HandleHfaiGroupArgs)
async def venv():
    """
    创建、查询、删除虚拟环境
    """
    pass


class VenvHandleHfaiCommandArgs(HandleHfaiCommandArgs):
    def format_options(self, ctx, formatter):
        with formatter.section("Arguments"):
            formatter.write_dl(rows=[('venv_name', 'venv的名字')])
        super(VenvHandleHfaiCommandArgs, self).format_options(ctx, formatter)


@venv.command(cls=VenvHandleHfaiCommandArgs)
@click.argument('venv_name', required=True, metavar='venv_name')
@click.option('--no_extend', required=False, is_flag=True, default=False, help='扩展当前python环境（默认为扩展），注意扩展当前环境极有可能出现版本兼容问题')
@click.option('-p', '--py', default='.'.join([str(i) for i in sys.version_info[:3]]), help='选择python版本，默认为当前python版本')
async def create(venv_name, no_extend, py):
    """
    使用conda创建新的虚拟环境，注意必须有conda并配置好相应代理（如有需要）
    """
    result = await create_venv(venv_name=venv_name, extend=('False' if no_extend else 'True'), py=py)
    print(result['msg'])


@venv.command(cls=HandleHfaiCommandArgs)
@click.option('-u', '--user', help='指定用户，默认为所有用户')
async def list(user):
    """
    列举所有虚拟环境
    """
    root_path = os.path.realpath(os.path.join(get_db_path(), '../..'))
    all_result = []
    for _user in sorted(os.listdir(root_path)):
        if user is not None and user != _user:
            continue
        result = await list_venv(_user)
        if len(result) == 0:
            continue
        all_result += [(_user, *r) for r in result]
    venv_table = Table(show_header=True, box=box.ASCII_DOUBLE_HEAD)
    for k in ['user', 'venv_name', 'path', 'extend', 'extend_env', 'py']:
        venv_table.add_column(k)
    for item in all_result:
        venv_table.add_row(*item)
    console = Console()
    console.print(f'请在 bash 中使用以下命令加载 hfai_env: source hfai_env <venv_name>')
    console.print(venv_table)


@venv.command(cls=VenvHandleHfaiCommandArgs)
@click.argument('venv_name', required=True, metavar='venv_name')
async def remove(venv_name):
    """
    删除虚拟环境
    """
    result = await remove_venv(venv_name=venv_name)
    print(result['msg'])


@venv.command(cls=VenvHandleHfaiCommandArgs)
@click.argument('venv_name', required=True, metavar='venv_name')
@click.option('-f', '--force', required=False, is_flag=True, default=False, help='是否强制推送并覆盖远端目录，默认值为False')
@click.option('-n', '--no_checksum', required=False, is_flag=True, default=False, help='是否对文件禁用checksum，默认值为False')
@click.option('-z', '--no_zip', required=False, is_flag=True, default=False, help='是否禁用venv打包上传，默认值为False')
@click.option('-d', '--no_diff', required=False, is_flag=True, default=False, help='是否禁用差量上传，如是，本地和远端不一致文件将被强制覆盖，默认值为False')
@click.option('-l', '--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='遍历集群工作区的超时时间，单位(s)')
@click.option('-s', '--sync_timeout', required=False, is_flag=False, type=click.IntRange(5, 21600), default=1800, show_default=True, help='从oss同步到集群的超时时间, 单位(s)')
@click.option('-t', '--token_expires', required=False, is_flag=False, type=click.IntRange(900, 21600), default=1800, show_default=True, help='从本地上传到oss的sts token有效时间, 单位(s)')
async def push(venv_name, force, no_checksum, no_zip, no_diff, list_timeout, sync_timeout, token_expires):
    """
    上传虚拟环境
    """
    result = await push_venv(venv_name=venv_name, force=force, no_checksum=no_checksum, no_zip=no_zip, no_diff=no_diff, list_timeout=list_timeout, sync_timeout=sync_timeout, token_expires=token_expires)
    print(result['msg'])
