import asyncclick as click
import re
import sys
import os
from .api import create_haienv, list_haienv, remove_haienv
from rich import box
from rich.console import Console
from rich.table import Table
from rich.box import ASCII2
from .model import get_db_path, get_path_prefix, Haienv, check_user_name
import getpass
import json


# 支持的 CUDA 版本：默认放行 **CUDA 11.x 的全部小版本**（11.0 ~ 11.9，含 11.5）。
# 可用环境变量 HAIENV_CUDA_VERSION_RE 覆盖，例如：
#   HAIENV_CUDA_VERSION_RE='^11\.(1|3|5)$'    # 收紧
#   HAIENV_CUDA_VERSION_RE='^1[12]\.\d+$'      # 放宽到 12.x
DEFAULT_CUDA_VERSION_RE = r'^11\.\d+$'
NVCC_CMD = '/usr/local/cuda/bin/nvcc -V 2>/dev/null || nvcc -V 2>/dev/null'


def get_cuda_version(nvcc_output: str = None) -> str:
    '''
    从 `nvcc -V` 输出里取 CUDA release 版本号（如 '11.5'）；取不到返回 ''。

    nvcc 输出形如：`Cuda compilation tools, release 11.5, V11.5.119`
    '''
    if nvcc_output is None:
        nvcc_output = os.popen(NVCC_CMD).read()
    match = re.search(r'release\s+(\d+\.\d+)', nvcc_output or '')
    return match.group(1) if match else ''


def check_cuda_version(nvcc_output: str = None) -> str:
    '''
    校验容器内的 CUDA 版本（`haienv create` 的前置检查）。

    默认要求 CUDA **11.x**（含 11.5）；不满足时抛 AssertionError 并给出可操作提示。
    '''
    output = os.popen(NVCC_CMD).read() if nvcc_output is None else nvcc_output
    assert output, '未找到/usr/local/cuda/bin/nvcc 以及 nvcc，请设置环境变量PATH'
    version = get_cuda_version(output)
    pattern = os.environ.get('HAIENV_CUDA_VERSION_RE') or DEFAULT_CUDA_VERSION_RE
    assert version and re.match(pattern, version), (
        f'目前 haienv 支持 CUDA 11.x（含 11.5），当前检测到 CUDA {version or "未知"}；'
        f'如确需其它版本，可设置环境变量 HAIENV_CUDA_VERSION_RE 覆盖当前规则 {pattern}')
    return version


class HandleHfaiGroupArgs(click.Group):
    def format_usage(self, ctx, formatter):
        pieces = ['COMMAND', '<argument>...', '[OPTIONS]']
        formatter.write_usage(ctx.command_path, " ".join(pieces))


class HaienvHandleHfaiCommandArgs(click.Command):
    def format_options(self, ctx, formatter):
        with formatter.section("Arguments"):
            formatter.write_dl(rows=[('haienv_name', 'haienv的名字')])
        super(HaienvHandleHfaiCommandArgs, self).format_options(ctx, formatter)


@click.command(cls=HaienvHandleHfaiCommandArgs)
@click.argument('haienv_name', required=True, metavar='haienv_name')
@click.option('--no_extend', required=False, is_flag=True, default=False, help='扩展当前python环境（默认为扩展），注意扩展当前环境极有可能出现版本兼容问题')
@click.option('-p', '--py', default=os.environ.get('HAIENV_PY', '.'.join([str(i) for i in sys.version_info[:3]])), help='选择python版本，默认为当前python版本')
@click.option('--extra-search-dir', required=False, type=str, multiple=True, help='指定在进入该虚拟环境时额外的pythonpath')
@click.option('--extra-search-bin-dir', required=False, type=str, multiple=True, help='指定在进入该虚拟环境时额外的path')
@click.option('--extra-environment', required=False, type=str, multiple=True, help='指定在进入该虚拟环境时额外的环境变量')
async def create(haienv_name, no_extend, py, extra_search_dir, extra_search_bin_dir, extra_environment):
    """
    使用conda创建新的虚拟环境，注意必须有conda并配置好相应代理（如有需要）

    eg. haienv create my_env --no_extend --py 3.6 --extra-search-dir /tmp/123 --extra-search-dir /tmp/456 --extra-environment TEMP=temp
    """
    print(f"当前虚拟环境目录为{get_path_prefix()}，如需更改请设置环境变量HAIENV_PATH", flush=True)
    assert os.popen('uname').read().strip() == 'Linux', 'haienv只支持Linux环境'
    # 默认支持 CUDA 11.x（含 11.5），可用 HAIENV_CUDA_VERSION_RE 覆盖
    check_cuda_version()
    result = await create_haienv(haienv_name=haienv_name, extend=('False' if no_extend else 'True'), py=py, extra_search_dir=extra_search_dir, extra_search_bin_dir=extra_search_bin_dir, extra_environment=extra_environment)
    print(result['msg'])


@click.command(cls=HaienvHandleHfaiCommandArgs)
@click.argument('haienv_name', required=True, metavar='haienv_name')
@click.option('--force', required=False, is_flag=True, default=False, help='是否强制推送并覆盖集群侧同名文件, 默认值为False')
@click.option('-n', '--no_checksum', required=False, is_flag=True, default=False, help='是否对文件禁用checksum比对, 默认值为False')
@click.option('-z', '--no_zip', required=False, is_flag=True, default=False, help='是否禁用打包上传, 默认值为False')
@click.option('-d', '--no_diff', required=False, is_flag=True, default=False, help='是否禁用差量上传, 默认值为False')
@click.option('-l', '--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='遍历集群目录的超时时间, 单位(s)')
@click.option('-s', '--sync_timeout', required=False, is_flag=False, type=click.IntRange(5, 21600), default=1800, show_default=True, help='等待同步任务提交成功的超时时间, 单位(s)')
@click.option('-o', '--cloud_connect_timeout', required=False, is_flag=False, type=click.IntRange(60, 43200), default=120, show_default=True, help='从本地上传分片到云端的连接超时时间, 单位(s)')
@click.option('-t', '--token_expires', required=False, is_flag=False, type=click.IntRange(900, 43200), default=1800, show_default=True, help='从本地上传到云端的sts token有效时间, 单位(s)')
@click.option('-m', '--part_mb_size', required=False, is_flag=False, type=click.IntRange(10, 10240), default=100, show_default=True, help='从本地上传到云端的分片大小, 单位(MB)')
@click.option('--provider', required=False, is_flag=False, default='', help='云端存储 provider, 默认取 $CLOUD_STORAGE_PROVIDER 或 oss')
@click.option('--proxy', required=False, is_flag=False, default='', help='从本地上传到云端时使用的代理url')
async def push(haienv_name, force, no_checksum, no_zip, no_diff, list_timeout, sync_timeout,
               cloud_connect_timeout, token_expires, part_mb_size, provider, proxy):
    """
    把本地虚拟环境推送到集群（仅支持非 extend 环境）

    eg. haienv push my_env
    """
    from hfai.client.api.venv_api import push_venv
    result = await push_venv(venv_name=haienv_name, force=force, no_checksum=no_checksum,
                             no_zip=no_zip, no_diff=no_diff, list_timeout=list_timeout,
                             sync_timeout=sync_timeout, cloud_connect_timeout=cloud_connect_timeout,
                             token_expires=token_expires, part_mb_size=part_mb_size,
                             provider=provider, proxy=proxy)
    print(result.get('msg', ''), flush=True)
    if not result.get('success'):
        sys.exit(1)


@click.command(cls=HaienvHandleHfaiCommandArgs)
@click.option('-u', '--user', help='指定用户，默认为所有用户')
@click.option('-a', '--all', 'show_all', required=False, is_flag=True, default=False, help='列出所有环境')
@click.option('-o', 'output_format', default='', help='输出格式，可以选择json')
async def list(user, show_all, output_format=''):
    """
    列举所有虚拟环境
    """
    assert output_format in ['', 'json'], '目前输出格式只支持json'
    try:  # SEC-06 / E9：-u 会被拼进路径，先校验再使用
        check_user_name(user)
    except ValueError as e:
        print(f'参数错误：{e}')
        sys.exit(1)
    root_path = os.path.realpath(os.path.join(get_db_path(), '../..'))
    all_result = []
    for _user in sorted(os.listdir(root_path)):
        if user is not None and user != _user:
            continue
        result = await list_haienv(_user)
        if len(result) == 0:
            continue
        for k, v in result.items():
            all_result.append((_user, k, v.path, v.extend, v.extend_env, v.py))
    result_dict = {'others': [], 'own': []}
    haienv_table = Table(title='其它环境', title_justify=True, box=ASCII2, style='dim', show_header=True)
    haienv_own_table = Table(title='自己创建的环境', title_justify=True, box=ASCII2, style='dim', show_header=True)
    if user is not None:
        haienv_table = Table(show_header=True, box=box.ASCII_DOUBLE_HEAD)
    haienv_key_list = ['user', 'haienv_name', 'path', 'extend', 'extend_env', 'py']
    for k in haienv_key_list:
        haienv_table.add_column(k)
        haienv_own_table.add_column(k)
    for item in all_result:
        if user is not None or item[0] != getpass.getuser():
            tp = 'others'
            haienv_table.add_row(*item)
        else:
            tp = 'own'
            haienv_own_table.add_row(*item)
        result_dict[tp].append({key: value for key, value in zip(haienv_key_list, item)})
    if output_format == '':
        print(f"当前虚拟环境目录为{get_path_prefix()}，如需更改请设置环境变量HAIENV_PATH", flush=True)
        console = Console()
        console.print(f'请在 bash 中使用以下命令加载 env: source haienv <haienv_name>')
        console.print(haienv_table)
        if user is None:
            console.print(haienv_own_table)
    if output_format == 'json':
        if user is not None:
            result_dict.pop('own', None)
        print(json.dumps(result_dict))


@click.command(cls=HaienvHandleHfaiCommandArgs)
@click.argument('haienv_name', required=True, metavar='haienv_name')
async def remove(haienv_name):
    """
    删除虚拟环境
    """
    result = await remove_haienv(haienv_name=haienv_name)
    print(result['msg'])


@click.group(cls=HandleHfaiGroupArgs)
async def config():
    """
    创建、查询、删除虚拟环境
    """
    pass


@config.command(cls=HaienvHandleHfaiCommandArgs)
@click.option('-n', '--haienv_name', required=True, help='haienv_name')
@click.option('-u', '--user', help='指定用户，默认走当前用户')
async def show(haienv_name, user):
    """
    展示指定haienv的各项参数
    """
    try:  # SEC-06 / E9
        check_user_name(user)
    except ValueError as e:
        print(f'参数错误：{e}')
        sys.exit(1)
    root_path = os.path.realpath(os.path.join(get_db_path(), f'../../{user}/venv.db')) if user is not None else get_db_path()
    haienv_config = Haienv.select(haienv_name=haienv_name, outside_db_path=root_path)
    assert haienv_config is not None, f'未找到该环境，当前虚拟环境目录为{get_path_prefix()}，请通过haienv list查看所有环境，或设置环境变量HAIENV_PATH进行更改'
    config_table = Table(show_header=True, box=box.ASCII_DOUBLE_HEAD)
    for k in ['item', 'content']:
        config_table.add_column(k)
    config_table.add_row('haienv_name', haienv_name)
    for item in ['path', 'extend', 'extend_env', 'py', 'extra-search-dir', 'extra-search-bin-dir', 'extra-environment']:
        config_table.add_row(item, f'{getattr(haienv_config, item.replace("-", "_"))}')
    console = Console()
    console.print(config_table)


@config.command(cls=HaienvHandleHfaiCommandArgs)
@click.option('-n', '--haienv_name', required=True, help='haienv_name')
@click.option('-k', '--key', required=True, help='选择清除的参数，目前只能指定extra-search-dir, extra-search-bin-dir, extra-environment中的一种')
async def clear(haienv_name, key):
    """
    清除haienv的某项参数

    eg. haienv clear -n my_env -k extra-search-dir
    """
    assert key in ['extra-search-dir', 'extra-search-bin-dir', 'extra-environment']
    key = key.replace('-', '_')
    assert Haienv.select(haienv_name=haienv_name) is not None, f'未找到该环境，当前虚拟环境目录为{get_path_prefix()}，请通过haienv list查看所有环境，或设置环境变量HAIENV_PATH进行更改'
    Haienv.update(haienv_name=haienv_name, key=key, value=[])
    print('设置成功, 目前的参数如下：')
    await show.callback(haienv_name=haienv_name, user=None)
    print(f'重新 source haienv {haienv_name} 生效该参数')


@config.command(cls=HaienvHandleHfaiCommandArgs)
@click.option('-n', '--haienv_name', required=True, help='haienv_name')
@click.option('-k', '--key', required=True, help='选择追加的参数，目前只能指定extra-search-dir, extra-search-bin-dir, extra-environment中的一种')
@click.option('-v', '--value', required=True, help='追加的参数值')
async def append(haienv_name, key, value):
    """
    追加haienv的某项参数

    eg. haienv clear -n my_env -k extra-search-dir -v /tmp/123
    """
    assert key in ['extra-search-dir', 'extra-search-bin-dir', 'extra-environment']
    key = key.replace('-', '_')
    haienv_config = Haienv.select(haienv_name=haienv_name)
    assert haienv_config is not None, f'未找到该环境，当前虚拟环境目录为{get_path_prefix()}，请通过haienv list查看所有环境，或设置环境变量HAIENV_PATH进行更改'
    Haienv.update(haienv_name=haienv_name, key=key, value=getattr(haienv_config, key) + [value])
    print('设置成功, 目前的参数如下：')
    await show.callback(haienv_name=haienv_name, user=None)
    print(f'重新 source haienv {haienv_name} 生效该参数')
