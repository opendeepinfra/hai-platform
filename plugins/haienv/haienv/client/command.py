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


# ---------------------------------------------------------------------------
# `haienv create` 的前置提示（默认**只告警不阻断**）
#
# 设计取舍（见 docs/haiplatform/env/env-server-test-report.md C-8 与方案分析）：
#   * CUDA 版本**不参与**环境内容生成：conda prefix 里不记录任何 CUDA 信息，
#     真正决定「任务里能否跑」的是任务容器映像 + 驱动 + 环境里安装的 wheel。
#     因此它只适合做提示，不适合做硬门禁（硬门禁会误拦：一台机器上 apt 的
#     nvidia-cuda-toolkit(11.5) 与 /usr/local/cuda(12.9) 并存时，只探测后者就会误判；
#     平台自建镜像里甚至根本没有 nvcc）。
#   * 需要硬门禁的部署可设置 HAIENV_CUDA_STRICT=1。
# ---------------------------------------------------------------------------

# 平台基线：生产镜像基于 nvcr.io/nvidia/cuda:11.3.0-devel（更早为 11.1），见 one/release.sh:31
CLUSTER_CUDA_BASELINE = '11.x'
DEFAULT_CUDA_VERSION_RE = r'^11\.\d+$'
# 依次探测这些 nvcc（顺序只影响展示顺序）：**任一**满足受支持版本即视为匹配。
NVCC_CANDIDATES = ('nvcc', '/usr/local/cuda/bin/nvcc')

# 集群基础环境的 python（平台镜像为 3.8.10；可用 HAIENV_CLUSTER_PY 覆盖）
CLUSTER_BASE_PY = os.environ.get('HAIENV_CLUSTER_PY', '3.8')
# 平台镜像/开发容器特征：命中任一即认为「在平台环境内」
PLATFORM_ENV_MARKERS = (
    '/marsv2/scripts/pip_conf.yaml',
    '/marsv2/scripts',
    '/high-flyer/code/multi_gpu_runner_server',
)
_TRUE_VALUES = ('1', 'true', 'yes', 'y', 'on')


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name, '')).strip().lower() in _TRUE_VALUES


def _warn(message: str):
    print(f'\033[1;33m WARNING: \033[0m{message}', flush=True)


def _major_minor(version: str) -> str:
    match = re.match(r'\s*(\d+)\.(\d+)', str(version or ''))
    return f'{match.group(1)}.{match.group(2)}' if match else ''


def get_cuda_version(nvcc_output: str = None) -> str:
    '''
    从 `nvcc -V` 输出里取 CUDA release 版本号（如 '11.5'）；取不到返回 ''。

    nvcc 输出形如：`Cuda compilation tools, release 11.5, V11.5.119`
    '''
    match = re.search(r'release\s+(\d+\.\d+)', nvcc_output or '')
    return match.group(1) if match else ''


def collect_nvcc_reports(candidates=None) -> dict:
    '''返回 {nvcc 路径: `nvcc -V` 输出}，只保留有输出的候选。'''
    reports = {}
    for candidate in (candidates or NVCC_CANDIDATES):
        output = os.popen(f'{candidate} -V 2>/dev/null').read()
        if output and output.strip():
            reports[candidate] = output
    return reports


def check_cuda_version(nvcc_output: str = None, strict: bool = None) -> dict:
    '''
    `haienv create` 的 CUDA **提示**（默认不阻断）。

    - 依次探测 `NVCC_CANDIDATES`（PATH 中的 `nvcc` 与 `/usr/local/cuda/bin/nvcc`），
      **任一**命中受支持范围即视为匹配（解决「一台机器多个 nvcc」的误判）；
    - 默认受支持范围 CUDA `11.x`（含 11.5），可用 `HAIENV_CUDA_VERSION_RE` 覆盖；
    - 不匹配/未检测到时：默认打印 WARNING 并继续；`HAIENV_CUDA_STRICT=1`
      （或显式 `strict=True`）时才抛 AssertionError。

    :return: `{'ok', 'strict', 'pattern', 'versions', 'matched'}`
    '''
    pattern = os.environ.get('HAIENV_CUDA_VERSION_RE') or DEFAULT_CUDA_VERSION_RE
    if strict is None:
        strict = _env_truthy('HAIENV_CUDA_STRICT')

    if nvcc_output is not None:
        reports = {'(给定输出)': nvcc_output}
    else:
        reports = collect_nvcc_reports()

    versions = {candidate: get_cuda_version(output) for candidate, output in reports.items()}
    matched = next((v for v in versions.values() if v and re.match(pattern, v)), None)
    result = {'ok': bool(matched), 'strict': bool(strict), 'pattern': pattern,
              'versions': versions, 'matched': matched}

    if matched:
        print(f'CUDA 提示：检测到 CUDA {matched}（基线 {CLUSTER_CUDA_BASELINE}），在受支持范围内', flush=True)
        return result

    if versions:
        detail = '、'.join(f'{candidate}: {version or "无法解析"}' for candidate, version in versions.items())
        head = f'检测到的 nvcc 均不在平台基线 {CLUSTER_CUDA_BASELINE} 内：[{detail}]'
    else:
        detail = ''
        head = f'未检测到 nvcc（已尝试 {", ".join(NVCC_CANDIDATES)}）'
    tail = (f'CUDA 版本不影响 conda 环境本身，但若之后要安装 CUDA 相关 wheel（torch/cupy 等），'
            f'可能与集群运行时/驱动不匹配；如确需调整判定范围可设置 HAIENV_CUDA_VERSION_RE（当前 {pattern}）')
    if strict:
        raise AssertionError(f'{head}；已启用 HAIENV_CUDA_STRICT=1，拒绝创建。{tail}')
    _warn(f'{head}；{tail}；如需强制拦截请设置 HAIENV_CUDA_STRICT=1')
    return result


def check_python_version(py: str, strict: bool = None) -> dict:
    '''
    提示「目标 python 版本」与集群基础环境（默认 3.8）是否一致。

    conda 环境默认取**当前解释器**的版本，在裸机（如 python 3.10）上建出的环境，
    在集群 3.8 的任务里很可能不可用 —— 这是比 CUDA 更常见的真实坑。
    '''
    want, base = _major_minor(py), _major_minor(CLUSTER_BASE_PY)
    result = {'ok': (not want) or want == base, 'py': py, 'want': want,
              'cluster_base': base, 'strict': False}
    if not want or want == base:
        return result
    _warn(f'目标 python 版本 {py} 与集群基础环境 python {CLUSTER_BASE_PY} 不一致；'
          f'该环境在集群任务里可能不可用，建议加 `-p {CLUSTER_BASE_PY}`（或用 HAIENV_CLUSTER_PY 调整基线）')
    return result


def in_platform_env() -> bool:
    '''是否在平台镜像/开发容器内（判断 `extend` 语义是否成立）。'''
    return bool(os.environ.get('TASK_NAME')) or any(os.path.exists(p) for p in PLATFORM_ENV_MARKERS)


def check_extend_policy(extend: bool, in_platform: bool = None) -> dict:
    '''
    提示 `extend`（默认开启）在**非平台环境**下的语义问题。

    `extend=True` 会继承「当前 python 环境」：在平台镜像/开发容器里继承的是平台基础环境（符合预期），
    但在裸机上继承的是**本机** python，生成的环境在集群里通常不可用；此时应使用 `--no_extend`。
    '''
    if in_platform is None:
        in_platform = in_platform_env()
    result = {'ok': not extend or bool(in_platform), 'extend': bool(extend),
              'in_platform': bool(in_platform)}
    if extend and not in_platform:
        _warn('当前不在平台镜像/开发容器内，但未指定 `--no_extend`：extend 会继承**本机** python 环境，'
              '生成的环境在集群任务里大概率不可用；建议加 `--no_extend`')
    return result


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
    # 前置检查一律「提示不阻断」（HAIENV_CUDA_STRICT=1 可让 CUDA 检查变成硬门禁）：
    #   ① CUDA：与平台基线 11.x 的差异只影响之后安装的 CUDA 相关 wheel，不影响 conda 环境本身
    #   ② python：默认取当前解释器版本，裸机上常与集群基础环境 3.8 不一致
    #   ③ extend：默认开启，在非平台环境里会继承本机 python（语义不对）
    check_cuda_version()
    check_python_version(py)
    check_extend_policy(extend=not no_extend)
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
