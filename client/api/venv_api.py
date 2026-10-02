'''
`hai-cli env push` 的客户端实现 —— 设计 docs/haiplatform/env/env-server-design.md §6.2 / §6.3。

修复的历史缺陷：
  E3/F2  `--file_type {FileType.ENV}` 被 f-string 插值成字面量 'FileType.ENV' → 一律取 `.value`
  E13    子进程命令用 `sys.argv[0]` 拼接（插件进程下是 /usr/local/bin/haienv）→ 显式解析 haiworkspace
  E7     未校验 API-11 返回的 path（None 时直接拼进命令行）→ 明确失败，不抛 KeyError
  E4     上传成功后不写集群侧注册表 → push 成功后调用 API-13

分级结果（FR-06 / NFR-06）：
  上传失败              → success=0 '上传失败:<原因>'
  上传成功 + 注册失败   → success=0 '环境已上传但注册失败，可重试：env push X'
  全部成功              → success=1 '上传并注册成功，可用 source haienv X'
'''

import json
import os
import shlex
import shutil
import sysconfig

from .api_config import get_mars_url as mars_url
from .api_config import get_mars_token as mars_token
from .api_utils import async_requests, RequestMethod
from hfai.conf.utils import FileType
from haienv.client.model import Haienv


def _resolve_workspace_bin() -> str:
    '''
    解析 haiworkspace 可执行文件（修 E13）。

    顺序：$HAI_WORKSPACE_BIN → PATH → PLUGIN_LIST → sysconfig scripts → /usr/local/bin。
    绝不再用 sys.argv[0]（插件进程下那是 /usr/local/bin/haienv，会拼出非法子命令）。
    '''
    candidates = []
    env_bin = os.environ.get('HAI_WORKSPACE_BIN')
    if env_bin:
        candidates.append(env_bin)
    which = shutil.which('haiworkspace')
    if which:
        candidates.append(which)
    try:
        from hfai.client.commands.utils import PLUGIN_LIST
        candidates.append(PLUGIN_LIST.get('haiworkspace'))
    except Exception:
        pass
    try:
        candidates.append(os.path.join(sysconfig.get_path('scripts'), 'haiworkspace'))
    except Exception:
        pass
    candidates.append('/usr/local/bin/haiworkspace')
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return next((c for c in candidates if c), 'haiworkspace')


def _build_push_cmd(venv_name, local_path, remote_path, provider, force, no_checksum,
                    no_zip, no_diff, list_timeout, sync_timeout, cloud_connect_timeout,
                    token_expires, part_mb_size, proxy):
    workspace_bin = _resolve_workspace_bin()
    cmd = [workspace_bin]
    # `hai-cli workspace push` 会派发成 `haiworkspace push`（插件自身就是子命令集合），
    # 因此只有落到主 CLI 时才需要补一个 `workspace` 词，否则会得到
    # "Error: No such command 'workspace'"。
    if os.path.basename(workspace_bin) in ('hai-cli', 'hfai', 'hfai_cli.py'):
        cmd.append('workspace')
    cmd += [
        'push',
        '--file_type', FileType.ENV.value,          # E3/F2：必须是字面量 'env'
        '--env_provider', provider,
        '--env_local_path', local_path,
        '--env_remote_path', remote_path,
        '--list_timeout', str(list_timeout),
        '--sync_timeout', str(sync_timeout),
        '--cloud_connect_timeout', str(cloud_connect_timeout),
        '--token_expires', str(token_expires),
        '--part_mb_size', str(part_mb_size),
    ]
    if force:
        cmd.append('--force')
    if no_checksum:
        cmd.append('--no_checksum')
    if no_zip:
        cmd.append('--no_zip')
    if no_diff:
        cmd.append('--no_diff')
    if proxy:
        cmd.extend(['--proxy', proxy])
    return ' '.join(shlex.quote(str(item)) for item in cmd)


async def push_venv(venv_name, force=False, no_checksum=False, no_zip=False, no_diff=False,
                    list_timeout=300, sync_timeout=1800, cloud_connect_timeout=120,
                    token_expires=1800, part_mb_size=100, provider='', proxy=''):
    '''
    把本地 haienv 推送到集群（① API-11 预检 → ② workspace push → ③ API-13 注册）。

    :return dict: {'success': 0/1, 'msg': <可读结果>}
    '''
    item = Haienv.select(venv_name)
    if not item:
        return {
            'success': 0,
            'msg': f'未找到名为{venv_name}的虚拟环境，当前虚拟环境目录为'
                   f'{os.environ.get("HAIENV_PATH", os.environ.get("HOME", ""))}，'
                   f'请用 haienv list 查看所有可用的虚拟环境，如需更改请设置环境变量 HAIENV_PATH'
        }
    if item.extend == 'True':
        return {
            'success': 0,
            'msg': f'名为{venv_name}的虚拟环境为extend模式，暂不支持上传extend模式的venv'
        }

    provider = provider or os.environ.get('CLOUD_STORAGE_PROVIDER', 'oss')

    # 设计 §4.4 修复路径 ①：服务端需要写 {user_env_dir}/venv.db，而该目录通常由用户以
    # 755 创建。这里尽力把它放宽到 777（只影响权限位，不改语义）；失败不回滚、不影响主流程，
    # 由服务端返回 ENV_REGISTRY_NOT_WRITABLE 时再走运维处置。
    try:
        os.chmod(os.path.dirname(item.path), 0o777)
    except Exception:
        pass

    # ---------------------------------------------------------------- ① API-11 预检
    pre_url = (f'{mars_url()}/ugc/update_cluster_venv?token={mars_token()}'
               f'&venv_name={venv_name}&py={item.py or ""}')
    pre_result = await async_requests(RequestMethod.POST, pre_url, assert_success=[0, 1])
    if pre_result.get('success') != 1:
        return {
            'success': 0,
            'msg': f'预检失败：{pre_result.get("msg") or pre_result}'
                   f'（若为接口不存在，请升级集群服务端到包含 env push 的版本）'
        }
    remote_path = pre_result.get('path')          # E7：必须显式校验
    if not remote_path:
        return {
            'success': 0,
            'msg': '预检失败：集群服务端未返回 env 落盘路径（path 为空），已中止上传，请联系管理员'
        }

    # ---------------------------------------------------------------- ② 上传
    push_cmd = _build_push_cmd(venv_name, item.path, remote_path, provider, force, no_checksum,
                               no_zip, no_diff, list_timeout, sync_timeout, cloud_connect_timeout,
                               token_expires, part_mb_size, proxy)
    if os.system(push_cmd):
        return {
            'success': 0,
            'msg': f'上传失败：`{push_cmd}` 退出码非 0，请检查网络/对象存储配置后重试'
        }

    # ---------------------------------------------------------------- ③ API-13 注册
    body = {
        'venv_name': venv_name,
        'path': remote_path,
        'py': item.py or '',
        'extra_search_dir': list(getattr(item, 'extra_search_dir', []) or []),
        'extra_search_bin_dir': list(getattr(item, 'extra_search_bin_dir', []) or []),
        'extra_environment': list(getattr(item, 'extra_environment', []) or []),
    }
    reg_result = await async_requests(
        RequestMethod.POST, f'{mars_url()}/ugc/register_cluster_venv?token={mars_token()}',
        assert_success=[0, 1], data=json.dumps(body))
    if reg_result.get('success') != 1:
        return {
            'success': 0,
            'msg': f'环境已上传但注册失败，可重试：env push {venv_name}'
                   f'（原因：{reg_result.get("msg") or reg_result}；已上传的文件不会回滚）'
        }

    return {
        'success': 1,
        'msg': f'上传并注册成功，可用 source haienv {venv_name}'
    }
