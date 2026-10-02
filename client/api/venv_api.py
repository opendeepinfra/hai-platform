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


def _base_version(text) -> str:
    '''取版本的基础部分（`1.4.1+e3c42c` → `1.4.1`）：带 git rev 的完整版本几乎总不相同。'''
    return str(text or '').split('+')[0].split('-')[0].strip()


def _local_haienv_version() -> str:
    '''本机 haienv 版本；取不到返回空串（不阻断主流程）。'''
    try:
        from importlib.metadata import version as _pkg_version
        return str(_pkg_version('haienv'))
    except Exception:
        pass
    try:
        import haienv
        return str(getattr(haienv, '__version__', '') or '')
    except Exception:
        return ''


def _version_note(server_version: str) -> str:
    '''
    版本偏移提示（ADR-E4 / CMP-04）。

    注册表里的值是 pickle 的 `haienv.client.model.HaienvConfig`，客户端与服务端必须能
    import 到同一个类。这里只比较**基础版本**（去掉 git rev）并在不一致时提示；
    设 `HAIENV_STRICT_VERSION=1` 时改为直接中止上传。
    '''
    local_version = _local_haienv_version()
    if not server_version or not local_version:
        return ''
    if _base_version(server_version) == _base_version(local_version):
        return ''
    return (f'（注意：集群侧 haienv {server_version} 与本机 {local_version} 基础版本不同，'
            f'若任务里出现环境读不到/反序列化报错，请对齐两侧 haienv 版本）')


def _build_push_cmd(venv_name, local_path, remote_path, provider, force, no_checksum,
                    no_zip, no_diff, list_timeout, sync_timeout, cloud_connect_timeout,
                    token_expires, part_mb_size, proxy):
    '''
    `remote_path` 必须是**对象存储的 key 前缀**（形如 `{group}/shared/hfai_envs/{user}/{dir}`），
    而不是集群文件系统路径 —— 客户端上传时用它拼对象 key（`workspace_util.upload_files`），
    服务端则用同一个 `get_base_path(..., FileType.ENV)` 推导该前缀去下载。
    '''
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
    try:
        pre_result = await async_requests(RequestMethod.POST, pre_url, assert_success=[0, 1])
    except Exception as e:
        # 二级回滚（移除两条路由注册）或服务端版本过旧时，这里会拿到 404：
        # `async_requests` 对没有 success 字段的响应体抛异常，必须翻译成可读结论（RB-02）。
        return {
            'success': 0,
            'msg': f'预检失败：{e}（若为「接口不存在 / Not Found」，说明集群服务端还没上线 env push —— '
                   f'发布顺序必须是「先服务端，后客户端」）'
        }
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
    # C-6：`--env_remote_path` 必须是**对象存储 key 前缀**（cloud_path），不是集群文件系统路径。
    # 传集群路径会把对象写到 `nfs-shared/...` 这种错误 key 下，服务端 stage2 必然 404。
    upload_prefix = pre_result.get('cloud_path') or ''
    if not upload_prefix:
        return {
            'success': 0,
            'msg': '预检失败：集群服务端未返回对象存储前缀（cloud_path），无法确定上传位置；'
                   '请升级集群服务端到包含该字段的版本后再试'
        }

    # CMP-04：服务端返回集群侧 haienv 版本，版本基础号不一致时提示（严格模式下直接中止）
    server_version = str(pre_result.get('haienv_version') or '')
    note = _version_note(server_version)
    if note and os.environ.get('HAIENV_STRICT_VERSION') == '1':
        return {
            'success': 0,
            'msg': f'集群侧与本机 haienv 版本不一致，已按 HAIENV_STRICT_VERSION=1 中止上传{note}'
        }

    # ---------------------------------------------------------------- ② 上传
    push_cmd = _build_push_cmd(venv_name, item.path, upload_prefix, provider, force, no_checksum,
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
    try:
        reg_result = await async_requests(
            RequestMethod.POST, f'{mars_url()}/ugc/register_cluster_venv?token={mars_token()}',
            assert_success=[0, 1], data=json.dumps(body))
    except Exception as e:
        # 上传已经成功，注册这一步的异常必须落回「已上传未注册」这一档（可重试），而不是抛栈
        return {
            'success': 0,
            'msg': f'环境已上传但注册失败，可重试：env push {venv_name}'
                   f'（原因：{e}；已上传的文件不会回滚）{note}'
        }
    if reg_result.get('success') != 1:
        return {
            'success': 0,
            'msg': f'环境已上传但注册失败，可重试：env push {venv_name}'
                   f'（原因：{reg_result.get("msg") or reg_result}；已上传的文件不会回滚）{note}'
        }

    return {
        'success': 1,
        'msg': f'上传并注册成功，可用 source haienv {venv_name}{note}'
    }
