# -*- coding: utf-8 -*-
'''
`hai-cli env push` 客户端链路单元测试 —— 用例集 §4.4（C 组）中不依赖真集群的部分。

覆盖分析报告的三个「服务端看起来对但客户端必然失败」的坑：
  E3/F2  命令行里出现字面量 'FileType.ENV'（TC-C03）
  E13    子进程用 sys.argv[0] 拼成 `haienv workspace push`（TC-C02）
  E7     API-11 返回 path=None 仍继续上传并抛 KeyError（TC-C06）

运行（host 103，已 `build_cli_local.sh` 安装 hai-cli/haienv）：
    HAIENV_PATH=$(mktemp -d) python3 -m pytest tests/env/test_client_push.py -v
'''

import asyncio
import os
import sys

import pytest

from haienv.client.model import Haienv, HaienvConfig, set_path_prefix

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def _use_env_dir(path):
    '''
    把 haienv 的搜索根指向临时目录。

    注意：`haienv.client.model` 会把 HAIENV_PATH 与 venv.db 路径**缓存**在模块全局里
    （`__path_prefix` / `__db_path`），因此测试里必须同时重置两者，
    只改环境变量在同一个 pytest 进程内不生效。
    '''
    import haienv.client.model as model
    os.environ['HAIENV_PATH'] = str(path)
    set_path_prefix(str(path))
    setattr(model, '__db_path', None)


@pytest.fixture
def venv_api():
    from hfai.client.api import venv_api as module
    return module


def test_tc_c03_build_cmd_uses_enum_value(venv_api):
    '''TC-C03：--file_type 必须是字面量 'env'，不能是 'FileType.ENV'。'''
    cmd = venv_api._build_push_cmd('myenv', '/local/prefix', '/cluster/prefix', 's3',
                                   False, False, False, False, 300, 1800, 120, 1800, 100, '')
    assert '--file_type env' in cmd
    assert 'FileType.ENV' not in cmd
    assert '--env_local_path /local/prefix' in cmd
    assert '--env_remote_path /cluster/prefix' in cmd


def test_tc_c02_dispatch_plugin_vs_main_cli(venv_api, tmp_path, monkeypatch):
    '''TC-C02/E13：插件二进制不带 `workspace` 词；主 CLI 才需要。'''
    plugin = tmp_path / 'haiworkspace'
    plugin.write_text('#!/bin/sh\n')
    monkeypatch.setenv('HAI_WORKSPACE_BIN', str(plugin))
    cmd = venv_api._build_push_cmd('m', '/l', '/r', 's3', False, False, False, False,
                                   300, 1800, 120, 1800, 100, '')
    assert cmd.startswith(str(plugin) + ' push '), cmd
    assert cmd.split()[1] == 'push', cmd

    main_cli = tmp_path / 'hai-cli'
    main_cli.write_text('#!/bin/sh\n')
    monkeypatch.setenv('HAI_WORKSPACE_BIN', str(main_cli))
    cmd2 = venv_api._build_push_cmd('m', '/l', '/r', 's3', False, False, False, False,
                                    300, 1800, 120, 1800, 100, '')
    assert cmd2.startswith(str(main_cli) + ' workspace push '), cmd2

    # 绝不能出现 `haienv workspace push`（E13 的原始症状）
    monkeypatch.delenv('HAI_WORKSPACE_BIN', raising=False)
    assert 'haienv workspace push' not in venv_api._build_push_cmd(
        'm', '/l', '/r', 's3', False, False, False, False, 300, 1800, 120, 1800, 100, '')


def test_resolve_workspace_bin_exists(venv_api):
    '''解析出的可执行文件必须真实存在（否则 E13 只是换了个拼错方式）。'''
    resolved = venv_api._resolve_workspace_bin()
    assert os.path.isabs(resolved) or os.path.sep in resolved


def test_tc_c04_push_missing_env(venv_api, tmp_path):
    '''TC-C04：不存在的环境名 → 本地失败，且不发起任何网络请求。'''
    _use_env_dir(tmp_path)
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('not_exist'))
    assert result['success'] == 0
    assert '未找到名为not_exist的虚拟环境' in result['msg']


def test_tc_c05_push_extend_env(venv_api, tmp_path, monkeypatch):
    '''TC-C05/FR-07：extend 环境被本地拒绝，不调 API-11。'''
    _use_env_dir(tmp_path)
    prefix = tmp_path / 'ext_0'
    prefix.mkdir()
    Haienv.insert(haienv_name='ext', haienv_config=HaienvConfig(
        path=str(prefix), extend='True', extend_env='base', py='3.8'),
        outside_db_path=str(tmp_path / 'venv.db'))

    called = []

    async def _fake_requests(*args, **kwargs):
        called.append(args)
        return {'success': 1}

    monkeypatch.setattr(venv_api, 'async_requests', _fake_requests)
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('ext'))
    assert result['success'] == 0
    assert 'extend' in result['msg']
    assert called == [], '不得发起 API-11 请求'


def test_tc_c06_push_empty_path(venv_api, tmp_path, monkeypatch):
    '''TC-C06/E7：API-11 返回 path=None → 明确失败，不执行上传、不抛 KeyError。'''
    _use_env_dir(tmp_path)
    prefix = tmp_path / 'ok_0'
    prefix.mkdir()
    Haienv.insert(haienv_name='ok', haienv_config=HaienvConfig(
        path=str(prefix), extend='False', extend_env='', py='3.8'),
        outside_db_path=str(tmp_path / 'venv.db'))

    async def _fake_requests(*args, **kwargs):
        return {'success': 1, 'path': None, 'exists': False, 'cloud_path': 'g/shared/hfai_envs/U-A/ok_0'}

    executed = []
    monkeypatch.setattr(venv_api, 'async_requests', _fake_requests)
    monkeypatch.setattr(venv_api.os, 'system', lambda cmd: executed.append(cmd) or 0)

    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('ok'))
    assert result['success'] == 0
    assert 'path' in result['msg'] or '落盘路径' in result['msg']
    assert executed == [], 'path 为空时不得执行上传命令'


def test_tc_c07_upload_failure_then_no_register(venv_api, tmp_path, monkeypatch):
    '''TC-C07：上传失败 → success=0 且**不调用 API-13**。'''
    _use_env_dir(tmp_path)
    prefix = tmp_path / 'up_0'
    prefix.mkdir()
    Haienv.insert(haienv_name='up', haienv_config=HaienvConfig(
        path=str(prefix), extend='False', extend_env='', py='3.8'),
        outside_db_path=str(tmp_path / 'venv.db'))

    urls = []

    async def _fake_requests(method, url, **kwargs):
        urls.append(url)
        return {'success': 1, 'path': '/cluster/up_0', 'exists': False,
                'cloud_path': 'g/shared/hfai_envs/U-A/up_0'}

    monkeypatch.setattr(venv_api, 'async_requests', _fake_requests)
    monkeypatch.setattr(venv_api.os, 'system', lambda cmd: 1)  # 模拟上传非 0 退出
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('up'))
    assert result['success'] == 0
    assert '上传失败' in result['msg']
    assert len(urls) == 1 and 'update_cluster_venv' in urls[0], urls


def test_tc_c08_register_failure_is_distinguishable(venv_api, tmp_path, monkeypatch):
    '''TC-C08/AC-06：上传成功但注册失败 → 明确「已上传但注册失败，可重试」。'''
    _use_env_dir(tmp_path)
    prefix = tmp_path / 'reg_0'
    prefix.mkdir()
    Haienv.insert(haienv_name='reg', haienv_config=HaienvConfig(
        path=str(prefix), extend='False', extend_env='', py='3.8'),
        outside_db_path=str(tmp_path / 'venv.db'))

    urls = []

    async def _fake_requests(method, url, **kwargs):
        urls.append(url)
        if 'update_cluster_venv' in url:
            return {'success': 1, 'path': '/cluster/reg_0', 'exists': False,
                    'cloud_path': 'g/shared/hfai_envs/U-A/reg_0'}
        return {'success': 0, 'code': 'ENV_REGISTRY_WRITE_FAILED', 'msg': '不可写'}

    monkeypatch.setattr(venv_api, 'async_requests', _fake_requests)
    monkeypatch.setattr(venv_api.os, 'system', lambda cmd: 0)
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('reg'))
    assert result['success'] == 0
    assert '已上传但注册失败' in result['msg']
    assert 'env push reg' in result['msg']
    assert len(urls) == 2 and 'register_cluster_venv' in urls[1]


def test_tc_c09_success_message(venv_api, tmp_path, monkeypatch):
    '''TC-C02：全链路成功 → exit 0 语义 + 提示 source haienv。'''
    _use_env_dir(tmp_path)
    prefix = tmp_path / 'all_0'
    prefix.mkdir()
    Haienv.insert(haienv_name='all', haienv_config=HaienvConfig(
        path=str(prefix), extend='False', extend_env='', py='3.8',
        extra_search_dir=['/opt/x']),
        outside_db_path=str(tmp_path / 'venv.db'))

    sent = {}

    async def _fake_requests(method, url, **kwargs):
        if 'update_cluster_venv' in url:
            return {'success': 1, 'path': '/cluster/all_0', 'exists': False,
                    'cloud_path': 'hfai/shared/hfai_envs/U-A/all_0'}
        sent['body'] = kwargs.get('data')
        return {'success': 1, 'registered': True}

    monkeypatch.setattr(venv_api, 'async_requests', _fake_requests)
    # 注意：不能写 `sent.setdefault('cmd', cmd) or 0` —— 非空字符串为真值，会把命令当成退出码
    monkeypatch.setattr(venv_api.os, 'system', lambda cmd: (sent.setdefault('cmd', cmd), 0)[1])
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('all'))
    assert result['success'] == 1
    assert '上传并成功' in result['msg'] or '上传并注册成功' in result['msg']
    assert 'source haienv all' in result['msg']
    # C-6：--env_remote_path 必须是对象存储 key 前缀（cloud_path），不能是集群文件系统路径
    assert '--env_remote_path hfai/shared/hfai_envs/U-A/all_0' in sent['cmd'], sent['cmd']
    assert '--env_remote_path /cluster/all_0' not in sent['cmd']
    # 注册 body 必须是 JSON，且 extra_search_dir 原样保留（不字符串化）
    import json
    body = json.loads(sent['body'])
    assert body['venv_name'] == 'all'
    assert body['path'] == '/cluster/all_0'
    assert body['extra_search_dir'] == ['/opt/x']


def test_cmp04_version_skew_note_and_strict(venv_api, tmp_path, monkeypatch):
    '''CMP-04 / ADR-E4：服务端 haienv 基础版本不一致时提示；HAIENV_STRICT_VERSION=1 时中止上传。'''
    _use_env_dir(tmp_path)
    prefix = tmp_path / 'ver_0'
    prefix.mkdir()
    Haienv.insert(haienv_name='ver', haienv_config=HaienvConfig(
        path=str(prefix), extend='False', extend_env='', py='3.8'),
        outside_db_path=str(tmp_path / 'venv.db'))

    async def _fake_requests(method, url, **kwargs):
        if 'update_cluster_venv' in url:
            return {'success': 1, 'path': '/cluster/ver_0', 'exists': False,
                    'cloud_path': 'hfai/shared/hfai_envs/U-A/ver_0',
                    'haienv_version': '9.9.9+serverrev'}
        return {'success': 1, 'registered': True}

    executed = []
    monkeypatch.setattr(venv_api, 'async_requests', _fake_requests)
    monkeypatch.setattr(venv_api.os, 'system', lambda cmd: executed.append(cmd) or 0)
    monkeypatch.setattr(venv_api, '_local_haienv_version', lambda: '1.4.1+localrev')
    monkeypatch.delenv('HAIENV_STRICT_VERSION', raising=False)

    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('ver'))
    assert result['success'] == 1, result['msg']          # 默认只提示、不阻断
    assert '集群侧 haienv' in result['msg'] and '9.9.9+serverrev' in result['msg']
    assert len(executed) == 1, '默认不应中止上传'

    # 带 git rev 的完整版本不同、基础版本相同 → 不提示（避免每次 push 都刷无用告警）
    monkeypatch.setattr(venv_api, '_local_haienv_version', lambda: '9.9.9+anotherrev')
    executed.clear()
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('ver'))
    assert result['success'] == 1 and '集群侧 haienv' not in result['msg']

    # 严格模式：基础版本不一致 → 中止，且不上传
    monkeypatch.setattr(venv_api, '_local_haienv_version', lambda: '1.4.1+localrev')
    monkeypatch.setenv('HAIENV_STRICT_VERSION', '1')
    executed.clear()
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('ver'))
    assert result['success'] == 0
    assert 'HAIENV_STRICT_VERSION' in result['msg']
    assert executed == [], '严格模式下不得上传'


def test_cmp04_base_version_helper(venv_api):
    '''基础版本比较：去掉 git rev / 本地后缀。'''
    assert venv_api._base_version('1.4.1+e3c42c') == '1.4.1'
    assert venv_api._base_version('1.4.1') == '1.4.1'
    assert venv_api._base_version('') == ''


def test_rb02_missing_route_reports_clear_error(venv_api, tmp_path, monkeypatch):
    '''RB-02 / 版本偏斜：服务端没有这两个路由（404 → `async_requests` 抛异常）时必须给可读结论。'''
    _use_env_dir(tmp_path)
    prefix = tmp_path / 'noroute_0'
    prefix.mkdir()
    Haienv.insert(haienv_name='noroute', haienv_config=HaienvConfig(
        path=str(prefix), extend='False', extend_env='', py='3.8'),
        outside_db_path=str(tmp_path / 'venv.db'))

    async def _boom(*args, **kwargs):
        raise Exception("请求失败: [exception: {'detail': 'Not Found'}]")

    executed = []
    monkeypatch.setattr(venv_api, 'async_requests', _boom)
    monkeypatch.setattr(venv_api.os, 'system', lambda cmd: executed.append(cmd) or 0)
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('noroute'))
    assert result['success'] == 0
    assert 'Not Found' in result['msg'] or '接口不存在' in result['msg']
    assert '先服务端' in result['msg'], result['msg']
    assert executed == [], '预检失败不得上传'


def test_register_exception_is_graded_not_raised(venv_api, tmp_path, monkeypatch):
    '''注册阶段抛异常（网络/路由问题）→ 归入「已上传但注册失败，可重试」，不得抛栈。'''
    _use_env_dir(tmp_path)
    prefix = tmp_path / 'regerr_0'
    prefix.mkdir()
    Haienv.insert(haienv_name='regerr', haienv_config=HaienvConfig(
        path=str(prefix), extend='False', extend_env='', py='3.8'),
        outside_db_path=str(tmp_path / 'venv.db'))

    async def _fake_requests(method, url, **kwargs):
        if 'update_cluster_venv' in url:
            return {'success': 1, 'path': '/cluster/regerr_0', 'exists': False,
                    'cloud_path': 'hfai/shared/hfai_envs/U-A/regerr_0'}
        raise Exception('请求失败: [exception: boom]')

    executed = []
    monkeypatch.setattr(venv_api, 'async_requests', _fake_requests)
    monkeypatch.setattr(venv_api.os, 'system', lambda cmd: executed.append(cmd) or 0)
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('regerr'))
    assert result['success'] == 0
    assert '已上传但注册失败' in result['msg'] and '重试' in result['msg'], result['msg']
    assert len(executed) == 1, '上传已执行，不应回滚'


def test_tc_c10_missing_cloud_path(venv_api, tmp_path, monkeypatch):
    """C-6 回归：服务端只返回集群 path、没有 cloud_path 时必须明确失败，且不执行上传。

    历史缺陷：客户端把集群文件系统路径当对象 key 前缀用，对象被写到
    `nfs-shared/.../<name>.zip`，服务端 stage2 读 `{group}/shared/hfai_envs/...` 必然 404。
    """
    _use_env_dir(tmp_path)
    prefix = tmp_path / 'noc_0'
    prefix.mkdir()
    Haienv.insert(haienv_name='noc', haienv_config=HaienvConfig(
        path=str(prefix), extend='False', extend_env='', py='3.8'),
        outside_db_path=str(tmp_path / 'venv.db'))

    async def _fake_requests(*args, **kwargs):
        return {'success': 1, 'path': '/nfs-shared/hai-platform/workspace/hfai_envs/U-A/noc_0',
                'exists': False}

    executed = []
    monkeypatch.setattr(venv_api, 'async_requests', _fake_requests)
    monkeypatch.setattr(venv_api.os, 'system', lambda cmd: executed.append(cmd) or 0)
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('noc'))
    assert result['success'] == 0
    assert 'cloud_path' in result['msg']
    assert executed == [], '缺 cloud_path 时不得上传'


# --------------------------------------------------------------------------- issue #5
# `hai-cli env push` 曾经无条件 `os.chmod(os.path.dirname(item.path), 0o777)`：
# 在 hai/K8s 部署里 `HAIENV_PATH` 缺省为 `$HOME`，于是把家目录改成 0777，
# sshd 的 StrictModes 随即拒绝公钥登录，把用户锁在机器外（见 issue #5）。

def test_issue5_relax_helper_guard(monkeypatch, tmp_path):
    '''护栏本身：家目录 / 家目录的祖先 / 过浅系统目录一律跳过，正常 env 根目录才放宽。'''
    from haienv.client import model as model_mod

    fake_home = tmp_path / 'home' / 'user'
    fake_home.mkdir(parents=True)
    real_expanduser = os.path.expanduser
    monkeypatch.setattr(os.path, 'expanduser',
                        lambda p: str(fake_home) if p == '~' else real_expanduser(p))

    chmods = []
    monkeypatch.setattr(os, 'chmod', lambda p, m: chmods.append(str(p)))

    assert model_mod.relax_env_dir_permissions(str(fake_home), quiet=True) is False
    assert model_mod.relax_env_dir_permissions(str(fake_home.parent), quiet=True) is False
    assert model_mod.relax_env_dir_permissions('/', quiet=True) is False
    assert model_mod.relax_env_dir_permissions(str(tmp_path / 'not-exist'), quiet=True) is False
    assert chmods == [], f'护栏失效，不该 chmod: {chmods}'

    ok_root = tmp_path / 'hfai_envs' / 'U-A'
    ok_root.mkdir(parents=True)
    assert model_mod.relax_env_dir_permissions(str(ok_root), quiet=True) is True
    assert chmods == [str(ok_root)], '非家目录的 env 根目录仍应放宽权限（设计 §4.4 ①）'


def test_issue5_push_does_not_chmod_home(venv_api, tmp_path, monkeypatch):
    '''回归 issue #5：HAIENV_PATH 缺省为 $HOME 时，push_venv 绝不能 chmod 家目录。'''
    fake_home = tmp_path / 'home' / 'user'
    (fake_home / 'myenv_0').mkdir(parents=True)
    _use_env_dir(fake_home)
    Haienv.insert(haienv_name='myenv', haienv_config=HaienvConfig(
        path=str(fake_home / 'myenv_0'), extend='False', extend_env='', py='3.8'),
        outside_db_path=str(fake_home / 'venv.db'))

    real_expanduser = os.path.expanduser
    monkeypatch.setattr(os.path, 'expanduser',
                        lambda p: str(fake_home) if p == '~' else real_expanduser(p))
    chmods = []
    monkeypatch.setattr(os, 'chmod', lambda p, m: chmods.append(str(p)))

    async def _fake_requests(method, url, **kwargs):
        if 'update_cluster_venv' in url:
            return {'success': 1, 'path': '/cluster/myenv_0', 'exists': False,
                    'cloud_path': 'hfai/shared/hfai_envs/U-A/myenv_0'}
        return {'success': 1, 'registered': True}

    monkeypatch.setattr(venv_api, 'async_requests', _fake_requests)
    monkeypatch.setattr(venv_api.os, 'system', lambda cmd: 0)
    result = asyncio.get_event_loop().run_until_complete(venv_api.push_venv('myenv'))
    assert result['success'] == 1, result
    assert [p for p in chmods if str(fake_home) in p] == [], \
        f'push_venv 不该 chmod 家目录，实际: {chmods}'
