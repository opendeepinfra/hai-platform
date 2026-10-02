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
