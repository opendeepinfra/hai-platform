# -*- coding: utf-8 -*-
'''
`haienv create` 的前置提示（分析报告 E10 / 实现记录 C-8）。

设计取舍（方案 1）：CUDA 版本**不再作为硬门禁**，改成「提示 + 可选严格模式」；
同时补上两条更接近真实故障的提示——python 版本与集群基础环境是否一致、非平台环境下 extend 的语义问题。

覆盖：
  * CUDA：多个 nvcc 候选（union 判定）/ 11.x（含 11.5）放行 / 非 11.x 只告警
          / `HAIENV_CUDA_STRICT=1` 或 `strict=True` 才拦截 / `HAIENV_CUDA_VERSION_RE` 覆盖
  * python：与集群基线 3.8 不一致时告警
  * extend：非平台环境下未加 `--no_extend` 时告警

运行（host 103，已安装 hai-cli/haienv）：
    python3 -m pytest tests/env/test_haienv_create_prereq.py -v
'''

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from haienv.client import command as command_module  # noqa: E402


NVCC_11_5 = ('nvcc: NVIDIA (R) Cuda compiler driver\n'
             'Cuda compilation tools, release 11.5, V11.5.119\n'
             'Build cuda_11.5.r11.5/compiler.30044822_0\n')
NVCC_11_3 = 'Cuda compilation tools, release 11.3, V11.3.109\n'
NVCC_11_1 = 'Cuda compilation tools, release 11.1, V11.1.105\n'
NVCC_11_0 = 'Cuda compilation tools, release 11.0, V11.0.221\n'
NVCC_11_8 = 'Cuda compilation tools, release 11.8, V11.8.89\n'
NVCC_12_9 = 'Cuda compilation tools, release 12.9, V12.9.86\n'
NVCC_10_2 = 'Cuda compilation tools, release 10.2, V10.2.89\n'


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    '''避免宿主上的 HAIENV_* 影响用例。'''
    for name in ('HAIENV_CUDA_STRICT', 'HAIENV_CUDA_VERSION_RE', 'TASK_NAME', 'HAIENV_CLUSTER_PY'):
        monkeypatch.delenv(name, raising=False)
    yield


# --------------------------------------------------------------------------- CUDA

def test_get_cuda_version_parses_release():
    assert command_module.get_cuda_version(NVCC_11_5) == '11.5'
    assert command_module.get_cuda_version(NVCC_11_3) == '11.3'
    assert command_module.get_cuda_version(NVCC_11_0) == '11.0'
    assert command_module.get_cuda_version(NVCC_11_8) == '11.8'
    assert command_module.get_cuda_version(NVCC_12_9) == '12.9'
    assert command_module.get_cuda_version('nvcc: command not found') == ''
    assert command_module.get_cuda_version('') == ''


@pytest.mark.parametrize('output,expected', [
    (NVCC_11_0, '11.0'),
    (NVCC_11_1, '11.1'),
    (NVCC_11_3, '11.3'),
    (NVCC_11_5, '11.5'),   # E10 的核心诉求
    (NVCC_11_8, '11.8'),
])
def test_cuda_11_x_is_ok_and_not_blocking(output, expected, capsys):
    result = command_module.check_cuda_version(output)
    assert result['ok'] is True
    assert result['matched'] == expected
    assert '在受支持范围内' in capsys.readouterr().out


@pytest.mark.parametrize('output', [NVCC_12_9, NVCC_10_2, 'no version here'])
def test_non_11_x_only_warns(output, capsys):
    '''方案 1 的核心：不匹配时**不抛异常**，只打印 WARNING（含严格模式开关提示）。'''
    result = command_module.check_cuda_version(output)
    assert result['ok'] is False
    out = capsys.readouterr().out
    assert 'WARNING' in out
    assert 'HAIENV_CUDA_STRICT=1' in out
    assert 'HAIENV_CUDA_VERSION_RE' in out


def test_no_nvcc_only_warns(capsys, monkeypatch):
    monkeypatch.setattr(command_module, 'collect_nvcc_reports', lambda candidates=None: {})
    result = command_module.check_cuda_version()
    assert result['ok'] is False and result['versions'] == {}
    assert '未检测到 nvcc' in capsys.readouterr().out


def test_strict_flag_raises():
    with pytest.raises(AssertionError) as exc:
        command_module.check_cuda_version(NVCC_12_9, strict=True)
    assert 'HAIENV_CUDA_STRICT' in str(exc.value)


def test_strict_env_raises(monkeypatch):
    monkeypatch.setenv('HAIENV_CUDA_STRICT', '1')
    with pytest.raises(AssertionError):
        command_module.check_cuda_version(NVCC_12_9)
    # 11.x 在严格模式下依然放行
    assert command_module.check_cuda_version(NVCC_11_5)['matched'] == '11.5'


def test_multi_candidate_union(monkeypatch, capsys):
    '''一台机器多个 nvcc：任一命中即通过（本机 apt 11.5 + /usr/local/cuda 12.9）。'''
    reports = {'nvcc': NVCC_11_5, '/usr/local/cuda/bin/nvcc': NVCC_12_9}
    monkeypatch.setattr(command_module, 'collect_nvcc_reports', lambda candidates=None: reports)
    result = command_module.check_cuda_version()
    assert result['ok'] is True and result['matched'] == '11.5'

    # 反过来：PATH 是 12.9、/usr/local/cuda 是 11.3 → 也通过
    reports = {'nvcc': NVCC_12_9, '/usr/local/cuda/bin/nvcc': NVCC_11_3}
    monkeypatch.setattr(command_module, 'collect_nvcc_reports', lambda candidates=None: reports)
    assert command_module.check_cuda_version()['matched'] == '11.3'


def test_multi_candidate_all_unsupported_lists_versions(monkeypatch, capsys):
    reports = {'nvcc': NVCC_12_9, '/usr/local/cuda/bin/nvcc': NVCC_10_2}
    monkeypatch.setattr(command_module, 'collect_nvcc_reports', lambda candidates=None: reports)
    assert command_module.check_cuda_version()['ok'] is False
    out = capsys.readouterr().out
    assert '12.9' in out and '10.2' in out


def test_regex_override_tighten(monkeypatch):
    monkeypatch.setenv('HAIENV_CUDA_VERSION_RE', r'^11\.(1|3)$')
    assert command_module.check_cuda_version(NVCC_11_1)['ok'] is True
    assert command_module.check_cuda_version(NVCC_11_5)['ok'] is False


def test_regex_override_widen(monkeypatch):
    monkeypatch.setenv('HAIENV_CUDA_VERSION_RE', r'^1[12]\.\d+$')
    assert command_module.check_cuda_version(NVCC_12_9)['ok'] is True
    assert command_module.check_cuda_version(NVCC_11_5)['ok'] is True


def test_regex_override_empty_falls_back_to_default(monkeypatch):
    monkeypatch.setenv('HAIENV_CUDA_VERSION_RE', '')
    assert command_module.check_cuda_version(NVCC_11_5)['ok'] is True
    assert command_module.check_cuda_version(NVCC_12_9)['ok'] is False


def test_check_uses_nvcc_candidates_when_no_output(monkeypatch, tmp_path, capsys):
    '''走真实候选探测分支：把候选换成伪造的 nvcc 可执行文件。'''
    fake = tmp_path / 'nvcc'
    fake.write_text('#!/bin/sh\necho "Cuda compilation tools, release 11.5, V11.5.119"\n')
    fake.chmod(0o755)
    monkeypatch.setattr(command_module, 'NVCC_CANDIDATES', (str(fake),))
    result = command_module.check_cuda_version()
    assert result['matched'] == '11.5'
    assert '在受支持范围内' in capsys.readouterr().out


# --------------------------------------------------------------------------- python

@pytest.mark.parametrize('py', ['3.8', '3.8.10'])
def test_python_matching_cluster_base(py, capsys):
    result = command_module.check_python_version(py)
    assert result['ok'] is True
    assert 'WARNING' not in capsys.readouterr().out


def test_python_mismatch_warns(capsys):
    '''裸机默认取当前解释器（3.10），与集群基础环境 3.8 不一致 → 告警但不阻断。'''
    result = command_module.check_python_version('3.10.12')
    assert result['ok'] is False
    out = capsys.readouterr().out
    assert 'WARNING' in out and '3.8' in out and '-p 3.8' in out


def test_python_empty_is_skipped(capsys):
    assert command_module.check_python_version('')['ok'] is True
    assert 'WARNING' not in capsys.readouterr().out


def test_cluster_base_py_configurable(monkeypatch, capsys):
    monkeypatch.setattr(command_module, 'CLUSTER_BASE_PY', '3.10')
    assert command_module.check_python_version('3.10.12')['ok'] is True


# --------------------------------------------------------------------------- extend

def test_extend_warns_outside_platform(capsys):
    result = command_module.check_extend_policy(extend=True, in_platform=False)
    assert result['ok'] is False
    out = capsys.readouterr().out
    assert 'WARNING' in out and '--no_extend' in out


def test_extend_ok_inside_platform(capsys):
    result = command_module.check_extend_policy(extend=True, in_platform=True)
    assert result['ok'] is True
    assert 'WARNING' not in capsys.readouterr().out


def test_no_extend_ok(capsys):
    assert command_module.check_extend_policy(extend=False, in_platform=False)['ok'] is True
    assert 'WARNING' not in capsys.readouterr().out


def test_in_platform_env_detection(monkeypatch):
    monkeypatch.setattr(command_module, 'PLATFORM_ENV_MARKERS', ('/definitely/not/exist',))
    monkeypatch.delenv('TASK_NAME', raising=False)
    assert command_module.in_platform_env() is False
    monkeypatch.setenv('TASK_NAME', 'some_task')
    assert command_module.in_platform_env() is True

    monkeypatch.delenv('TASK_NAME', raising=False)
    tmp_marker = os.path.join(os.path.dirname(os.path.abspath(__file__)), '__platform_probe__')
    open(tmp_marker, 'w').close()
    try:
        monkeypatch.setattr(command_module, 'PLATFORM_ENV_MARKERS', (tmp_marker,))
        assert command_module.in_platform_env() is True
    finally:
        os.unlink(tmp_marker)


# --------------------------------------------------------------------------- issue #5

def test_issue5_create_haienv_does_not_chmod_home(monkeypatch, tmp_path):
    '''
    回归 issue #5：`create_haienv` 绝不能把 `$HOME`（HAIENV_PATH 缺省值）chmod 成 0777。

    原实现调用了一个没有 import 的 `get_path_prefix()`：必然抛 NameError，又被
    `except Exception: pass` 吞掉 —— 那处 chmod 一直是死代码。改成共享助手后，
    护栏必须真的拦住家目录。
    '''
    import asyncio

    import haienv.client.model as model
    from haienv.client import api as haienv_api

    fake_home = tmp_path / 'home' / 'user'
    env_dir = fake_home / 'myenv_0'
    env_dir.mkdir(parents=True)

    os.environ['HAIENV_PATH'] = str(fake_home)
    model.set_path_prefix(str(fake_home))
    setattr(model, '__db_path', None)

    real_expanduser = os.path.expanduser
    monkeypatch.setattr(os.path, 'expanduser',
                        lambda p: str(fake_home) if p == '~' else real_expanduser(p))
    monkeypatch.setattr('builtins.input', lambda prompt='': 'Y')

    chmods = []
    monkeypatch.setattr(os, 'chmod', lambda p, m: chmods.append(str(p)))
    monkeypatch.setattr(haienv_api, 'get_haienv_path',
                        lambda haienv_name: {'success': 1, 'msg': str(env_dir)})
    monkeypatch.setattr(haienv_api.os, 'system', lambda cmd: 0)  # 不真的调用 conda

    asyncio.get_event_loop().run_until_complete(
        haienv_api.create_haienv('myenv', 'False', '3.8', None, None, None))

    assert [p for p in chmods if str(fake_home) in p] == [], \
        f'create_haienv 不该 chmod 家目录，实际: {chmods}'
