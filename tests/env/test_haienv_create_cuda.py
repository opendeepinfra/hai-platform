# -*- coding: utf-8 -*-
'''
`haienv create` 的 CUDA 版本前置检查（分析报告 E10 / 实现记录 C-8）。

历史行为：只允许字面量 '11.1' 与 '11.3'（`command.py` 里 `any(v in nvcc_out for v in ['11.1','11.3'])`），
于是 CUDA 11.5 等新镜像**默认就不能创建环境**。

现在的默认行为：放行 **CUDA 11.x 全部小版本**（11.0 ~ 11.9，含 11.5），并可用
`HAIENV_CUDA_VERSION_RE` 收紧/放宽。

运行（host 103，已安装 hai-cli/haienv）：
    python3 -m pytest tests/env/test_haienv_create_cuda.py -v
'''

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from haienv.client import command as command_module  # noqa: E402


NVCC_11_5 = 'nvcc: NVIDIA (R) Cuda compiler driver\nCopyright (c) 2005-2021 NVIDIA Corporation\n' \
            'Built on Thu_Jun_17_08:23:53_PDT_2021\nCuda compilation tools, release 11.5, V11.5.119\n' \
            'Build cuda_11.5.r11.5/compiler.30044822_0\n'
NVCC_11_3 = 'Cuda compilation tools, release 11.3, V11.3.109\nBuild cuda_11.3.r11.3/compiler.30044822_0\n'
NVCC_11_1 = 'Cuda compilation tools, release 11.1, V11.1.105\n'
NVCC_11_0 = 'Cuda compilation tools, release 11.0, V11.0.221\n'
NVCC_11_8 = 'Cuda compilation tools, release 11.8, V11.8.89\n'
NVCC_12_0 = 'Cuda compilation tools, release 12.0, V12.0.140\n'
NVCC_10_2 = 'Cuda compilation tools, release 10.2, V10.2.89\n'


def test_get_cuda_version_parses_release():
    assert command_module.get_cuda_version(NVCC_11_5) == '11.5'
    assert command_module.get_cuda_version(NVCC_11_3) == '11.3'
    assert command_module.get_cuda_version(NVCC_11_1) == '11.1'
    assert command_module.get_cuda_version(NVCC_11_0) == '11.0'
    assert command_module.get_cuda_version(NVCC_11_8) == '11.8'
    assert command_module.get_cuda_version(NVCC_12_0) == '12.0'
    assert command_module.get_cuda_version('nvcc: command not found') == ''
    assert command_module.get_cuda_version('') == ''


@pytest.mark.parametrize('output,expected', [
    (NVCC_11_0, '11.0'),
    (NVCC_11_1, '11.1'),
    (NVCC_11_3, '11.3'),
    (NVCC_11_5, '11.5'),   # E10 的核心诉求
    (NVCC_11_8, '11.8'),
])
def test_default_accepts_all_cuda_11_x(output, expected):
    '''默认放行 CUDA 11.x 全部小版本（含 11.5）。'''
    assert command_module.check_cuda_version(output) == expected


@pytest.mark.parametrize('output', [NVCC_12_0, NVCC_10_2, 'no version here'])
def test_default_rejects_non_11_x(output):
    '''默认**不**放行 12.x / 10.x / 解析不出者，且错误信息里要给出 11.x 与覆盖方式。'''
    with pytest.raises(AssertionError) as exc:
        command_module.check_cuda_version(output)
    message = str(exc.value)
    assert '11.x' in message and '11.5' in message
    assert 'HAIENV_CUDA_VERSION_RE' in message


def test_missing_nvcc_message():
    '''nvcc 不存在时仍提示 PATH。'''
    with pytest.raises(AssertionError) as exc:
        command_module.check_cuda_version('')
    assert 'nvcc' in str(exc.value) and 'PATH' in str(exc.value)


def test_regex_override_tighten(monkeypatch):
    monkeypatch.setenv('HAIENV_CUDA_VERSION_RE', r'^11\.(1|3)$')
    assert command_module.check_cuda_version(NVCC_11_1) == '11.1'
    with pytest.raises(AssertionError):
        command_module.check_cuda_version(NVCC_11_5)


def test_regex_override_widen(monkeypatch):
    monkeypatch.setenv('HAIENV_CUDA_VERSION_RE', r'^1[12]\.\d+$')
    assert command_module.check_cuda_version(NVCC_12_0) == '12.0'
    assert command_module.check_cuda_version(NVCC_11_5) == '11.5'


def test_regex_override_empty_falls_back_to_default(monkeypatch):
    '''空字符串/未设置的覆盖值都必须回落到默认（11.x）。'''
    monkeypatch.setenv('HAIENV_CUDA_VERSION_RE', '')
    assert command_module.check_cuda_version(NVCC_11_5) == '11.5'
    with pytest.raises(AssertionError):
        command_module.check_cuda_version(NVCC_12_0)


def test_check_uses_nvcc_command_when_no_output(monkeypatch):
    '''走真实 `os.popen(NVCC_CMD)` 分支（用伪造的 nvcc 输出文件模拟容器内的 nvcc）。'''
    import tempfile

    with tempfile.NamedTemporaryFile('w', suffix='.txt', delete=False) as f:
        f.write(NVCC_11_5)
        path = f.name
    try:
        monkeypatch.setattr(command_module, 'NVCC_CMD', f'cat {path}')
        assert command_module.check_cuda_version() == '11.5'
    finally:
        os.unlink(path)
