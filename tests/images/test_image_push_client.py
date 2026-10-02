# -*- coding: utf-8 -*-
'''
`hai-cli images push` 客户端侧 L1 单元测试 —— 用例 TC-UP-11（客户端的部分）。

为什么单独一个文件：本文件只依赖**已安装的 hfai 客户端包**，可以在 host 上直接跑：

    cd ~/hai-platform && python3 -m pytest tests/images/test_image_push_client.py -q

而 tests/images/test_image_push.py 需要服务端依赖（conf/cloud_storage/…），只在镜像内跑。
两份文件都放在 tests/images/ 下，`pytest tests/images` 在两种环境下都能各自跑通未跳过的那部分。
'''

import asyncio
import os

import pytest

try:
    from hfai.client.api.image_api import _build_image_push_cmd, push_image_tar
except Exception:  # pragma: no cover - 镜像内没有客户端包
    _build_image_push_cmd = None
    push_image_tar = None

pytestmark = pytest.mark.skipif(_build_image_push_cmd is None,
                                reason='客户端 hfai 包未安装（host 专用）')


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_up11_build_push_cmd_contract():
    ''' FR-16：子进程命令必须是字面量 `--file_type image`、恒带 `--no_zip`、且不含枚举串。 '''
    cmd = _build_image_push_cmd('/tmp/stage', 'hfai/shared/images/U-A/demo', 'rustfs', False,
                                False, False, 300, 1800, 120, 1800, 100, '')
    assert '--file_type image' in cmd
    assert 'FileType.IMAGE' not in cmd
    assert '--no_zip' in cmd
    assert '--image_remote_path hfai/shared/images/U-A/demo' in cmd
    assert '--image_local_path /tmp/stage' in cmd
    assert '--image_provider rustfs' in cmd


def test_up11_local_missing_file_makes_no_request():
    ''' TC-UP-11③：本地文件不存在时不发起任何请求，直接给出可读提示。 '''
    assert push_image_tar is not None
    result = run(push_image_tar('/nonexistent/hai-image-demo.tar'))
    assert result['success'] == 0
    assert '不存在这个镜像包' in result['msg']
