# -*- coding: utf-8 -*-
'''
hai-cli images 上传通道（`images push`）L1 单元测试 —— 对应用例集 UP 组中可在无 DB / 无 S3 /
无 k8s 条件下验证的部分（TC-UP-01 / TC-UP-05 / TC-UP-07 / TC-UP-08 / TC-UP-10 / TC-UP-11）。

约束（NFR-10）：
  · 不启动 FastAPI、不连 PostgreSQL、不起 k8s、不需要真实对象存储
  · 路径/开关/白名单/容量上限全部用 monkeypatch 顶替

运行（镜像内，pytest 与全部依赖齐备）：
    cd /high-flyer/code/multi_gpu_runner_server && MARSV2_MANAGER_CONFIG_DIR=/etc/hai_one_config \
        python3 -m pytest tests/images -q

客户端命令组装的两条用例需要已安装 `hfai`（host 上 build_cli_local.sh 之后），
镜像内未安装时自动 skip，不影响服务端用例。
'''

import asyncio
import os

import pytest

from conf.utils import FileType, FilePrivacy
from cloud_storage.service import context as image_context
from cloud_storage.service.errors import ErrorCode, WorkspaceError
from cloud_storage.service import sync_to_cluster as stc
from cloud_storage.utils import check_is_subpath, get_base_path, get_bucket_name

try:  # 客户端命令组装（host 上跑才有）
    from hfai.client.api.image_api import _build_image_push_cmd, push_image_tar
except Exception:  # pragma: no cover - 镜像内通常没有 hfai
    _build_image_push_cmd = None
    push_image_tar = None


def run(coro):
    ''' 不依赖 pytest-asyncio 版本：直接跑协程（与 tests/images 其它文件同一写法）。 '''
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class FakeUser:
    def __init__(self, user_name='U-A', shared_group='hfai', token='tkn'):
        self.user_name = user_name
        self.shared_group = shared_group
        self.token = token


@pytest.fixture
def image_root(monkeypatch, tmp_path):
    root = tmp_path / 'image'
    root.mkdir()
    monkeypatch.setattr('conf.utils.get_image_path', lambda: str(root))
    return str(root)


@pytest.fixture(autouse=True)
def _pass_enabled_gate(monkeypatch):
    ''' 上传通道的 `enabled` 灰度由专门的用例覆盖，默认放行以免干扰其它断言。 '''
    monkeypatch.setattr(image_context, 'check_image_enabled', lambda user: None)


# --------------------------------------------------------------------------- TC-UP-01

def test_up01_get_base_path_image_has_cloud_and_cluster(image_root):
    ''' TC-UP-01：IMAGE 必须同时给出 cloud（S3 key 前缀）与 cluster（共享盘落点），且后者在根内。 '''
    cluster_path, cloud_path = get_base_path('U-A', 'hfai', 'demo', FileType.IMAGE,
                                             FilePrivacy.GROUP_SHARED)

    # Q-10：S3 key 前缀（也是 STS 授权前缀）
    assert cloud_path == 'hfai/shared/images/U-A/demo'
    # Q-9：目录 + tar，落点必须落在 image_path 之下（HC-13）
    assert cluster_path == os.path.join(image_root, 'demo')
    check_is_subpath(image_root, cluster_path)


def test_up01_bucket_for_image_is_private():
    ''' TC-UP-01：IMAGE 走 private bucket（get_bucket_name 无需新增分支）。 '''
    from conf import CONF
    assert get_bucket_name(FileType.IMAGE, FilePrivacy.GROUP_SHARED) == CONF.cloud.storage.private_bucket


def test_up01_cloud_path_is_scoped_to_user_and_name(image_root):
    ''' SEC-08：不同用户 / 不同 name 的授权前缀必须不同。 '''
    _, cloud_a = get_base_path('U-A', 'hfai', 'demo', FileType.IMAGE, FilePrivacy.GROUP_SHARED)
    _, cloud_b = get_base_path('U-B', 'hfai', 'demo', FileType.IMAGE, FilePrivacy.GROUP_SHARED)
    _, cloud_c = get_base_path('U-A', 'hfai', 'other', FileType.IMAGE, FilePrivacy.GROUP_SHARED)
    assert cloud_a != cloud_b and cloud_a != cloud_c


# --------------------------------------------------------------------------- TC-UP-10

def test_up10_max_tar_bytes_guard(monkeypatch):
    ''' TC-UP-10：超限快速失败，未声明或 0（不限制）时放行。 '''
    monkeypatch.setattr(image_context, 'cfg', lambda path, default=None: 100)
    assert image_context.get_image_max_tar_bytes() == 100

    with pytest.raises(WorkspaceError) as ei:
        image_context.check_image_max_tar_bytes(101)
    assert ei.value.code == ErrorCode.IMAGE_TAR_TOO_LARGE

    image_context.check_image_max_tar_bytes(100)      # 边界：等于上限放行
    image_context.check_image_max_tar_bytes(None)     # 未声明：不判定
    image_context.check_image_max_tar_bytes(0)

    monkeypatch.setattr(image_context, 'cfg', lambda path, default=None: 0)
    assert image_context.get_image_max_tar_bytes() == 0
    image_context.check_image_max_tar_bytes(10 ** 12)  # 不限制


# --------------------------------------------------------------------------- TC-UP-07 / TC-UP-08

def test_up07_enabled_gate_is_common_entry(monkeypatch):
    ''' FR-19 / HC-12：`enabled` 是上传与控制面的共同入口，先被调用。 '''
    calls = []
    monkeypatch.setattr(image_context, 'check_image_enabled',
                        lambda user: calls.append(user))
    monkeypatch.setattr(image_context, 'cfg', lambda path, default=None: True)

    image_context.check_image_upload_enabled(FakeUser())
    assert len(calls) == 1


def test_up08_upload_switch_is_independent(monkeypatch):
    ''' OPS-06：upload_enabled=false 只挡上传，不影响 load/delete/list（控制面不调用本函数）。 '''
    monkeypatch.setattr(image_context, 'check_image_enabled', lambda user: None)

    def cfg(path, default=None):
        return {'image.upload_enabled': False}.get(path, default)

    monkeypatch.setattr(image_context, 'cfg', cfg)
    assert image_context.image_upload_enabled() is False
    with pytest.raises(WorkspaceError) as ei:
        image_context.check_image_upload_enabled(FakeUser())
    assert ei.value.code == ErrorCode.FEATURE_DISABLED

    monkeypatch.setattr(image_context, 'cfg', lambda path, default=None: True)
    assert image_context.image_upload_enabled() is True
    image_context.check_image_upload_enabled(FakeUser())   # 不抛


def test_up08_precheck_required_default_false(monkeypatch):
    ''' 设计 §9.1：upload_require_precheck 默认关闭。 '''
    monkeypatch.setattr(image_context, 'cfg', lambda path, default=None: default)
    assert image_context.image_upload_precheck_required() is False


# --------------------------------------------------------------------------- TC-UP-03 / TC-UP-05

@pytest.fixture
def _stc_guards(monkeypatch):
    ''' 把 submit_to_cluster 的早期副作用（配置/开关/预检）替换成空实现，专测参数闸门。 '''
    monkeypatch.setattr(stc, 'ensure_cloud_storage_configured', lambda: None)
    monkeypatch.setattr(stc, 'check_feature_enabled', lambda user: None)
    monkeypatch.setattr(stc, 'check_image_upload_enabled', lambda user: None)
    monkeypatch.setattr(stc, 'image_upload_precheck_required', lambda: False)


def test_up03_image_requires_no_zip(_stc_guards):
    ''' ADR-I13：images 上传必须 no_zip=true，否则共享盘上会落成 xxx.tar.zip。 '''
    with pytest.raises(WorkspaceError) as ei:
        run(stc.submit_to_cluster(FakeUser(), 'demo', FileType.IMAGE, ['demo.tar'], no_zip=False))
    assert ei.value.code == ErrorCode.INVALID_PARAM
    assert 'no_zip' in ei.value.msg


def test_up03_image_passes_whitelist(_stc_guards, monkeypatch):
    ''' HC-11：IMAGE 必须在 `submit_to_cluster` 的白名单里（否则 400「不支持同步 image 类型」）。 '''
    def sentinel_base_path(*args, **kwargs):
        raise RuntimeError('BASE_PATH_REACHED')     # 走到这里说明已通过白名单与 no_zip 闸门

    monkeypatch.setattr(stc, 'get_base_path', sentinel_base_path)
    with pytest.raises(RuntimeError) as ei:
        run(stc.submit_to_cluster(FakeUser(), 'demo', FileType.IMAGE, ['demo.tar'], no_zip=True))
    assert 'BASE_PATH_REACHED' in str(ei.value)


def test_up05_image_name_must_not_contain_slash(_stc_guards):
    ''' SEC-09：name 不得含 '/'（不得借此逃出落点目录）。 '''
    with pytest.raises(WorkspaceError) as ei:
        run(stc.submit_to_cluster(FakeUser(), 'a/b', FileType.IMAGE, ['demo.tar'], no_zip=True))
    assert ei.value.code == ErrorCode.INVALID_PARAM
    assert '镜像条目名' in ei.value.msg


# --------------------------------------------------------------------------- TC-UP-11（客户端）

@pytest.mark.skipif(_build_image_push_cmd is None, reason='客户端 hfai 包未安装（host 专用）')
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


@pytest.mark.skipif(push_image_tar is None, reason='客户端 hfai 包未安装（host 专用）')
def test_up11_local_missing_file_makes_no_request():
    ''' TC-UP-11③：本地文件不存在时不发起任何请求，直接给出可读提示。 '''
    result = run(push_image_tar('/nonexistent/hai-image-demo.tar'))
    assert result['success'] == 0
    assert '不存在这个镜像包' in result['msg']
