# -*- coding: utf-8 -*-
'''
hai-cli images（用户自定义镜像）L1 单元测试 —— 对应用例集 U 组 TC-U01~TC-U12。

约束（NFR-04）：
  · 不启动 FastAPI、不连 PostgreSQL、不起 k8s、不需要内网 registry
  · DB 访问统一用 fake 替身（monkeypatch TrainImageSelector）
  · 共享根用 tmp_path 顶替（monkeypatch conf.utils.get_image_path）

运行（镜像内，pytest 与全部依赖齐备）：
    cd /high-flyer/code/multi_gpu_runner_server && python3 -m pytest tests/images -q
'''

import asyncio
import json
import os
import subprocess

import numpy as np
import pandas as pd
import pytest

from conf.utils import (FileType, IMAGE_NAME_RE, derive_image_name, get_image_root,
                       is_valid_image_name, normalize_image_path)
from cloud_storage.service import context as image_context
from cloud_storage.service.errors import ErrorCode, WorkspaceError
from server_model.selector.train_image_selector import TrainImageSelector, _json_scalar
from server_model.user_impl.user_image.implement import (ALL_STATUSES, REPORTABLE_FROM,
                                                         STATUS_DELETED, STATUS_FAILED,
                                                         STATUS_LOADED, STATUS_LOADING,
                                                         STATUS_PROCESSING, UserImage)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


# --------------------------------------------------------------------------- helpers

def run(coro):
    ''' 不依赖 pytest-asyncio 版本：直接跑协程（与 tests/env 同一写法）。 '''
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class FakeUser:
    def __init__(self, user_name='U-A', shared_group='hfai'):
        self.user_name = user_name
        self.shared_group = shared_group

    def in_any_group(self, groups):
        return self.shared_group in list(groups)


def make_image(shared_group='hfai', user_name='U-A'):
    return UserImage(FakeUser(user_name=user_name, shared_group=shared_group))


@pytest.fixture(autouse=True)
def _enable_feature(monkeypatch):
    ''' 单元测试不依赖运行期灰度配置：放行 check_image_enabled。 '''
    monkeypatch.setattr(image_context, 'check_image_enabled', lambda user: None)


@pytest.fixture
def image_root(monkeypatch, tmp_path):
    root = tmp_path / 'image'
    root.mkdir()
    monkeypatch.setattr('conf.utils.get_image_path', lambda: str(root))
    return root


class _Awaitable:
    def __init__(self, value):
        self.value = value

    def __await__(self):
        async def _coro():
            return self.value
        return _coro().__await__()


# --------------------------------------------------------------------------- TC-U01

def test_u01_image_root_single_point(monkeypatch, tmp_path):
    ''' TC-U01：get_image_root / get_base_path(FileType.IMAGE) 同源且归一化。 '''
    root = tmp_path / 'image'
    root.mkdir()
    monkeypatch.setattr('conf.utils.get_image_path', lambda: str(root))
    assert get_image_root() == str(root)

    from cloud_storage.utils import get_base_path
    cluster_path, cloud_path = get_base_path('U-A', 'hfai', 'demo:v1', FileType.IMAGE)
    assert cluster_path == os.path.join(str(root), 'demo:v1')
    assert cloud_path == ''

    assert normalize_image_path('/a/b/') == '/a/b'
    assert normalize_image_path(None) == '/nfs_shared/image'
    assert normalize_image_path('') == '/nfs_shared/image'


# --------------------------------------------------------------------------- TC-U02/U03/U04

@pytest.mark.parametrize('tar_path,expected', [
    ('/nfs-shared/hai-platform/image/demo.tar', 'demo'),
    ('/nfs-shared/hai-platform/image/demo', 'demo'),
    ('/nfs-shared/hai-platform/image/a b.tar', 'a b'),
    ('/nfs-shared/hai-platform/image/x.TAR', 'x.TAR'),
    ('/nfs-shared/hai-platform/image/sub/demo:v1.tar', 'demo:v1'),
])
def test_u02_derive_image_name(tar_path, expected):
    '''
    TC-U02：缺省由 basename 去 .tar 后缀派生。

    ⚠️ 与用例文档 TC-U02 / Checklist DEV-07 的「无 tag 补 :latest」**有意不一致**：
    设计 §4.1「实现修正 I6b」明确禁止自动补 tag —— 任务侧 K2 是**逐字节**比较，
    补 :latest 会让 `-i registry/<group>/demo` 永远匹配不上。这里以设计为准（见决策记录）。
    '''
    assert derive_image_name(tar_path) == expected
    assert ':' not in expected or expected.count(':') == 1


@pytest.mark.parametrize('name', ['demo:v1', 'demo', 'a.b_c-d:v1.2', 'A1:x'])
def test_u03_valid_names(name):
    assert is_valid_image_name(name)
    assert IMAGE_NAME_RE.match(name)


@pytest.mark.parametrize('name', ['', 'a/b:v1', '../x', 'a b', 'a' * 65, 'a:v1:v2', ':v1', 'a:', 'x '])
def test_u03_invalid_names(name):
    assert not is_valid_image_name(name)


@pytest.mark.parametrize('name', ['demo:v1', 'demo', 'a.b_c-d:v1.2', 'A1:x'])
def test_u04_three_segments(name):
    ''' TC-U04：image 自身永不含 '/'，与 registry/group 拼出来恰好 3 段。 '''
    assert '/' not in name
    assert len(f'registry.high-flyer.cn/hfai/{name}'.split('/')) == 3


# --------------------------------------------------------------------------- TC-U05/U06

def test_u05_path_escape_and_missing(image_root):
    ''' TC-U05：越界路径全部拒绝；root 自身（目录）也必须拒绝。 '''
    sub = image_root / 'a'
    sub.mkdir()
    tar = sub / 'b.tar'
    tar.write_bytes(b'demo')
    img = make_image()

    assert img._normalize_image_tar(str(tar)) == str(tar)

    cases = [
        (str(image_root / '..' / 'etc' / 'passwd'), ErrorCode.PATH_ESCAPE),
        ('/etc/passwd', ErrorCode.PATH_ESCAPE),
        ('/tmp/fake.tar', ErrorCode.PATH_ESCAPE),
        (str(image_root), ErrorCode.IMAGE_TAR_NOT_FOUND),
        (str(sub), ErrorCode.IMAGE_TAR_NOT_FOUND),
        ('', ErrorCode.INVALID_PARAM),
        (None, ErrorCode.INVALID_PARAM),
    ]
    for raw, code in cases:
        with pytest.raises(WorkspaceError) as e:
            img._normalize_image_tar(raw)
        assert e.value.code == code, f'{raw}: {e.value.code} != {code}'


def test_u06_symlink_escape(image_root):
    ''' TC-U06：符号链接不得绕过共享根断言。 '''
    link = image_root / 'link'
    link.symlink_to('/etc')
    img = make_image()
    with pytest.raises(WorkspaceError) as e:
        img._normalize_image_tar(str(link / 'passwd'))
    assert e.value.code == ErrorCode.PATH_ESCAPE


# --------------------------------------------------------------------------- TC-U07

def _patch_selector(monkeypatch, rows, report_result=1):
    async def fake_find(shared_group, image_tar):
        return rows.get(image_tar)

    async def fake_report(shared_group, image_tar, status, path=None, message=None, task_id=None):
        return report_result

    monkeypatch.setattr(TrainImageSelector, 'a_find_by_group_and_tar', staticmethod(fake_find))
    monkeypatch.setattr(TrainImageSelector, 'a_report_status', staticmethod(fake_report))


def test_u07_state_machine(monkeypatch):
    ''' TC-U07：迁移矩阵 —— 允许的全部通过，deleted->loaded / 缺 path / 伪造 task_id 被拒。 '''
    img = make_image()
    tar = '/nfs-shared/hai-platform/workspace/image/demo.tar'

    _patch_selector(monkeypatch, {tar: {'status': STATUS_PROCESSING, 'task_id': 7, 'path': ''}})
    assert run(img.async_report_image_status(tar, STATUS_LOADED, path=tar, task_id=7))['status'] == STATUS_LOADED
    assert run(img.async_report_image_status(tar, STATUS_FAILED, message='boom', task_id=7))['status'] == STATUS_FAILED

    # failed 行不能再回报 loaded（必须先重新 load，见设计 §7.3）
    _patch_selector(monkeypatch, {tar: {'status': STATUS_FAILED, 'task_id': 7, 'path': ''}})
    with pytest.raises(WorkspaceError) as e:
        run(img.async_report_image_status(tar, STATUS_LOADED, path=tar, task_id=7))
    assert e.value.code == ErrorCode.ILLEGAL_TRANSITION

    # deleted -> loaded 永远非法
    _patch_selector(monkeypatch, {tar: {'status': STATUS_DELETED, 'task_id': 7, 'path': tar}})
    with pytest.raises(WorkspaceError) as e:
        run(img.async_report_image_status(tar, STATUS_LOADED, path=tar, task_id=7))
    assert e.value.code == ErrorCode.ILLEGAL_TRANSITION

    # task_id 不一致 / 缺失
    _patch_selector(monkeypatch, {tar: {'status': STATUS_PROCESSING, 'task_id': 7, 'path': ''}})
    with pytest.raises(WorkspaceError) as e:
        run(img.async_report_image_status(tar, STATUS_LOADED, path=tar, task_id=9))
    assert e.value.code == ErrorCode.FORBIDDEN
    with pytest.raises(WorkspaceError) as e:
        run(img.async_report_image_status(tar, STATUS_LOADED, path=tar, task_id=None))
    assert e.value.code == ErrorCode.INVALID_PARAM

    # loaded 必须带 path
    with pytest.raises(WorkspaceError) as e:
        run(img.async_report_image_status(tar, STATUS_LOADED, path=None, task_id=7))
    assert e.value.code == ErrorCode.INVALID_PARAM

    # 行不存在
    _patch_selector(monkeypatch, {})
    with pytest.raises(WorkspaceError) as e:
        run(img.async_report_image_status(tar, STATUS_LOADED, path=tar, task_id=7))
    assert e.value.code == ErrorCode.IMAGE_NOT_FOUND


# --------------------------------------------------------------------------- TC-U08

def test_u08_status_literals():
    ''' TC-U08：任务侧精确 loaded（HC-03）+ 客户端子串 deleted（HC-04）。 '''
    assert STATUS_LOADED == 'loaded'
    assert 'deleted' in STATUS_DELETED
    assert STATUS_PROCESSING in REPORTABLE_FROM and STATUS_LOADING in REPORTABLE_FROM
    assert STATUS_LOADED not in REPORTABLE_FROM
    assert set(ALL_STATUSES) == {'processing', 'loading', 'loaded', 'failed', 'deleted'}


def test_u08b_load_result_image_url():
    ''' Checklist 附录 A.1：load 响应的 image 是**完整三段 URL**（用户原样传给 -i），
    train_image.image 列仍是裸名（另以 image_name 返回）。 '''
    result = UserImage._load_result(
        {'image': 'demo:v1', 'registry': 'registry.high-flyer.cn', 'shared_group': 'hfai',
         'image_tar': '/r/demo.tar', 'status': 'loaded', 'task_id': 0, 'path': '/r/demo.tar'},
        backend='register')
    assert result['image'] == 'registry.high-flyer.cn/hfai/demo:v1'
    assert result['image_name'] == 'demo:v1'
    assert result['path'] == '/r/demo.tar'


# --------------------------------------------------------------------------- TC-U09

def test_u09_delete_group_isolation(monkeypatch):
    ''' TC-U09：跨组删除 FORBIDDEN、非 3 段 INVALID_PARAM；域层不接受调用方传组。 '''
    img = make_image(shared_group='haigraph', user_name='U-C')
    called = {}

    async def fake_delete(shared_group, image):
        called['args'] = (shared_group, image)
        return 2

    monkeypatch.setattr(TrainImageSelector, 'a_delete_by_group_image', staticmethod(fake_delete))

    with pytest.raises(WorkspaceError) as e:
        run(img.async_delete('registry.high-flyer.cn/hfai/demo:v1'))
    assert e.value.code == ErrorCode.FORBIDDEN
    assert called == {}

    for bad in ['demo:v1', 'a/b/c/d', '', None]:
        with pytest.raises(WorkspaceError) as e:
            run(img.async_delete(bad))
        assert e.value.code == ErrorCode.INVALID_PARAM

    result = run(img.async_delete('registry.high-flyer.cn/haigraph/demo:v1'))
    assert result['deleted'] == 2
    assert called['args'] == ('haigraph', 'demo:v1')


# --------------------------------------------------------------------------- TC-U10

def test_u10_json_normalization():
    ''' TC-U10：numpy 标量 / 时间 / NaN 归一化后必须能被 json.dumps（修 I8）。 '''
    row = {'task_id': np.int64(7), 'quota': np.float64(1.5), 'flag': np.bool_(True),
           'updated_at': pd.Timestamp('2026-10-02T18:00:00'), 'missing': np.nan, 'text': 'x'}
    out = {k: _json_scalar(v) for k, v in row.items()}
    assert type(out['task_id']) is int and out['task_id'] == 7
    assert type(out['quota']) is float
    assert type(out['flag']) is bool
    assert isinstance(out['updated_at'], str)
    assert out['missing'] is None
    json.dumps(out)


# --------------------------------------------------------------------------- TC-U11

def test_u11_list_order_is_desc(monkeypatch):
    ''' TC-U11：updated_at DESC —— 客户端取首个为基准，DESC 才等于「以最新为准」（修 I7）。 '''
    from server_model.user_data import TrainImageTable
    df = pd.DataFrame([
        {'image_tar': '/root/a.tar', 'image': 'demo:v1', 'path': '/root/a.tar',
         'shared_group': 'hfai', 'registry': 'r', 'status': 'loaded', 'task_id': 1,
         'message': '', 'user_name': 'U-A',
         'created_at': pd.Timestamp('2026-10-01T00:00:00'), 'updated_at': pd.Timestamp('2026-10-01T00:00:00')},
        {'image_tar': '/root/b.tar', 'image': 'demo:v1', 'path': '/root/b.tar',
         'shared_group': 'hfai', 'registry': 'r', 'status': 'deleted', 'task_id': 2,
         'message': '', 'user_name': 'U-A',
         'created_at': pd.Timestamp('2026-10-02T00:00:00'), 'updated_at': pd.Timestamp('2026-10-02T00:00:00')},
        {'image_tar': '/root/c.tar', 'image': 'other:v1', 'path': '/root/c.tar',
         'shared_group': 'hfai', 'registry': 'r', 'status': 'loaded', 'task_id': 3,
         'message': '', 'user_name': 'U-A',
         'created_at': pd.Timestamp('2026-09-30T00:00:00'), 'updated_at': pd.Timestamp('2026-09-30T00:00:00')},
    ])
    monkeypatch.setattr(TrainImageTable, 'async_df', _Awaitable(df))
    rows = run(TrainImageSelector.a_find_user_group_images('hfai'))
    assert [r['image_tar'] for r in rows] == ['/root/b.tar', '/root/a.tar', '/root/c.tar']
    assert type(rows[0]['task_id']) is int
    assert isinstance(rows[0]['updated_at'], str)
    json.dumps(rows)


# --------------------------------------------------------------------------- TC-U12

def test_u12_self_check_never_raises(monkeypatch):
    ''' TC-U12：image_self_check 逐项告警、不抛异常、不阻断启动。 '''
    monkeypatch.setattr('conf.utils.get_image_path', lambda: '/no/such/image_root')
    bad = {
        'cloud.storage.service.image_path': '/no/such/image_root',
        'image.registry': '',
        'image.loader_backend': 'bogus',
        'image.load_helper_image': '',
        'image.enabled': False,
    }
    monkeypatch.setattr(image_context, 'cfg', lambda path, default=None: bad.get(path, default))
    result = image_context.image_self_check()
    assert result['ok'] is False
    assert any('image_root' in p for p in result['problems'])
    assert any('loader_backend' in p for p in result['problems'])
    assert any('load_helper_image' in p for p in result['problems'])
    # registry 为空是**回退默认值**（CMP-04：保留 registry.high-flyer.cn），不是自检告警项
    assert result['registry'] == 'registry.high-flyer.cn'
    assert result['loader_backend'] == 'register'  # 非法值回退 P0 默认
    assert result['enabled'] is False


# --------------------------------------------------------------------------- 运行面脚本静态检查

def test_link_script_contract():
    ''' DEV-13/DEV-14：link 脚本存在、语法通过、只读 env（不接受用户可控参数）。 '''
    script = os.path.join(REPO_ROOT, 'marsv2', 'scripts', 'link_hfai_image.sh')
    assert os.path.isfile(script)
    with open(script, 'r', encoding='utf-8') as f:
        content = f.read()
    assert subprocess.run(['sh', '-n', script], capture_output=True).returncode == 0
    assert 'HFAI_IMAGE' in content and 'HFAI_IMAGE_WEKA_PATH' in content
    # 不接受任何用户可控命令行参数：显式拒绝 + 不使用 $1
    assert '"$#" -ne 0' in content and '不接受任何命令行参数' in content
    assert '$1' not in content and '"$*"' not in content
    assert 'exit 0' in content
