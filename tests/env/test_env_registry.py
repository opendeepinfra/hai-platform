# -*- coding: utf-8 -*-
'''
`hai-cli env`（haienv）服务端 L1 单元测试 —— 用例集 docs/haiplatform/env/env-server-test-cases.md §4.1 / §4.3 / §4.5。

覆盖：
  U 组（TC-U01 ~ TC-U14）：路径纯函数 / 名称校验 / 路径推导 / 注册 / 幂等 / 越界 / 回读 / 自检
  P 组（TC-P01/P02/P03/P05）：数据面落盘路径与运行时搜索根三方一致
  REG 组（TC-REG-01/02/03/07/08）：注册表表结构、BLOB、客户端可反序列化、并发

约束（用例集 §1.2 第 5 条 / Checklist UT-02）：**不 mock `HaienvConfig` / `SqliteDict`**，
注册表读写一律走真实安装的 `haienv` 包。

运行（在 hai-platform-0 pod 内，cwd=/high-flyer/code/multi_gpu_runner_server）：
    python3 -m pytest tests/env/test_env_registry.py -v
'''

import os
import pickle
import sqlite3
import threading
import time

import pytest
from munch import Munch

from conf import CONF
from conf.utils import (ENV_NAME_RE, get_env_dir_name, get_env_path, get_env_registry_path,
                        get_env_root, get_user_env_dir, normalize_env_path)
from cloud_storage.service import env_registry
from cloud_storage.service.env_registry import (validate_env_name, derive_env_path_sync,
                                                register_env_sync, env_registry_self_check)
from cloud_storage.service.errors import WorkspaceError, ErrorCode


GROUP = 'test-group'


class _FakeUser:
    '''最小 User 替身：领域层只用 user_name / shared_group / in_any_group。'''

    def __init__(self, user_name):
        self.user_name = user_name
        self.shared_group = GROUP

    def in_any_group(self, groups):
        return True


def _set_conf(dotted_path, value):
    node = CONF
    parts = dotted_path.split('.')
    for part in parts[:-1]:
        if part not in node or not isinstance(node[part], dict):
            node[part] = Munch()
        node = node[part]
    node[parts[-1]] = value


def _get_conf(dotted_path, default=None):
    node = CONF
    for part in dotted_path.split('.'):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node


@pytest.fixture
def env_conf(tmp_path):
    '''把 env_path 指到 tmp_path/hai-test（= 用例集 §2.1 的 /tmp/hai-test 等价物）。'''
    root = tmp_path / 'hai-test'
    (root / 'hfai_envs').mkdir(parents=True)
    backup = {key: _get_conf(key) for key in (
        'cloud.storage.service.env_path',
        'cloud.storage.service.env_push_enabled',
        'cloud.storage.service.env_push_enabled_users',
        'cloud.storage.service.env_push_enabled_groups',
        'cloud.storage.service.env_name_regex',
        'cloud.storage.service.env_register_isdir_wait_seconds',
    )}
    _set_conf('cloud.storage.service.env_path', str(root))
    _set_conf('cloud.storage.service.env_push_enabled', True)
    _set_conf('cloud.storage.service.env_push_enabled_users', [])
    _set_conf('cloud.storage.service.env_push_enabled_groups', [])
    _set_conf('cloud.storage.service.env_name_regex', ENV_NAME_RE.pattern)
    # 单测里不需要等 NFS 属性缓存（生产默认 10s，见 env_registry._isdir_wait_seconds）
    _set_conf('cloud.storage.service.env_register_isdir_wait_seconds', 0)
    yield root
    for key, value in backup.items():
        _set_conf(key, value if value is not None else '')


@pytest.fixture
def user_a(env_conf):
    user_dir = env_conf / 'hfai_envs' / 'U-A'
    user_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(str(user_dir), 0o777)
    return _FakeUser('U-A')


def _make_prefix(user_dir, dir_name, py='3.8'):
    '''构造 conda prefix 骨架 + 环境内独有探针包（用例集 §2.5 / §2.6）。'''
    prefix = os.path.join(str(user_dir), dir_name)
    site_packages = os.path.join(prefix, 'lib', f'python{py}', 'site-packages')
    os.makedirs(site_packages, exist_ok=True)
    with open(os.path.join(prefix, 'activate'), 'w') as f:
        f.write('export HF_ENV_NAME=%s\n' % dir_name.rsplit('_', 1)[0])
    probe = os.path.join(site_packages, 'haienv_probe_unique')
    os.makedirs(probe, exist_ok=True)
    with open(os.path.join(probe, '__init__.py'), 'w') as f:
        f.write("VALUE = 'env-push-ok'\n")
    return prefix


# =========================================================================== U 组

def test_tc_u01_path_functions(env_conf):
    '''TC-U01：env_path / env_root / user_env_dir / registry 同源且前缀一致。'''
    assert get_env_path() == os.path.normpath(str(env_conf))
    env_root = get_env_root()
    assert env_root == os.path.join(os.path.normpath(str(env_conf)), 'hfai_envs')
    assert get_user_env_dir('U-A') == os.path.join(env_root, 'U-A')
    assert get_env_registry_path('U-A') == os.path.join(env_root, 'U-A', 'venv.db')
    assert get_env_dir_name('myenv', 2) == 'myenv_2'


def test_tc_u02_normalize(env_conf):
    '''TC-U02：尾斜杠 / 重复分隔符 / 相对路径归一化；缺省取 /hf_shared 且不出现 //。'''
    assert normalize_env_path(str(env_conf) + '/') == os.path.normpath(str(env_conf))
    assert '//' not in os.path.join(normalize_env_path(str(env_conf) + '//'), 'hfai_envs')
    assert normalize_env_path('') == '/hf_shared'
    assert normalize_env_path(None) == '/hf_shared'
    assert os.path.isabs(normalize_env_path('relative/dir'))


def test_tc_u03_validate_env_name():
    '''TC-U03：合法集原样返回，非法集全部 INVALID_PARAM（含 '..' 与 '/'）。'''
    legal = ['a', 'myenv', 'my-env', 'a.b_c', 'A1']
    for name in legal:
        assert validate_env_name(name) == name
    illegal = ['', '.', '..', 'a/b', '../x', '/abs', 'a b', 'a' * 65, '中文名', 'a\nb',
               'a\\b', 'a..b', ' a']
    for name in illegal:
        with pytest.raises(WorkspaceError) as exc:
            validate_env_name(name)
        assert exc.value.code == ErrorCode.INVALID_PARAM, name


def test_tc_u04_derive_reuse_registry(user_a):
    '''TC-U04：注册表命中则复用其 path，exists=true，不重新分配后缀。'''
    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'myenv_0')
    register_env_sync(user_a, 'myenv', prefix, '3.8')
    result = derive_env_path_sync(user_a, 'myenv', '3.8')
    assert result['exists'] is True
    assert result['path'] == prefix
    # C-6：cloud_path 是对象存储 key 前缀（客户端 --env_remote_path），basename 必须等于目录名
    assert result['cloud_path'] == f'{GROUP}/shared/hfai_envs/U-A/myenv_0', result


def test_tc_u05_derive_allocates_suffix(user_a):
    '''TC-U05（N3 修订）：未注册且磁盘上没有同名目录 → 取 `_0`；已有同名目录 → 复用（幂等重试）。

    语义变更（N3）：旧实现无条件取「第一个空闲后缀」，于是「上传成功但注册失败」后重试会
    分配 `_1` 并把整份环境重传一遍。现在优先复用磁盘上已存在的同名目录（与客户端
    `get_haienv_path` 的复用语义一致）。
    '''
    user_dir = get_user_env_dir('U-A')
    result = derive_env_path_sync(user_a, 'new1', '3.8')
    assert result['exists'] is False
    assert result['path'] == os.path.join(user_dir, 'new1_0')
    assert result['reused'] is False

    os.makedirs(os.path.join(user_dir, 'new1_0'), exist_ok=True)
    retry = derive_env_path_sync(user_a, 'new1', '3.8')
    assert retry['exists'] is False
    assert retry['path'] == os.path.join(user_dir, 'new1_0')
    assert retry['reused'] is True
    assert retry['cloud_path'] == f'{GROUP}/shared/hfai_envs/U-A/new1_0', retry
    assert os.path.basename(retry['cloud_path']) == os.path.basename(retry['path'])
    # 预检只读：不得创建任何目录（new1_0 是测试自己建的，new2 应完全不存在）
    assert not os.path.exists(os.path.join(user_dir, 'new2_0'))
    assert derive_env_path_sync(user_a, 'new2', '3.8')['path'].endswith('new2_0')
    assert not os.path.exists(os.path.join(user_dir, 'new2_0'))


def test_tc_u06_derive_not_writable(env_conf, user_a):
    '''TC-U06：用户目录不可写 → ENV_REGISTRY_NOT_WRITABLE（ADR-E5 前移失败）。'''
    if os.geteuid() == 0:
        pytest.skip('以 root 运行，chmod 555 无法制造不可写场景（FI-01 需非 root 账号）')
    user_dir = get_user_env_dir('U-A')
    os.chmod(user_dir, 0o555)
    try:
        with pytest.raises(WorkspaceError) as exc:
            derive_env_path_sync(user_a, 'x', '3.8')
        assert exc.value.code == ErrorCode.ENV_REGISTRY_NOT_WRITABLE
    finally:
        os.chmod(user_dir, 0o777)


def test_tc_u06b_derive_probe_failure(user_a, monkeypatch):
    '''TC-U06/FI-01：写权限探测失败（无论进程是否 root）→ ENV_REGISTRY_NOT_WRITABLE。'''
    import tempfile as _tempfile

    def _boom(*args, **kwargs):
        raise PermissionError(13, 'Permission denied')

    monkeypatch.setattr(_tempfile, 'mkstemp', _boom)
    monkeypatch.setitem(env_registry._probe_writable.__globals__, 'tempfile', _tempfile)
    with pytest.raises(WorkspaceError) as exc:
        derive_env_path_sync(user_a, 'x', '3.8')
    assert exc.value.code == ErrorCode.ENV_REGISTRY_NOT_WRITABLE


def test_tc_u07_register_and_read_back(user_a):
    '''TC-U07/U12：注册成功、字段正确，且能用客户端 Haienv 反序列化（CMP-03）。'''
    from haienv.client.model import Haienv

    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'myenv2_0')
    result = register_env_sync(user_a, 'myenv2', prefix, '3.8',
                               ['/opt/x'], ['/opt/bin'], ['TEMP=temp'])
    assert result['registered'] is True
    assert result['path'] == prefix
    assert result['db'] == get_env_registry_path('U-A')

    db_path = get_env_registry_path('U-A')
    got = Haienv.select(outside_db_path=db_path, haienv_name='myenv2')
    assert got is not None
    assert got.path == prefix
    assert got.py == '3.8'
    assert got.extend == 'False'
    assert list(got.extra_search_dir) == ['/opt/x']
    assert list(got.extra_search_bin_dir) == ['/opt/bin']
    assert list(got.extra_environment) == ['TEMP=temp']

    # 客户端侧（另一进程视角）读同一 DB
    from haienv.client.model import HaienvConfig
    from haienv.client.sqlite_dict import SqliteDict
    with sqlite3.connect(db_path) as db:
        row = db.execute('SELECT key, value FROM "haienv" WHERE key=?', ('myenv2',)).fetchone()
    assert row is not None
    assert isinstance(row[1], (bytes, bytearray)), 'TC-REG-02：value 必须是 BLOB（pickle）'
    assert pickle.loads(bytes(row[1])).path == prefix
    assert isinstance(SqliteDict(db_path, 'haienv')['myenv2'], HaienvConfig)


def test_tc_u08_register_idempotent(user_a):
    '''TC-U08：重复注册同名同路径只留一条记录（REPLACE）。'''
    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'dup_0')
    for _ in range(3):
        register_env_sync(user_a, 'dup', prefix, '3.8')
    db_path = get_env_registry_path('U-A')
    with sqlite3.connect(db_path) as db:
        count = db.execute('SELECT COUNT(*) FROM "haienv" WHERE key=?', ('dup',)).fetchone()[0]
    assert count == 1


def test_tc_u09_register_path_escape(user_a, tmp_path):
    '''TC-U09/TC-A15：path 不在用户目录下 → PATH_ESCAPE，且不碰注册表。'''
    evil = tmp_path / 'evil'
    evil.mkdir()
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'myenv', str(evil), '3.8')
    assert exc.value.code == ErrorCode.PATH_ESCAPE
    assert not os.path.exists(get_env_registry_path('U-A'))


def test_tc_u10_register_other_user(user_a, env_conf):
    '''TC-U10/TC-A14：path 指向他人目录 → 拒绝，且他人 DB 不被创建。'''
    other_dir = env_conf / 'hfai_envs' / 'U-B'
    other_dir.mkdir(parents=True, exist_ok=True)
    other_prefix = _make_prefix(str(other_dir), 'myenv_0')
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'myenv', other_prefix, '3.8')
    assert exc.value.code == ErrorCode.PATH_ESCAPE
    assert not os.path.exists(str(other_dir / 'venv.db'))


def test_tc_u11_register_write_failed(user_a, monkeypatch):
    '''TC-U11/TC-A16/FI-02：写库失败 → ENV_REGISTRY_WRITE_FAILED（不抛裸异常、信息含路径）。'''
    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'bad_0')

    def _boom(*args, **kwargs):
        raise OSError(13, 'Permission denied')

    monkeypatch.setattr(env_registry, '_write_registry_sync', _boom)
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'bad', prefix, '3.8')
    assert exc.value.code == ErrorCode.ENV_REGISTRY_WRITE_FAILED
    assert prefix in exc.value.msg
    assert 'token' not in exc.value.msg


def test_tc_u12_register_missing_dir(user_a):
    '''TC-REG-10 邻接：目标目录不存在 → INVALID_PARAM（不上传完就注册）。'''
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'nodir', os.path.join(get_user_env_dir('U-A'), 'nodir_0'), '3.8')
    assert exc.value.code == ErrorCode.INVALID_PARAM


def test_tc_u12b_register_requires_py(user_a):
    '''TC-REG-10：py 为空 → 明确 INVALID_PARAM，不静默写空 py。'''
    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'nopy_0')
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'nopy', prefix, '')
    assert exc.value.code == ErrorCode.INVALID_PARAM


def test_tc_u13_legacy_table_migration(user_a):
    '''TC-U13：旧版 venv / venv_config 表存在时，注册触发迁移且旧数据不丢。'''
    db_path = get_env_registry_path('U-A')
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as db:
        db.execute('CREATE TABLE "venv" (venv_name TEXT PRIMARY KEY, path TEXT, extend TEXT,'
                   ' extend_env TEXT, py TEXT)')
        db.execute('CREATE TABLE "venv_config" (venv_name TEXT PRIMARY KEY, extra_search_dir TEXT,'
                   ' extra_search_bin_dir TEXT, extra_environment TEXT)')
        db.execute('INSERT INTO "venv" VALUES (?,?,?,?,?)',
                   ('legacy', '/tmp/legacy_0', 'False', '', '3.8'))
        db.execute('INSERT INTO "venv_config" VALUES (?,?,?,?)', ('legacy', '/a:/b', '/c', 'X=1'))
        db.commit()

    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'new_0')
    register_env_sync(user_a, 'new', prefix, '3.8')

    from haienv.client.model import Haienv
    migrated = Haienv.select(outside_db_path=db_path, haienv_name='legacy')
    assert migrated is not None and migrated.path == '/tmp/legacy_0'
    assert Haienv.select(outside_db_path=db_path, haienv_name='new').path == prefix


def test_tc_u14_self_check(user_a):
    '''TC-U14/TC-P04：自检在一致时 ok=True；配置被改坏时 ok=False 且不抛异常。'''
    result = env_registry_self_check()
    assert result['ok'] is True, result
    assert result['env_root'] == get_env_root()
    assert result['cluster_base_dir'] == os.path.normpath(get_user_env_dir('__env_self_check__'))
    assert result['suggested_env_path'] == os.path.dirname(get_env_root())


def test_tc_u14b_self_check_mismatch(user_a, monkeypatch):
    '''TC-U14：人为制造不一致 → ok=False + 建议值，且不抛异常。'''
    import cloud_storage.utils as cs_utils
    monkeypatch.setattr(cs_utils, 'get_base_path',
                        lambda *a, **kw: ('/somewhere/else/probe_0', 'g/probe'))
    result = env_registry_self_check()
    assert result['ok'] is False
    assert result['cluster_base_dir'] == '/somewhere/else'
    assert result['suggested_env_path'] == os.path.dirname(get_env_root())


# =========================================================================== 灰度 / extend

def test_tc_a04_extend_rejected(user_a):
    '''TC-A04/FR-07：extend=True 一律拒绝。'''
    with pytest.raises(WorkspaceError) as exc:
        derive_env_path_sync(user_a, 'myenv', '3.8', extend='True')
    assert exc.value.code == ErrorCode.INVALID_PARAM


def test_tc_a10_feature_disabled(user_a, env_conf):
    '''TC-A10/A19/OPS-02：灰度关闭 → FEATURE_DISABLED 且不写库。'''
    _set_conf('cloud.storage.service.env_push_enabled', False)
    for call in (lambda: derive_env_path_sync(user_a, 'myenv', '3.8'),
                 lambda: register_env_sync(user_a, 'myenv', '/tmp/x', '3.8')):
        with pytest.raises(WorkspaceError) as exc:
            call()
        assert exc.value.code == ErrorCode.FEATURE_DISABLED
    assert not os.path.exists(get_env_registry_path('U-A'))


def test_tc_o04_feature_whitelist(user_a):
    '''TC-O04/OPS-02：白名单外用户被拒。'''
    _set_conf('cloud.storage.service.env_push_enabled_users', ['U-B'])
    with pytest.raises(WorkspaceError) as exc:
        derive_env_path_sync(user_a, 'myenv', '3.8')
    assert exc.value.code == ErrorCode.FEATURE_DISABLED


def test_tc_o06_name_regex_configurable(user_a):
    '''TC-O06：env_name_regex 可收紧；含 '/' 的配置被忽略（SEC-04）。'''
    _set_conf('cloud.storage.service.env_name_regex', '^[a-z][a-z0-9_]{0,15}$')
    with pytest.raises(WorkspaceError):
        validate_env_name('MyEnv')
    assert validate_env_name('small_env') == 'small_env'
    _set_conf('cloud.storage.service.env_name_regex', '^.*/.*$')
    with pytest.raises(WorkspaceError):
        validate_env_name('../etc')


# =========================================================================== P 组（路径三方一致）

def test_tc_p01_three_way_path_consistency(env_conf, user_a):
    '''TC-P01/AC-02：get_base_path(cluster) 的 dirname == dirname(HAIENV_PATH) == user_env_dir。'''
    from conf.utils import FileType
    from cloud_storage.utils import get_base_path

    cluster_base_path, cloud_base_path = get_base_path('U-A', GROUP, 'myenv', FileType.ENV)
    assert os.path.dirname(cluster_base_path) == os.path.normpath(get_user_env_dir('U-A'))
    assert cluster_base_path == os.path.join(get_user_env_dir('U-A'), 'myenv')
    # 任务侧 HAIENV_PATH 由同一函数推导（TC-T01：single_task_impl 已改用 get_user_env_dir，不再硬编码）
    _task_impl_src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))), 'server_model', 'task_impl', 'single_task_impl.py')).read()
    assert 'get_user_env_dir(self.task.user_name)' in _task_impl_src
    assert "'/hf_shared/hfai_envs/" not in _task_impl_src
    # TC-P02/CMP-05：S3 key 布局不变
    assert cloud_base_path == f'{GROUP}/shared/hfai_envs/U-A/myenv'


def test_tc_p03_production_env_path(env_conf, user_a):
    '''TC-P03：env_path=/hf_shared（生产取值）时三方仍一致。'''
    from conf.utils import FileType
    from cloud_storage.utils import get_base_path

    _set_conf('cloud.storage.service.env_path', '/hf_shared')
    cluster_base_path, _ = get_base_path('U-A', GROUP, 'myenv', FileType.ENV)
    assert get_env_root() == '/hf_shared/hfai_envs'
    assert cluster_base_path == '/hf_shared/hfai_envs/U-A/myenv'
    assert os.path.dirname(cluster_base_path) == get_user_env_dir('U-A')


def test_tc_p05_check_is_subpath(env_conf):
    '''TC-P05/SEC-04：check_is_subpath 放行子路径，拒绝 '..' 穿越。'''
    from cloud_storage.utils import check_is_subpath, ClientException
    root = get_env_root()
    check_is_subpath(root, f'{root}/U-A/x')
    with pytest.raises(ClientException):
        check_is_subpath(root, f'{root}/../etc')


# =========================================================================== REG 组（并发/幂等）

def test_tc_reg_01_table_created(user_a):
    '''TC-REG-01/02：首次写入自动建 haienv 表，value 为 BLOB。'''
    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'first_0')
    register_env_sync(user_a, 'first', prefix, '3.8')
    db_path = get_env_registry_path('U-A')
    with sqlite3.connect(db_path) as db:
        tables = {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert 'haienv' in tables
        value = db.execute('SELECT value FROM "haienv" WHERE key=?', ('first',)).fetchone()[0]
    assert isinstance(value, (bytes, bytearray))


def test_tc_reg_07_concurrent_register_different_envs(user_a):
    '''TC-REG-07/FI-03：并发注册不同 env 不丢记录、无 database is locked 泄漏。'''
    user_dir = get_user_env_dir('U-A')
    names = [f'conc{i}' for i in range(5)]
    prefixes = [_make_prefix(user_dir, f'{name}_0') for name in names]
    errors = []

    def _worker(idx):
        try:
            register_env_sync(user_a, names[idx], prefixes[idx], '3.8')
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(len(names))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    db_path = get_env_registry_path('U-A')
    with sqlite3.connect(db_path) as db:
        stored = {row[0] for row in db.execute('SELECT key FROM "haienv"')}
    assert set(names) <= stored


def test_tc_reg_08_concurrent_same_env(user_a):
    '''TC-REG-08：同 env 同 path 并发注册，记录数 = 1。'''
    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'same_0')
    errors = []

    def _worker():
        try:
            register_env_sync(user_a, 'same', prefix, '3.8')
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=_worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    db_path = get_env_registry_path('U-A')
    with sqlite3.connect(db_path) as db:
        assert db.execute('SELECT COUNT(*) FROM "haienv" WHERE key=?', ('same',)).fetchone()[0] == 1


# =========================================================================== S 组（安全）

def test_tc_s04_name_injection(user_a):
    '''TC-S04：名称注入不改变表结构、不进 SQL。'''
    for name in ["a';DROP TABLE haienv;--", 'a" OR 1=1', '../../etc']:
        with pytest.raises(WorkspaceError) as exc:
            validate_env_name(name)
        assert exc.value.code == ErrorCode.INVALID_PARAM


def test_tc_s09_oversized_lists(user_a):
    '''TC-S09 邻接：超长列表原样落库（不做隐式截断/字符串化），名称仍受白名单约束。'''
    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'big_0')
    big = [f'/opt/{i}' for i in range(1000)]
    register_env_sync(user_a, 'big', prefix, '3.8', big, [], [])
    from haienv.client.model import Haienv
    got = Haienv.select(outside_db_path=get_env_registry_path('U-A'), haienv_name='big')
    assert list(got.extra_search_dir) == big


# =========================================================================== 边界 / 防御分支（UT-01 覆盖率）

def _run(coro):
    '''不依赖 pytest-asyncio 版本：直接跑协程。'''
    import asyncio
    return asyncio.get_event_loop().run_until_complete(coro)


def test_async_wrappers(user_a):
    '''async 包装（API-11/API-13 实际入口）走线程池且行为与同步实现一致。'''
    user_dir = get_user_env_dir('U-A')
    derived = _run(env_registry.derive_env_path(user_a, 'async', '3.8'))
    assert derived['exists'] is False and derived['path'].endswith('async_0')
    prefix = _make_prefix(user_dir, 'async_0')
    result = _run(env_registry.register_env(user_a, 'async', prefix, '3.8', [], [], []))
    assert result['registered'] is True
    assert _run(env_registry.derive_env_path(user_a, 'async', '3.8'))['exists'] is True
    with pytest.raises(WorkspaceError):
        _run(env_registry.derive_env_path(user_a, 'async', '3.8', extend='True'))


def test_extend_string_forms(user_a):
    '''extend 的字符串形态（'False' / 'false' / ''）不得被误判为扩展环境。'''
    for value in ('False', 'false', '', None, False, 0):
        assert derive_env_path_sync(user_a, 'myenv', '3.8', extend=value)['exists'] is False


def test_name_regex_defensive(user_a, monkeypatch):
    '''名称白名单的防御分支：配置读取异常 / 配置含路径分隔符时退回内置白名单。'''
    monkeypatch.setitem(env_registry._name_regex.__globals__, 'get_env_name_regex',
                        lambda: (_ for _ in ()).throw(RuntimeError('boom')))
    assert env_registry._name_regex() is ENV_NAME_RE
    monkeypatch.setitem(env_registry._name_regex.__globals__, 'get_env_name_regex',
                        lambda: '^.*/.*$')
    assert env_registry._name_regex() is ENV_NAME_RE


def test_read_registry_unreadable_fail_closed(user_a):
    '''N3：注册表读不出来 ≠ 没注册过 —— 必须 fail-closed，不能静默当成空表。

    历史行为：`_read_registry` 把所有异常吞成 `{}`，于是「读不出来」被当成「没注册过」，
    预检分配新后缀 → 用户每重试一次就多传一份完整环境（103 上实测复现 `x_0 → x_1`）。
    '''
    db_path = get_env_registry_path('U-A')
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with open(db_path, 'wb') as f:
        f.write(b'this is not a sqlite database')
    with pytest.raises(WorkspaceError) as exc:
        env_registry._read_registry(_FakeUser('U-A'))
    assert exc.value.code == ErrorCode.ENV_REGISTRY_READ_FAILED
    with pytest.raises(WorkspaceError) as exc2:
        derive_env_path_sync(user_a, 'myenv', '3.8')
    assert exc2.value.code == ErrorCode.ENV_REGISTRY_READ_FAILED


def test_read_registry_partial_decode_fail_closed(user_a):
    '''N3：单个条目损坏不能让整表变空；损坏的 key 必须显式暴露，且对该名字 fail-closed。'''
    db_path = get_env_registry_path('U-A')
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    register_env_sync(user_a, 'good', _make_prefix(get_user_env_dir('U-A'), 'good_0'), '3.8')
    # 直接塞一行反序列化不了的值（等价于「别的 haienv 版本写进去的 / 数据损坏」）
    conn = sqlite3.connect(db_path)
    conn.execute('REPLACE INTO "haienv" (key, value) VALUES (?,?)',
                 ('broken', sqlite3.Binary(b'not-a-pickle')))
    conn.commit()
    conn.close()

    registry, broken = env_registry._read_registry(_FakeUser('U-A'))
    assert 'good' in registry, '一行坏掉不应让其余环境全部不可见'
    assert broken == {'broken'}

    with pytest.raises(WorkspaceError) as exc:
        derive_env_path_sync(user_a, 'broken', '3.8')
    assert exc.value.code == ErrorCode.ENV_REGISTRY_READ_FAILED

    fresh = derive_env_path_sync(user_a, 'brandnew', '3.8')
    assert fresh['exists'] is False


def test_n3_derive_reuses_existing_dir_on_retry(user_a):
    '''N3 幂等：注册表没有该名字、但磁盘上已有上一次 push 的目录 → 复用最小后缀。'''
    user_dir = get_user_env_dir('U-A')
    _make_prefix(user_dir, 'retry_0')
    result = derive_env_path_sync(user_a, 'retry', '3.8')
    assert result['path'] == os.path.join(user_dir, 'retry_0')
    assert result['reused'] is True
    # 目录在但没有注册 → exists 必须为 False（不能被当成「已可用、可跳过上传」）
    assert result['exists'] is False

    _make_prefix(user_dir, 'retry_2')
    assert derive_env_path_sync(user_a, 'retry', '3.8')['path'].endswith('retry_0')

    fresh = derive_env_path_sync(user_a, 'fresh', '3.8')
    assert fresh['path'].endswith('fresh_0') and fresh['reused'] is False


def test_n3_retry_after_register_failure_keeps_same_path(user_a, monkeypatch):
    '''N3：上传成功 + 注册失败 → 重试必须复用同一目录，不再产生 `_1` 与第二份上传。'''
    user_dir = get_user_env_dir('U-A')
    first = derive_env_path_sync(user_a, 'rt', '3.8')
    _make_prefix(user_dir, os.path.basename(first['path']))  # 模拟数据面已落盘

    calls = {'n': 0}
    real_write = env_registry._write_registry_sync

    def _flaky(*args, **kwargs):
        calls['n'] += 1
        if calls['n'] == 1:
            raise RuntimeError('boom（模拟注册表瞬时写失败）')
        return real_write(*args, **kwargs)

    monkeypatch.setattr(env_registry, '_write_registry_sync', _flaky)
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'rt', first['path'], '3.8')
    assert exc.value.code == ErrorCode.ENV_REGISTRY_WRITE_FAILED

    second = derive_env_path_sync(user_a, 'rt', '3.8')      # = 客户端重试 `env push`
    assert second['path'] == first['path']
    assert second['reused'] is True
    register_env_sync(user_a, 'rt', second['path'], '3.8')
    registry, broken = env_registry._read_registry(_FakeUser('U-A'))
    assert 'rt' in registry and not broken
    assert [d for d in os.listdir(user_dir) if d.startswith('rt_')] == ['rt_0']


def test_register_waits_for_dir_visibility(user_a):
    '''NFS 属性缓存：目录稍后才对 Pod 可见时，不应直接判「你没上传」（产品侧轮询）。'''
    _set_conf('cloud.storage.service.env_register_isdir_wait_seconds', 5)
    user_dir = get_user_env_dir('U-A')
    target = os.path.join(user_dir, 'lag_0')

    def _create_later():
        time.sleep(0.6)
        _make_prefix(user_dir, 'lag_0')

    thread = threading.Thread(target=_create_later)
    thread.start()
    try:
        register_env_sync(user_a, 'lag', target, '3.8')
    finally:
        thread.join()
    registry, _ = env_registry._read_registry(_FakeUser('U-A'))
    assert 'lag' in registry


def test_register_dir_timeout_still_rejects(user_a):
    '''等不到目录仍然报错（不放松校验），并提示可重试（重试会复用同一路径）。'''
    _set_conf('cloud.storage.service.env_register_isdir_wait_seconds', 0.2)
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'nodir', os.path.join(get_user_env_dir('U-A'), 'nodir_0'), '3.8')
    assert exc.value.code == ErrorCode.INVALID_PARAM
    assert '重试' in exc.value.msg


def test_n4_env_data_plane_gated_by_env_switch(user_a, env_conf, monkeypatch):
    '''N4：env_push_enabled=false 必须同时挡住数据面 /ugc/sync_to_cluster(file_type=env)。

    否则一级回滚只关掉了控制面（API-11/API-13），数据面仍能把整份环境写进集群共享盘。
    '''
    from cloud_storage.service import sync_to_cluster as stc
    from conf.utils import FileType

    monkeypatch.setattr(stc, 'ensure_cloud_storage_configured', lambda: None)
    monkeypatch.setattr(stc, 'check_feature_enabled', lambda user: None)
    # 让「过了开关之后」的流程立刻短路，避免依赖 Redis / 对象存储
    monkeypatch.setattr(stc, 'get_base_path',
                        lambda *a, **kw: (_ for _ in ()).throw(
                            WorkspaceError(ErrorCode.INVALID_PARAM, 'stop-after-gate')))
    real_gate = stc.check_env_push_enabled
    gated = []

    def _spy(user):
        gated.append(getattr(user, 'user_name', user))
        return real_gate(user)

    monkeypatch.setattr(stc, 'check_env_push_enabled', _spy)

    # 开关关闭：env 被拦（并且确实调用了 env 开关）
    _set_conf('cloud.storage.service.env_push_enabled', False)
    gated.clear()
    with pytest.raises(WorkspaceError) as exc:
        _run(stc.submit_to_cluster(user_a, 'myenv', FileType.ENV, []))
    assert exc.value.code == ErrorCode.FEATURE_DISABLED
    assert gated == ['U-A']

    # workspace 不受 env 开关影响（继续走到后面，被我们注入的 stop 打断）
    gated.clear()
    with pytest.raises(WorkspaceError) as exc2:
        _run(stc.submit_to_cluster(user_a, 'myws', FileType.WORKSPACE, []))
    assert exc2.value.msg == 'stop-after-gate'
    assert gated == [], 'workspace 不该受 env 开关约束'

    # 开关打开：env 也放行
    _set_conf('cloud.storage.service.env_push_enabled', True)
    gated.clear()
    with pytest.raises(WorkspaceError) as exc3:
        _run(stc.submit_to_cluster(user_a, 'myenv', FileType.ENV, []))
    assert exc3.value.msg == 'stop-after-gate'
    assert gated == ['U-A']


def test_list_used_suffixes_defensive(user_a, monkeypatch):
    '''扫描目录的两个防御分支：目录不存在 / listdir 抛异常。'''
    missing = env_registry._list_used_suffixes(_FakeUser('U-NOT-EXIST'), 'ghost')
    assert missing == set()
    monkeypatch.setattr(os, 'listdir', lambda *a, **kw: (_ for _ in ()).throw(OSError('boom')))
    assert env_registry._list_used_suffixes(_FakeUser('U-A'), 'ghost') == set()


def test_probe_writable_makedirs_failure(user_a, monkeypatch):
    '''目标目录不存在且无法创建 → ENV_REGISTRY_NOT_WRITABLE（共享盘未挂载场景）。'''
    monkeypatch.setattr(os, 'makedirs', lambda *a, **kw: (_ for _ in ()).throw(OSError('read-only fs')))
    with pytest.raises(WorkspaceError) as exc:
        env_registry._probe_writable('/nonexistent-hai/envs/U-A')
    assert exc.value.code == ErrorCode.ENV_REGISTRY_NOT_WRITABLE


def test_write_registry_read_back_mismatch(user_a, monkeypatch):
    '''设计 §5.2：写入后回读校验失败必须报错，不得静默 success。'''
    import haienv.client.model as haienv_model
    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'rb_0')
    db_path = get_env_registry_path('U-A')

    monkeypatch.setattr(haienv_model.Haienv, 'select', classmethod(lambda cls, **kw: None))
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'rb', prefix, '3.8')
    assert exc.value.code == ErrorCode.ENV_REGISTRY_WRITE_FAILED

    class _Wrong:
        path = '/somewhere/else'

    monkeypatch.setattr(haienv_model.Haienv, 'select', classmethod(lambda cls, **kw: _Wrong()))
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'rb2', prefix, '3.8')
    assert exc.value.code == ErrorCode.ENV_REGISTRY_WRITE_FAILED
    assert db_path  # 路径信息在 msg 中，不泄漏 token


def test_failure_reason_classification():
    '''指标标签 reason 的分类（NFR-05 / TC-L03）。'''
    assert env_registry._failure_reason(OSError(13, 'Permission denied')) == 'permission'
    assert env_registry._failure_reason(RuntimeError('database is locked')) == 'locked'
    assert env_registry._failure_reason(ImportError('No module named x')) == 'import'
    assert env_registry._failure_reason(RuntimeError('回读校验失败')) == 'assert'
    assert env_registry._failure_reason(ValueError('whatever')) == 'unknown'


def test_register_param_errors(user_a):
    '''path 为空 / path 等于用户目录本身 → 明确错误码。'''
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'myenv', '', '3.8')
    assert exc.value.code == ErrorCode.INVALID_PARAM
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'myenv', get_user_env_dir('U-A'), '3.8')
    assert exc.value.code == ErrorCode.PATH_ESCAPE


def test_register_subpath_unexpected_error(user_a, monkeypatch):
    '''check_is_subpath 抛非 ClientException 时也归类为 PATH_ESCAPE（不抛裸 500）。'''
    import cloud_storage.utils as cs_utils
    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'weird_0')
    monkeypatch.setattr(cs_utils, 'check_is_subpath',
                        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('weird')))
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'weird', prefix, '3.8')
    assert exc.value.code == ErrorCode.PATH_ESCAPE

    monkeypatch.setattr(cs_utils, 'check_is_subpath',
                        lambda *a, **kw: (_ for _ in ()).throw(
                            WorkspaceError(ErrorCode.PATH_ESCAPE, 'inner')))
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'weird2', prefix, '3.8')
    assert exc.value.code == ErrorCode.PATH_ESCAPE


def test_register_write_workspace_error_passthrough(user_a, monkeypatch):
    '''_write_registry_sync 抛 WorkspaceError 时原样上抛（保留 code）。'''
    user_dir = get_user_env_dir('U-A')
    prefix = _make_prefix(user_dir, 'pass_0')

    def _boom(*args, **kwargs):
        raise WorkspaceError(ErrorCode.ENV_REGISTRY_NOT_WRITABLE, 'inner')

    monkeypatch.setattr(env_registry, '_write_registry_sync', _boom)
    with pytest.raises(WorkspaceError) as exc:
        register_env_sync(user_a, 'pass', prefix, '3.8')
    assert exc.value.code == ErrorCode.ENV_REGISTRY_NOT_WRITABLE


def test_self_check_exception_path(user_a, monkeypatch):
    '''配置缺失导致 get_base_path 抛异常 → ok=False + error 字段，不抛异常。'''
    import cloud_storage.utils as cs_utils
    monkeypatch.setattr(cs_utils, 'get_base_path',
                        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('no config')))
    result = env_registry_self_check()
    assert result['ok'] is False
    assert 'no config' in result['error']

