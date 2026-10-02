'''
haienv（`hai-cli env push`）领域层 —— 设计 docs/haiplatform/env/env-server-design.md §5.2。

职责：
- `validate_env_name`     名称白名单校验（FR-08 / SEC-04）
- `derive_env_path`       API-11 预检：注册表命中则复用，否则分配第一个空闲后缀（FR-03）
- `register_env`          API-13 注册：flock + REPLACE 写入 {user_env_dir}/venv.db 的 haienv 表并回读（FR-04）
- `env_registry_self_check` OPS-01 启动自检：env_root 与 get_base_path(FileType.ENV) 是否同源

硬约束：
- HC-06：本模块**不 import fastapi**、不注册路由；`cloud_storage.utils` 惰性 import
- ADR-E4：注册表写入**复用镜像内 haienv 包**（惰性 import），不手写 pickle
- NFR-04：同步 I/O 由调用方（或本模块的 async 包装）走线程池，不阻塞事件循环
- HC-05：失败一律抛 WorkspaceError（带 code），不抛裸 500
'''

import asyncio
import fcntl
import functools
import os
import re
import tempfile
import time
from contextlib import contextmanager

from logm import logger

from conf.utils import ENV_NAME_RE, get_env_dir_name

from .context import (get_env_name_regex, get_env_registry_path, get_env_root, get_user_env_dir,
                      check_env_push_enabled)
from .errors import WorkspaceError, ErrorCode

# '..' / 空白 / 绝对路径的显式兜底（正则本身已覆盖，这里保证即使正则被改坏也不会放行）
_FORBIDDEN_NAME_CHARS = ('/', '\\', '\x00')


async def _to_thread(func, *args):
    '''
    把同步 I/O 放到默认线程池执行（NFR-04）。

    注意：**不能**用 `asyncio.to_thread` —— 它要求 Python >= 3.9，
    而平台镜像（one/release.sh）与 ugc-server 都是 python3.8。
    '''
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args))


def _name_regex() -> 're.Pattern':
    pattern = ENV_NAME_RE.pattern
    try:
        pattern = get_env_name_regex() or pattern
        compiled = re.compile(pattern)
    except Exception:
        compiled = ENV_NAME_RE
    # 配置被改坏（含 '/'）时退回内置白名单，绝不接受路径分隔符
    if '/' in compiled.pattern or '\\' in compiled.pattern:
        compiled = ENV_NAME_RE
    return compiled


def validate_env_name(env_name) -> str:
    '''
    名称白名单校验：合法返回原值，非法抛 WorkspaceError(INVALID_PARAM)（FR-08 / SEC-04）。
    '''
    if env_name is None or (isinstance(env_name, str) and env_name.strip() == ''):
        raise WorkspaceError(ErrorCode.INVALID_PARAM, 'venv_name 不能为空')
    name = str(env_name)
    if any(ch in name for ch in _FORBIDDEN_NAME_CHARS) or '..' in name:
        raise WorkspaceError(ErrorCode.INVALID_PARAM,
                             f'venv_name 取值非法: {name}（不允许包含 "/"、"\\\\"、".."）')
    if name != name.strip():
        raise WorkspaceError(ErrorCode.INVALID_PARAM, f'venv_name 取值非法: {name}（不允许空白字符）')
    if not _name_regex().fullmatch(name):
        raise WorkspaceError(ErrorCode.INVALID_PARAM, f'venv_name 取值非法: {name}')
    return name


def _user_name(user) -> str:
    '''日志字段统一用 user_name（与仓库既有 `user={user.user_name}` 约定一致，NFR-05/TC-L04）。'''
    return str(getattr(user, 'user_name', user))


def _truthy(value) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'y', 'on')


def _reject_extend(extend):
    '''FR-07：与服务端/客户端一致，拒绝 extend 环境上传。'''
    if _truthy(extend):
        raise WorkspaceError(ErrorCode.INVALID_PARAM, '暂不支持上传 extend 模式的虚拟环境')


# --------------------------------------------------------------------------- 读注册表

def _haienv_version() -> str:
    '''
    当前进程内 `haienv` 包的版本（ADR-E4 的版本耦合必须是可观测的）。

    只用于日志与客户端兼容性提示，取不到时返回 'unknown'，绝不抛异常。
    '''
    try:
        from importlib.metadata import version as _pkg_version  # py3.8+
        return str(_pkg_version('haienv'))
    except Exception:
        pass
    try:
        import haienv
        return str(getattr(haienv, '__version__', 'unknown') or 'unknown')
    except Exception:
        return 'unknown'


def _read_registry_rowwise(db_path: str) -> tuple:
    '''
    逐行读取 `haienv` 表，跳过无法反序列化的行，返回 `(registry, broken_keys)`。

    只在整表 `Haienv.select` 失败时兜底调用：**单个**条目损坏/版本不一致，不应该让
    「其余环境」也一起变得不可见（那会直接导致重复上传，N3）。
    结构性问题（打不开库 / 表读不出来）抛 ENV_REGISTRY_READ_FAILED（fail-closed）。
    '''
    import sqlite3
    try:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    except Exception as e:
        raise WorkspaceError(
            ErrorCode.ENV_REGISTRY_READ_FAILED,
            f'读取集群侧注册表失败（无法打开 {db_path}）: {e}；为避免把「读不出来」误判成'
            f'「没注册过」而产生重复环境，本次请求已停止，请运维按手册检查该文件')
    try:
        try:
            rows = conn.execute('SELECT key, value FROM "haienv" ORDER BY rowid').fetchall()
        except Exception as e:
            if 'no such table' in str(e).lower():
                logger.warning(f'[ENV] 注册表无 haienv 表（旧版 venv 表未迁移？）db={db_path}: {e}')
                return {}, set()
            raise WorkspaceError(
                ErrorCode.ENV_REGISTRY_READ_FAILED,
                f'读取集群侧注册表失败（{db_path}）: {e}；为避免产生重复环境，本次请求已停止，'
                f'请运维按手册检查该文件')
    finally:
        try:
            conn.close()
        except Exception:
            pass

    registry, broken = {}, set()
    for key, blob in rows:
        try:
            from haienv.client.sqlite_dict import SqliteDict
            registry[key] = SqliteDict.decode(blob)
        except Exception as e:
            broken.add(key)
            logger.error(f'[ENV] 注册表条目无法反序列化 key={key} db={db_path}: {e}')
    return registry, broken


def _read_registry(user) -> tuple:
    '''
    读 `{user_env_dir}/venv.db` 的 haienv 表，返回 `(registry, broken_keys)`。

    - db 不存在      → `({}, set())`；只读、不创建任何文件（避免 sqlite3.connect 的建文件副作用）
    - 正常           → `(全部条目, set())`
    - 部分行读不出来 → `(能读出来的条目, 读不出来的 key 集合)`，并计入读失败指标
    - 整表读不出来   → 抛 `ENV_REGISTRY_READ_FAILED`

    为什么必须 fail-closed（N3）：以前这里把所有异常都吞成 `{}`，于是「注册表读不出来」
    等价于「这个名字没注册过」→ 预检分配新后缀 → 用户每重试一次就多传一份完整环境
    （已在 103 实测复现 `xxx_0 → xxx_1`）。宁可明确失败，也不要静默产生重复环境。
    '''
    from cloud_storage.metrics import env_registry_read_failures_total

    db_path = get_env_registry_path(user)
    if not os.path.exists(db_path):
        return {}, set()
    try:
        from haienv.client.model import Haienv
        result = Haienv.select(outside_db_path=db_path)
        return (result or {}), set()
    except Exception as e:
        env_registry_read_failures_total.labels(reason='decode_all').inc()
        logger.warning(f'[ENV] 整表读取注册表失败，降级为逐行读取 user={_user_name(user)} '
                       f'db={db_path}: {e}')
        registry, broken = _read_registry_rowwise(db_path)
        if broken:
            env_registry_read_failures_total.labels(reason='decode_partial').inc()
        return registry, broken


def _list_used_suffixes(user, env_name: str) -> set:
    '''
    扫描 {user_env_dir}/{name}_<int>，返回已占用的后缀集合。
    与客户端 get_haienv_path（plugins/haienv/haienv/client/model.py:140-163）保持一致。
    '''
    user_dir = get_user_env_dir(user)
    used = set()
    if not os.path.isdir(user_dir):
        return used
    prefix = f'{env_name}_'
    try:
        entries = os.listdir(user_dir)
    except Exception as e:
        logger.warning(f'[ENV] 扫描 env 目录失败 dir={user_dir}: {e}')
        return used
    for entry in entries:
        if entry.startswith(prefix):
            suffix = entry[len(prefix):]
            if suffix.isdigit():
                used.add(int(suffix))
    return used


# --------------------------------------------------------------------------- 写权限探测（ADR-E5）

def _probe_writable(path: str):
    '''
    在目标目录下 mkstemp + 删除，失败即 ENV_REGISTRY_NOT_WRITABLE（SEC-03 / AC-06 前移失败）。
    '''
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        raise WorkspaceError(
            ErrorCode.ENV_REGISTRY_NOT_WRITABLE,
            f'集群侧 env 目录 {path} 不存在且无法创建（{e}），请运维检查共享盘挂载与目录权限')
    try:
        fd, tmp = tempfile.mkstemp(dir=path, prefix='.env_write_probe_')
        os.close(fd)
        os.remove(tmp)
    except Exception as e:
        raise WorkspaceError(
            ErrorCode.ENV_REGISTRY_NOT_WRITABLE,
            f'集群侧 env 目录 {path} 不可写（{e}），请运维检查目录权限（建议 chmod 777 该用户 env 目录）')


# --------------------------------------------------------------------------- 目录可见性（NFS）

_ENV_DIR_WAIT_DEFAULT_SECONDS = 10.0
_ENV_DIR_WAIT_MAX_SECONDS = 120.0
_ENV_DIR_WAIT_INTERVAL_SECONDS = 0.5


def _isdir_wait_seconds() -> float:
    '''
    目录可见性等待上限（秒），可用 cloud.storage.service.env_register_isdir_wait_seconds 调整。
    0 = 只看一次（旧行为）。
    '''
    from .context import cfg
    try:
        value = float(cfg('cloud.storage.service.env_register_isdir_wait_seconds',
                          default=_ENV_DIR_WAIT_DEFAULT_SECONDS))
    except Exception:
        value = _ENV_DIR_WAIT_DEFAULT_SECONDS
    return max(0.0, min(value, _ENV_DIR_WAIT_MAX_SECONDS))


def _wait_for_dir(path: str, timeout: float = None) -> bool:
    '''
    目录可见性探测（产品侧兜底，而不是只在脚本里轮询）。

    宿主与 Pod 是**不同的 NFS 客户端**，属性/负缓存彼此不同步（`lookupcache=all`、
    `acdirmax` 默认 60s）：刚上传完落盘的目录，在 Pod 侧可能短暂看不到。这里做一次
    有上限的轮询（默认 10s），把「缓存还没过期」与「用户真的没上传」区分开；
    超时后仍然报错（不放松任何路径校验）。
    '''
    if os.path.isdir(path):
        return True
    if timeout is None:
        timeout = _isdir_wait_seconds()
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(min(_ENV_DIR_WAIT_INTERVAL_SECONDS, max(0.05, deadline - time.time())))
        if os.path.isdir(path):
            return True
    return False


# --------------------------------------------------------------------------- API-11 预检

def _cloud_path_for(user, dir_name: str) -> str:
    '''
    该 env 对应的**对象存储 key 前缀** = `{group}/shared/hfai_envs/{user}/{dir_name}`

    为什么 API-11 必须把它返回给客户端：客户端的 `--env_remote_path` 不是集群文件系统路径，
    而是对象存储的 key 前缀（`workspace_util.upload_files` 里 `dst_file = f'{remote_path}/{f.path}'`）。
    集群侧落盘路径由服务端用同一个 `get_base_path(..., FileType.ENV)` 自行推导
    （`sync_to_cluster.submit_to_cluster`），两边必须一致（CMP-05：S3 key 布局不变）。
    '''
    try:
        from cloud_storage.utils import get_base_path
        from conf.utils import FileType
        _, cloud_base_path = get_base_path(_user_name(user), getattr(user, 'shared_group', '') or '',
                                           dir_name, FileType.ENV)
        return cloud_base_path
    except Exception as e:
        logger.warning(f'[ENV] 计算 cloud_path 失败 user={_user_name(user)} dir={dir_name}: {e}')
        return ''


def derive_env_path_sync(user, venv_name, py=None, extend=None) -> dict:
    '''
    API-11 领域实现（同步）。只读 + 写权限探测，**不写注册表、不创建 env 目录**。

    返回 `{'path': <集群绝对路径>, 'exists': bool, 'reused': bool,
           'cloud_path': <对象存储 key 前缀>, 'haienv_version': <str>}`：
    - `path`       给客户端做展示与 API-13 注册（集群侧落盘目录）
    - `exists`     **是否已注册可用**（不是「目录在不在」）；目录在但没注册仍为 False，
                   因为那种环境下 `source haienv` 取不到配置，不能让客户端以为可以跳过上传
    - `reused`     本次是否复用了上一次 push 的目录（幂等重试）
    - `cloud_path` 给客户端做 `--env_remote_path`（对象存储 key 前缀，basename 必须等于目录名）
    - `haienv_version` 集群侧 haienv 版本，供客户端做版本偏移提示（ADR-E4 / CMP-04）
    '''
    check_env_push_enabled(user)
    env_name = validate_env_name(venv_name)
    _reject_extend(extend)

    registry, broken = _read_registry(user)
    if env_name in registry:
        registered_path = getattr(registry[env_name], 'path', None)
        if registered_path:
            dir_name = os.path.basename(os.path.normpath(registered_path))
            return {'path': registered_path, 'exists': True, 'reused': True,
                    'cloud_path': _cloud_path_for(user, dir_name),
                    'haienv_version': _haienv_version()}
    if env_name in broken:
        # N3 fail-closed：条目存在但读不出来（版本不一致/数据损坏）。此时**不能**当作
        # 「没注册过」去分配新后缀 —— 那会再上传一份完整环境，并把注册表指向新目录。
        raise WorkspaceError(
            ErrorCode.ENV_REGISTRY_READ_FAILED,
            f'环境 {env_name} 在集群侧注册表中存在，但该条目当前无法读取'
            f'（多为服务端/客户端 haienv 版本不一致或条目损坏）；为避免产生重复环境，'
            f'本次 push 已停止。请运维按手册检查 {get_env_registry_path(user)}')

    user_dir = get_user_env_dir(user)
    _probe_writable(user_dir)

    used = _list_used_suffixes(user, env_name)
    if used:
        # 幂等重试（N3）：注册表里没有这个名字，但磁盘上已经有 `{name}_{i}` 目录 —— 那是
        # 上一次 push 留下的（上传成功但注册失败、或客户端中断）。复用**最小后缀**，让重试
        # 写回同一目录、同一批对象 key，而不是分配新后缀把整份环境重传一遍。
        # 语义与客户端 `get_haienv_path`（同名目录已存在则复用）一致。
        suffix = min(used)
        reused = True
    else:
        suffix = 0
        reused = False
    dir_name = get_env_dir_name(env_name, suffix)
    return {'path': os.path.join(user_dir, dir_name), 'exists': False, 'reused': reused,
            'cloud_path': _cloud_path_for(user, dir_name),
            'haienv_version': _haienv_version()}


async def derive_env_path(user, venv_name, py=None, extend=None) -> dict:
    return await _to_thread(derive_env_path_sync, user, venv_name, py, extend)


# --------------------------------------------------------------------------- API-13 注册

@contextmanager
def _registry_lock(db_path: str):
    '''flock 串行化服务端与客户端的并发写（R-4 / NFR-03）。'''
    lock_path = f'{db_path}.lock'
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _write_registry_sync(db_path: str, env_name: str, path: str, py: str,
                         extra_search_dir, extra_search_bin_dir, extra_environment):
    '''
    用镜像内 haienv 包写 haienv 表（ADR-E4：必须复用 HaienvConfig，保证客户端可反序列化）。
    写入后回读校验（设计 §5.2 第 3 点）。
    '''
    from haienv.client.model import Haienv, HaienvConfig
    haienv_config = HaienvConfig(
        path=path,
        extend='False',
        extend_env='',
        py=py,
        extra_search_dir=list(extra_search_dir or []),
        extra_search_bin_dir=list(extra_search_bin_dir or []),
        extra_environment=list(extra_environment or []),
    )
    Haienv.insert(haienv_name=env_name, haienv_config=haienv_config, outside_db_path=db_path)
    got = Haienv.select(outside_db_path=db_path, haienv_name=env_name)
    if got is None:
        raise RuntimeError(f'回读校验失败：{db_path} 中未找到 {env_name}')
    if os.path.normpath(str(getattr(got, 'path', ''))) != os.path.normpath(path):
        raise RuntimeError(f'回读校验失败：{env_name} 路径不一致 {getattr(got, "path", None)} != {path}')


def _failure_reason(exc: Exception) -> str:
    text = f'{type(exc).__name__}: {exc}'.lower()
    if 'permission' in text or 'readonly' in text or 'not permitted' in text:
        return 'permission'
    if 'locked' in text or 'lock' in text:
        return 'locked'
    if 'import' in text or 'module' in text:
        return 'import'
    if 'assert' in text or '回读' in text:
        return 'assert'
    return 'unknown'


def register_env_sync(user, venv_name, path, py, extra_search_dir=None,
                      extra_search_bin_dir=None, extra_environment=None) -> dict:
    '''
    API-13 领域实现（同步）：写 {user_env_dir}/venv.db 的 haienv 表并回读校验。

    - path 必须落在 get_user_env_dir(user) 之下（SEC-02 / SEC-04）
    - 幂等：REPLACE INTO haienv，同名同路径重复注册无副作用（NFR-03）
    - 失败：PATH_ESCAPE / INVALID_PARAM / ENV_REGISTRY_NOT_WRITABLE / ENV_REGISTRY_WRITE_FAILED
    '''
    from cloud_storage.metrics import env_registry_write_failures_total

    check_env_push_enabled(user)
    env_name = validate_env_name(venv_name)

    if not py or not str(py).strip():
        raise WorkspaceError(ErrorCode.INVALID_PARAM, 'py 不能为空')
    py = str(py).strip()

    if not path or not str(path).strip():
        raise WorkspaceError(ErrorCode.INVALID_PARAM, 'path 不能为空')
    path = os.path.normpath(str(path).strip())

    user_dir = get_user_env_dir(user)
    # 惰性 import：cloud_storage.utils 会拉起 fastapi，不能出现在领域层模块导入期（HC-06）
    from cloud_storage.utils import check_is_subpath
    from cloud_storage.utils import ClientException
    if path == os.path.normpath(user_dir):
        raise WorkspaceError(ErrorCode.PATH_ESCAPE, f'目的路径 {path} 必须是用户 env 目录下的子目录，非法！')
    try:
        check_is_subpath(user_dir, path)
    except ClientException as e:
        raise WorkspaceError(ErrorCode.PATH_ESCAPE, str(e))
    except WorkspaceError:
        raise
    except Exception as e:
        raise WorkspaceError(ErrorCode.PATH_ESCAPE, f'目的路径 {path} 超出限定范围，非法！({e})')

    if not _wait_for_dir(path):
        wait = int(_isdir_wait_seconds())
        raise WorkspaceError(ErrorCode.INVALID_PARAM,
                             f'目标目录 {path} 不存在（等待 {wait}s 后仍不可见），'
                             f'请先完成 env 上传后再注册；若确认上传已完成，直接重试 '
                             f'`env push` 即可（会复用同一目录与同一批对象 key，不会多占后缀）')

    _probe_writable(user_dir)

    # N3：注册前先读一次注册表 —— 若同名条目存在但读不出来，本次写入是**修复**行为，
    # 需要留下显著日志与指标（否则「用一个读不出来的值覆盖另一个读不出来的值」是静默的）。
    registry, broken = _read_registry(user)
    if env_name in broken:
        logger.warning(f'[ENV] 注册表中原条目无法读取，本次注册将覆盖为新值 user={_user_name(user)} '
                       f'env={env_name} db={get_env_registry_path(user)}')

    db_path = get_env_registry_path(user)
    started = time.time()
    try:
        with _registry_lock(db_path):
            _write_registry_sync(db_path, env_name, path, py,
                                 extra_search_dir, extra_search_bin_dir, extra_environment)
    except WorkspaceError:
        raise
    except Exception as e:
        reason = _failure_reason(e)
        env_registry_write_failures_total.labels(reason=reason).inc()
        logger.error(f'[ENV] 注册表写入失败 user={_user_name(user)} env={env_name} path={path} '
                     f'db={db_path} reason={reason} error={e}')
        raise WorkspaceError(
            ErrorCode.ENV_REGISTRY_WRITE_FAILED,
            f'写入集群侧注册表失败（{reason}）: {path}，上传的文件已保留，可直接重试 `env push` 补登记')
    elapsed_ms = int((time.time() - started) * 1000)
    logger.info(f'[ENV] 注册成功 user={_user_name(user)} env={env_name} path={path} db={db_path} '
                f'elapsed_ms={elapsed_ms} haienv_version={_haienv_version()}')
    return {'registered': True, 'path': path, 'db': db_path,
            'haienv_version': _haienv_version()}


async def register_env(user, venv_name, path, py, extra_search_dir=None,
                       extra_search_bin_dir=None, extra_environment=None) -> dict:
    return await _to_thread(register_env_sync, user, venv_name, path, py,
                            extra_search_dir, extra_search_bin_dir, extra_environment)


# --------------------------------------------------------------------------- OPS-01 启动自检

def env_registry_self_check() -> dict:
    '''
    校验 get_env_root() 与 get_base_path(..., FileType.ENV) 的 cluster 侧前缀是否同源。
    不一致时打印 ERROR + 建议值，**不抛异常、不阻断启动**（OPS-01）。
    '''
    env_root = get_env_root()
    probe_user = '__env_self_check__'
    expected_user_dir = os.path.normpath(get_user_env_dir(probe_user))
    cluster_dir = None
    error = None
    try:
        from cloud_storage.utils import get_base_path
        from conf.utils import FileType
        cluster_base_path, _ = get_base_path(probe_user, probe_user, 'probe', FileType.ENV)
        cluster_dir = os.path.normpath(os.path.dirname(cluster_base_path))
    except Exception as e:  # 配置缺失等：只告警
        error = str(e)

    ok = (cluster_dir is not None) and (cluster_dir == expected_user_dir)
    result = {
        'ok': ok,
        'env_root': env_root,
        'expected_user_env_dir': expected_user_dir,
        'cluster_base_dir': cluster_dir,
        'suggested_env_path': os.path.dirname(env_root),
        # ADR-E4/CMP-04：注册表值是 pickle 的 HaienvConfig，版本耦合必须可见 ——
        # 出问题时第一步就是对齐「镜像里的 haienv 版本」与「客户端 haienv 版本」。
        'haienv_version': _haienv_version(),
    }
    if error:
        result['error'] = error
    if ok:
        logger.info(f'env path check: OK env_root={env_root} HAIENV_PATH={expected_user_dir} '
                    f'haienv_version={result["haienv_version"]}')
    else:
        logger.error(
            'env path check: FAILED —— 数据面落盘目录与运行时 HAIENV_PATH 不同源，'
            f'env_root={env_root}（建议 [cloud.storage.service] env_path = {os.path.dirname(env_root)}），'
            f'get_base_path 得到 {cluster_dir}，期望 {expected_user_dir}'
            + (f'，错误: {error}' if error else ''))
    return result
