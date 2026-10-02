import hashlib
import fnmatch
import mmap
import os
import re
import shutil
import stat
import zipfile

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel


# 文件分片大小
slice_bytes = 104857600  # 100 * 1024 * 1024

# 北京时间
tz_utc_8 = timezone(timedelta(hours=8))

class FileType(str, Enum):
    # 私有、公开数据集
    DATASET = 'dataset'
    # 工作区
    WORKSPACE = 'workspace'
    # venv
    ENV = 'env'
    # 用户自定义镜像（hai-cli images）
    IMAGE = 'image'
    # hfai 文档
    DOC = 'doc'
    # hfai pip 源
    PYPI = 'pypi'
    # hfai 官网
    WEBSITE = 'website'


class FilePrivacy(str, Enum):
    PUBLIC = "public"
    GROUP_SHARED = "group_shared"
    PRIVATE = "private"


class DatasetType(str, Enum):
    FULL = 'full'
    MINI = 'mini'


class SyncStatus(str, Enum):
    '''
    数据库中push/pull的状态记录
    stage1, stage2 分别标识同步过程的两个阶段: 本地 - 云端 - 集群
    状态转换为:  stage1_running -> stage1_finished -> stage2_running -> finished
                                |                                    |
                                -> stage1_failed                     -> stage2_failed
    '''
    INIT = 'init'
    STAGE1_RUNNING = 'stage1_running'
    STAGE2_RUNNING = 'stage2_running'
    RUNNING = 'running'
    STAGE1_FINISHED = 'stage1_finished'
    FINISHED = 'finished'
    STAGE1_FAILED = 'stage1_failed'
    STAGE2_FAILED = 'stage2_failed'
    FAILED = 'failed'


class SyncDirection(str, Enum):
    PUSH = 'push'
    PULL = 'pull'


class FileInfo(BaseModel):
    path: str
    size: Optional[int] = None
    last_modified: Optional[str] = None
    md5: Optional[str] = None
    ignored: Optional[bool] = None


class FileInfoList(BaseModel):
    files: List[FileInfo]


class FileList(BaseModel):
    files: List[str]


@contextmanager
def directio_iter(filename, readsize, offset):
    if offset % mmap.PAGESIZE:
        raise ValueError(f"offset {offset} doesn't align with os PAGESIZE")
    fd = os.open(filename, os.O_RDONLY | os.O_DIRECT)
    try:
        with mmap.mmap(fd, readsize, access=mmap.ACCESS_READ, offset=offset) as m:
            yield m
    finally:
        os.close(fd)


def calculate_md5(file_name, size, directio=False):
    """
    计算文件的md5 hash
    @param file_name: 文件路径
    """
    md5 = hashlib.md5()
    chunk_size = 1 << 30  # 1G
    if directio:
        with open(file_name, mode='rb') as fobj:
            size = min(fobj.seek(0, os.SEEK_END), size)
        offset = 0
        while offset < size:
            read_size = min(size - offset, chunk_size)
            with directio_iter(file_name, read_size, offset) as m:
                if not m:
                    break
                offset += len(m)
                md5.update(m)
    else:
        with open(file_name, mode='rb') as fobj:
            while True:
                data = fobj.read(chunk_size)
                if not data:
                    break
                md5.update(data)
    return md5.hexdigest()


# 默认忽略文件
default_ignored_patterns = [
    '.vscode',
    '.idea',  # ide generated config
    '.git',
    '.gitignore',
    '.gitattributes',  # git
    '__pycache__',
]


def get_ignored_pattern(hfignore_path):
    """
    默认从workspace根目录读取.hfignore文件, 如没有, 则使用default_ignored_patterns
    规则为：
      *       匹配所有字符
      ?       匹配任意单个字符
      [seq]   匹配seq中的任意单个字符
      [!seq]  匹配任意不在seq中的单个字符
      不支持转义，即 \[ \? 等不会被解析
      末尾带 / 匹配目录下的所有内容，不包括目录本身；末尾不带 / 则匹配同名文件、同名目录和目录下的所有内容
      pattern按行优先, 在冲突情况下, 以前面的pattern为准

    示例:
      test?.py      匹配 testn.py
      test*.py      匹配 testabc.py
      test[0-5].py  匹配 test1.py, 不匹配 test6.py
      test[!0-5].py 匹配 test6.py, 不匹配 test1.py
      test          匹配 任意目录下 test 文件或 任意名为 test 的子目录及 test/ 目录下所有文件
      test/         匹配 任意名为 test 的子目录下所有文件
    """
    if os.path.exists(hfignore_path):
        with open(hfignore_path, 'r') as f:
            lines = list(f)
    else:
        lines = default_ignored_patterns
    patterns = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith('#') and not line.startswith(
                './') and not '\\' in line:
            line = line.lstrip('/')
            if line.endswith('/'):
                patterns.append(line + '*')
                patterns.append('*/' + line + '*')
            else:
                patterns.append(line)
                patterns.append(line + '/*')
                patterns.append('*/' + line)
                patterns.append('*/' + line + '/*')
    return patterns


def is_file_ignored(abspath, base_path, patterns, no_hfignore=False):
    if no_hfignore:
        return False
    subpath = abspath[len(base_path):]
    subpath = subpath.lstrip(os.path.sep)
    return any(fnmatch.fnmatch(subpath, p) for p in patterns)


def get_file_info(file_path, base_path, no_checksum, directio=False):
    key = file_path[len(base_path):].replace('//', '/').replace('\\', '/').lstrip('/')
    size = os.path.getsize(file_path)
    last_modified = datetime.fromtimestamp(
        os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
    if no_checksum:
        return FileInfo(path=key, size=size, last_modified=last_modified)
    else:
        md5 = calculate_md5(file_path, size, directio)
        return FileInfo(path=key,
                        size=size,
                        last_modified=last_modified,
                        md5=md5)


def list_local_files_inner(base_path, subpath, no_checksum=False, no_hfignore=False, recursive=True, directio=False):
    """
    获取本地文件详情列表
    @param base_path: 工作区目录
    @param subpath: 子目录
    @param no_checksum: 是否禁用checksum
    @param no_hfignore: 会否忽略hfignore
    @param recursive: 是否递归list子目录
    @return: FileInfo列表
    """
    ret = []
    base_path, subpath = os.path.normpath(base_path), os.path.normpath(subpath)
    root_path = os.path.abspath(f'{base_path}/{subpath}')
    if not root_path.startswith(base_path):
        return ret
    subpath = root_path[len(base_path):]
    patterns = get_ignored_pattern(f'{base_path}/.hfignore')

    if not os.path.exists(root_path):
        return ret

    if recursive:
        if is_file_ignored(root_path, base_path, patterns, no_hfignore):
            return ret
        if os.path.isfile(root_path):
            info = get_file_info(root_path, base_path, no_checksum, directio)
            ret.append(info)
            return ret

        for filepath, _, files in os.walk(root_path):
            if is_file_ignored(filepath, base_path, patterns, no_hfignore):
                continue
            for filename in files:
                fullpath = os.path.join(filepath, filename)
                if os.path.dirname(fullpath).split('/')[-1] == '.hfai' and '.zip' in os.path.basename(fullpath):
                    continue
                if is_file_ignored(fullpath, base_path, patterns, no_hfignore):
                    continue
                try:
                    info = get_file_info(fullpath, base_path, no_checksum, directio)
                except FileNotFoundError as e:
                    # print(f'本地文件 {fullpath} 被删除或链接不存在，忽略: {str(e)}')
                    continue
                ret.append(info)
    else:
        # 该分支仅给前端展示用
        if os.path.isfile(root_path):
            info = get_file_info(root_path, base_path, no_checksum, directio)
            if is_file_ignored(root_path, base_path, patterns, no_hfignore):
                info.ignored = True
            ret.append(info)
            return ret
        for f in os.listdir(root_path):
            key = f'{subpath}/{f}'.replace('//', '/').replace('\\', '/').lstrip('/').rstrip('/')
            file_path = f'{base_path}/{key}'
            try:
                if os.path.isdir(file_path):
                    key += '/'
                size = getPathSize(file_path)
                last_modified = datetime.fromtimestamp(
                    os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
                info = FileInfo(path=key, size=size, last_modified=last_modified)
                if is_file_ignored(file_path, base_path, patterns, no_hfignore):
                    info.ignored = True
                ret.append(info)
            except FileNotFoundError as e:
                # print(f'{file_path} 可能正在被删除: {str(e)}')
                continue

    return ret


def getPathSize(filePath):
    if os.path.isdir(filePath):
        size=0
        for root, _, files in os.walk(filePath):
            for f in files:
                try:
                    size += os.path.getsize(os.path.join(root, f))
                except FileNotFoundError:
                    continue
        return size
    else:
        return os.path.getsize(filePath)


def hashkey(*args):
    return hashlib.sha256(''.join(args).encode('utf-8')).hexdigest()


class MyZipFile(zipfile.ZipFile):
    '''
    extend builtin zipfile class with file mode persistence
    '''
    def _extract_member(self, member, targetpath, pwd):
        """Extract the ZipInfo object 'member' to a physical
           file on the path targetpath.
        """
        if not isinstance(member, zipfile.ZipInfo):
            member = self.getinfo(member)

        # build the destination pathname, replacing
        # forward slashes to platform specific separators.
        arcname = member.filename.replace('/', os.path.sep)

        if os.path.altsep:
            arcname = arcname.replace(os.path.altsep, os.path.sep)
        # interpret absolute pathname as relative, remove drive letter or
        # UNC path, redundant separators, "." and ".." components.
        arcname = os.path.splitdrive(arcname)[1]
        invalid_path_parts = ('', os.path.curdir, os.path.pardir)
        arcname = os.path.sep.join(x for x in arcname.split(os.path.sep)
                                   if x not in invalid_path_parts)
        if os.path.sep == '\\':
            # filter illegal characters on Windows
            arcname = self._sanitize_windows_name(arcname, os.path.sep)

        targetpath = os.path.join(targetpath, arcname)
        targetpath = os.path.normpath(targetpath)

        # Create all upper directories if necessary.
        upperdirs = os.path.dirname(targetpath)
        if upperdirs and not os.path.exists(upperdirs):
            os.makedirs(upperdirs)
            os.chmod(upperdirs, stat.S_IMODE(member.external_attr>>16))

        if member.is_dir():
            if not os.path.isdir(targetpath):
                os.mkdir(targetpath)
                os.chmod(targetpath, stat.S_IMODE(member.external_attr>>16))
            return targetpath

        with self.open(member, pwd=pwd) as source, \
             open(targetpath, "wb") as target:
            shutil.copyfileobj(source, target)
        os.chmod(targetpath, stat.S_IMODE(member.external_attr>>16))

        return targetpath


def zip_dir(base_path, target_files, zip_file_path, exclude_list):
    '''
    压缩目录到zip包
    '''
    os.makedirs(os.path.dirname(zip_file_path), exist_ok=True)
    f = MyZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)
    if target_files is None:
        # 没传入子目录，则打包整个base_path
        for path, filepaths, filenames in os.walk(base_path):
            fpath = path[len(base_path):]
            if fpath in exclude_list:
                continue
            f.write(path, fpath)

            for filepath in filepaths:
                p = os.path.join(path, filepath)
                if p[len(base_path):] in exclude_list:
                    continue
                f.write(p, p[len(base_path):])

            for filename in filenames:
                if os.path.join(path, filename) == zip_file_path or filename in exclude_list:
                    continue
                f.write(os.path.join(path, filename), os.path.join(fpath, filename))
    else:
        dirnames = []
        for target_file in target_files:
            if target_file.path.split('/')[-1] in exclude_list:
                continue
            src_path = os.path.join(base_path, target_file.path)
            parent_path = base_path
            for target_file_subpath in target_file.path.split(os.path.sep)[:-1]:
                parent_path += f'{os.path.sep}{target_file_subpath}'
                if parent_path not in dirnames:
                    f.write(parent_path, parent_path[len(base_path):])
                    dirnames.append(parent_path)
            if src_path not in dirnames:
                f.write(src_path, target_file.path)
                dirnames.append(src_path)
    f.close()


def unzip_dir(zip_file_path, dst_dir):
    """
    解压缩zip包到指定路径
    """
    os.makedirs(dst_dir, exist_ok=True)
    f = MyZipFile(zip_file_path)
    f.extractall(dst_dir)
    f.close()
    return f.namelist()


def bytes_to_human(n):
    if n is None or n == '-':
        return '-'
    n = float(n) / 1024
    symbol = ['K', 'M', 'G', 'T', 'P', 'E']
    idx = 0
    while n >= 1024:
        n /= 1024
        idx += 1
    return '%.2f%sB' % (n, symbol[idx])


# ---------------------------------------------------------------------------
# haienv（`hai-cli env`）路径单点定义 —— 设计 docs/haiplatform/env/env-server-design.md §3.1
#
# 约定：
#   env_root     = {env_path}/hfai_envs                # 集群侧所有用户 env 的父目录
#   user_env_dir = {env_root}/{user} = dirname(HAIENV_PATH)
#   注册表        = {user_env_dir}/venv.db 的 haienv 表
#   env prefix   = {user_env_dir}/{name}_{suffix}
#
# 约束（ADR-E3）：
#   - 本模块被 `conf/__init__.py` star-import，**不得**在导入期 import conf（循环导入）
#   - 只依赖 os/re + 惰性 CONF；任何外部异常都退化为默认值，不得让 import 失败
#   - 服务端（cloud_storage / server_model）与客户端（hfai client 的 conf/utils.py 副本）
#     共用这一份实现，禁止在别处硬编码 'hfai_envs'
# ---------------------------------------------------------------------------

ENV_DIR_NAME = 'hfai_envs'
DEFAULT_ENV_PATH = '/hf_shared'
# 名称白名单：首字符字母数字，其余允许字母数字与 . _ -，总长 1~64
ENV_NAME_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$')


def normalize_env_path(path) -> str:
    '''
    归一化 env 家族父根：去尾斜杠 / 去重复分隔符 / 转绝对路径。
    空值取默认 /hf_shared（设计 §3.1）。
    '''
    if path is None or (isinstance(path, str) and path.strip() == ''):
        path = DEFAULT_ENV_PATH
    try:
        path = os.path.expanduser(str(path).strip())
        return os.path.normpath(os.path.abspath(path))
    except Exception:
        return DEFAULT_ENV_PATH


def get_env_path() -> str:
    '''配置项 [cloud.storage.service] env_path（env 家族父根），默认 /hf_shared。'''
    try:
        from conf import CONF
        value = CONF.try_get('cloud.storage.service.env_path', default=DEFAULT_ENV_PATH)
    except Exception:
        value = DEFAULT_ENV_PATH
    return normalize_env_path(value)


def get_env_root() -> str:
    '''env_root = {env_path}/hfai_envs，等于任务运行时 dirname(HAIENV_PATH)。'''
    return os.path.join(get_env_path(), ENV_DIR_NAME)


def get_user_env_dir(user) -> str:
    '''某个用户的 env 目录 = dirname(HAIENV_PATH)。'''
    return os.path.join(get_env_root(), str(user))


def get_env_registry_path(user) -> str:
    '''某个用户的注册表（venv.db）绝对路径。'''
    return os.path.join(get_user_env_dir(user), 'venv.db')


def get_env_dir_name(name, suffix=0) -> str:
    '''env 目录名：{name}_{suffix}，与客户端 haienv.client.model.get_haienv_path 一致。'''
    return f'{name}_{int(suffix)}'


# ---------------------------------------------------------------------------
# hai-cli images（用户自定义镜像）路径与命名单点定义 —— 设计 docs/haiplatform/images/images-server-design.md §3
#
# 约定（三个概念必须分清，设计 §3.1）：
#   image_tar  = 用户提供的 tar 包在共享盘上的路径（API-15 入参 / train_image.image_tar）
#   image      = 镜像名 name[:tag]，自身**不含 '/'**（与 registry/shared_group 拼成 3 段 URL）
#   path       = 镜像资产在共享盘上的位置（运行期 HFAI_IMAGE_WEKA_PATH → link 脚本）
#
# 约束：
#   - 本模块被 `conf/__init__.py` star-import，**不得**在导入期 import conf（循环导入）
#   - 只依赖 os/re + 惰性 CONF；任何外部异常都退化为默认值，不得让 import 失败
#   - 服务端与客户端（hfai client 的 conf/utils.py 副本）共用这一份实现
# ---------------------------------------------------------------------------

DEFAULT_IMAGE_PATH = '/nfs_shared/image'
# 镜像名白名单：name[:tag]；首字符字母数字，其余允许字母数字与 . _ -；**不允许 '/'**（HC-05 / SEC-03）
IMAGE_NAME_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,63}(?::[A-Za-z0-9][A-Za-z0-9._-]{0,63})?$')


def normalize_image_path(path) -> str:
    '''
    归一化镜像资产共享根：去尾斜杠 / 去重复分隔符 / 转绝对路径。
    空值取默认 /nfs_shared/image（设计 §3.2）。
    '''
    if path is None or (isinstance(path, str) and path.strip() == ''):
        path = DEFAULT_IMAGE_PATH
    try:
        path = os.path.expanduser(str(path).strip())
        return os.path.normpath(os.path.abspath(path))
    except Exception:
        return DEFAULT_IMAGE_PATH


def get_image_path() -> str:
    '''配置项 [cloud.storage.service] image_path（镜像资产共享根），默认 /nfs_shared/image。'''
    try:
        from conf import CONF
        value = CONF.try_get('cloud.storage.service.image_path', default=DEFAULT_IMAGE_PATH)
    except Exception:
        value = DEFAULT_IMAGE_PATH
    return normalize_image_path(value)


def get_image_root() -> str:
    '''
    image_root = 镜像资产共享根（单点定义，对齐 env 的 get_env_root()）。

    所有 image_tar 必须落在它之下（SEC-01 / FR-13），并且是运行期 initContainer 里
    可见的路径（HFAI_IMAGE_WEKA_PATH 的前缀）。
    '''
    return get_image_path()


def derive_image_name(image_tar) -> str:
    '''
    由 tar 包路径派生镜像名：basename 去掉 .tar 后缀（I6：旧客户端只发 tar 路径）。

    **不自动补 tag**（设计 §4.1「实现修正 I6b」）：任务侧做的是**逐字节**比较，
    补 :latest 会让用户 `-i registry/<group>/demo` 永远匹配不上。
    '''
    base = os.path.basename(str(image_tar).rstrip('/'))
    if base.endswith('.tar'):
        base = base[:-len('.tar')]
    return base


def is_valid_image_name(name) -> bool:
    '''镜像名白名单校验（不含 '/'、非空、长度受限）。返回 bool，不抛异常（分层纪律）。'''
    if not name or not isinstance(name, str):
        return False
    if '/' in name:
        return False
    return bool(IMAGE_NAME_RE.match(name))
