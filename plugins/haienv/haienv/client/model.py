import os
import sqlite3
from .sqlite_dict import SqliteDict

__db_path = None
__path_prefix = None


def get_path_prefix():
    global __path_prefix
    if __path_prefix is None:
        __path_prefix = os.environ.get('HAIENV_PATH', os.environ['HOME'])
    return os.path.abspath(__path_prefix)


def relax_env_dir_permissions(path=None, quiet=False):
    '''
    设计 §4.4 路径 ①（`docs/haiplatform/env/env-server-design.md`）：把用户 env 目录放宽到
    0o777，让平台账号（ugc-server）能写 `{user_env_dir}/venv.db`。

    ⚠️ 安全护栏（issue #5）：**绝不对用户家目录（及其祖先、以及过浅的系统目录）做 chmod**。
    本函数在客户端侧调用，而客户端侧的 env 根目录在 hai/K8s 部署里默认就是 `$HOME`
    （`HAIENV_PATH` 未设置时，见 `get_path_prefix`）。把家目录改成 0777 会让 sshd 的
    StrictModes 拒绝公钥登录，把用户锁在机器外；而它对这里想解决的问题（**集群侧**目录
    不可写）毫无帮助 —— 那是另一台机器 / 另一份文件系统上的路径。

    :param path: 目标目录，缺省为 `get_path_prefix()`（本机 env 根目录）
    :param quiet: True 时不打印跳过/失败原因
    :return: True 表示确实放宽了权限；False 表示跳过（不存在 / 命中护栏 / chmod 失败）
    '''
    target = os.path.abspath(path or get_path_prefix())
    if not os.path.isdir(target):
        return False

    home = os.path.abspath(os.path.expanduser('~'))
    if target == home or home.startswith(target.rstrip(os.sep) + os.sep):
        if not quiet:
            print(f'[haienv] 跳过 chmod {target}：这是用户家目录或其祖先，放宽为 0777 会让 '
                  f'sshd StrictModes 拒绝公钥登录（见 issue #5）')
        return False
    if target.count(os.sep) < 2:  # '/'、'/home'、'/tmp' 这类过浅路径同样不动
        if not quiet:
            print(f'[haienv] 跳过 chmod {target}：路径过浅，疑似系统目录')
        return False

    try:
        os.chmod(target, 0o777)
        return True
    except OSError as e:
        if not quiet:
            print(f'[haienv] 放宽 {target} 权限失败（已忽略）：{e}')
        return False


def get_db_path(outside_db_path=None):
    if outside_db_path is not None:
        return outside_db_path
    global __db_path
    if __db_path is None:
        path_prefix = get_path_prefix()
        try:
            if not os.path.exists(path_prefix):
                os.makedirs(path_prefix)
            __db_path = os.path.join(path_prefix, 'venv.db')  # 为兼容旧版，仍然叫venv.db
        except:
            pass
    return __db_path


def set_path_prefix(path_prefix):
    global __path_prefix
    __path_prefix = path_prefix


def check_user_name(user):
    '''
    校验 -u/--user 参数（SEC-06 / 分析报告 E9）。

    该参数会被直接拼进 `{HAIENV_PATH}/../../{user}/venv.db`，因此必须拒绝
    '..' / '/' / '\\\\' / NUL 与首尾空白；合法时原样返回，非法抛 ValueError。
    '''
    if user is None:
        return None
    if not isinstance(user, str) or user == '' or user != user.strip():
        raise ValueError(f'用户名 {user!r} 非法')
    if user in ('.', '..') or any(ch in user for ch in ('/', '\\', '\x00')):
        raise ValueError(f'用户名 {user!r} 非法：不允许包含路径分隔符或父目录')
    return user


class HaienvConfig:
    def __init__(self, path='', extend='', extend_env='', py='', extra_search_dir=None, extra_search_bin_dir=None, extra_environment=None, **kwargs):
        self.path = path
        self.extend = extend
        self.extend_env = extend_env
        self.py = py
        self.extra_search_dir = [] if extra_search_dir is None else list(extra_search_dir)
        self.extra_search_bin_dir = [] if extra_search_bin_dir is None else list(extra_search_bin_dir)
        self.extra_environment = [] if extra_environment is None else list(extra_environment)

    def update(self, **kwargs):
        for attr in ['path', 'extend', 'extend_env', 'py', 'extra_search_dir', 'extra_search_bin_dir', 'extra_environment']:
            if attr in kwargs:
                setattr(self, attr, kwargs[attr])


def get_haienv_from_venv(outside_db_path=None, haienv_name=None):
    """
    获取旧版本下的venv_name对应的HaienvConfig
    :param outside_db_path:
    :param haienv_name: 如果是None，则返回所有venv转换得到的haienv结果，返回值是dict{haienv_name: HaienvConfig}；如果是字符串，则根据venv中是否找到该venv_name返回HaienvConfig或None
    :return: Optional(dict, HaienvConfig, None)
    """
    def get_haienv_config(item, db):
        haienv_config = HaienvConfig(path=item[1], extend=item[2], extend_env=item[3], py=item[4])
        try:
            venv_config = db.execute(f"SELECT * from `venv_config` WHERE venv_name='{item[0]}'").fetchall()[0] + ('', '', '')
            haienv_config.update(extra_search_dir=venv_config[1].split(':'), extra_search_bin_dir=venv_config[2].split(':'), extra_environment=venv_config[3].split(':'))
        except:  # 没有venv_config这个表或者没有venv_name这一项，就不填了
            pass
        return haienv_config

    with sqlite3.connect(get_db_path(outside_db_path)) as db:
        if haienv_name is None:
            haienv_dict = {}
            for item in db.execute("SELECT * from `venv`").fetchall():
                haienv_dict[item[0]] = get_haienv_config(item=item, db=db)
            return haienv_dict
        else:  # 只找venv_name的haienv_config
            rst = db.execute(f"SELECT * from `venv` WHERE venv_name='{haienv_name}'").fetchall()
            if len(rst) == 0:
                return None
            haienv_config = get_haienv_config(item=rst[0], db=db)
            return haienv_config


def update_venv_to_haienv(func):
    """
    将venv.db中的venv、venv_config table合并成haienv table
    """
    def run(*argv, **kwargs):
        outside_db_path = kwargs.get('outside_db_path', None)
        with sqlite3.connect(get_db_path(outside_db_path)) as db:
            result = [item[0] for item in db.execute("SELECT name from `sqlite_master` WHERE `type` = 'table'").fetchall()]
        old_version = False
        if 'venv' in result and 'haienv' not in result:  # 需要去更新haienv
            if os.access(get_db_path(outside_db_path), os.W_OK):
                db_dict = SqliteDict(get_db_path(outside_db_path), tablename='haienv')
                haienv_dict = get_haienv_from_venv(outside_db_path=outside_db_path)
                for k, v in haienv_dict.items():
                    db_dict[k] = v
            else:  # 需要更新haienv，但更新失败，很有可能是权限问题导致的，还是走原来的venv table
                old_version = True
        return func(*argv, old_version=old_version, **kwargs)
    return run


class Haienv:
    @classmethod
    @update_venv_to_haienv
    def insert(cls, haienv_name, haienv_config, outside_db_path=None, old_version=False):
        assert not old_version, f'不再支持更新旧版venv表，在创建haienv表时出现问题，请检查您{get_db_path(outside_db_path)}路径下venv.db的写权限'
        db_dict = SqliteDict(get_db_path(outside_db_path), tablename='haienv')
        db_dict[haienv_name] = haienv_config

    @classmethod
    @update_venv_to_haienv
    def select(cls, haienv_name=None, outside_db_path=None, old_version=False):
        """
        如果haienv_name是None，则返回所有venv转换得到的haienv结果，返回值是dict{haienv_name: HaienvConfig}；如果是字符串，则根据venv中是否找到该venv_name返回HaienvConfig或None
        """
        if old_version:
            return get_haienv_from_venv(outside_db_path=outside_db_path, haienv_name=haienv_name)
        db_dict = SqliteDict(get_db_path(outside_db_path), tablename='haienv')
        return {key: db_dict[key] for key in list(db_dict.keys())} if haienv_name is None else db_dict.get(haienv_name, None)

    @classmethod
    @update_venv_to_haienv
    def delete(cls, haienv_name, outside_db_path=None, old_version=False):
        assert not old_version, f'不再支持更新旧版venv表，在创建haienv表时出现问题，请检查您{get_db_path(outside_db_path)}路径下venv.db的写权限'
        db_dict = SqliteDict(get_db_path(outside_db_path), tablename='haienv')
        db_dict.pop(haienv_name, None)

    @classmethod
    @update_venv_to_haienv
    def update(cls, haienv_name, key, value, outside_db_path=None, old_version=False):
        assert not old_version, f'不再支持更新旧版venv表，在创建haienv表时出现问题，请检查您{get_db_path(outside_db_path)}路径下venv.db的写权限'
        db_dict = SqliteDict(get_db_path(outside_db_path), tablename='haienv')
        assert haienv_name in db_dict, f'未找到{haienv_name}'
        haienv_config = db_dict[haienv_name]
        setattr(haienv_config, key, value)
        db_dict[haienv_name] = haienv_config


def get_haienv_path(haienv_name):
    if not os.path.exists(get_path_prefix()):
        try:
            os.makedirs(get_path_prefix())
        except:
            return {
                'success': 0,
                'msg': '权限有问题，请检查haienv目录或联系管理员'
            }
    if Haienv.select(haienv_name=haienv_name) is not None:
        return {
            'success': 0,
            'msg': f'虚拟环境{haienv_name}已存在'
        }
    suffix_id = 0
    while True:
        path = os.path.join(get_path_prefix(), f"{haienv_name}_{suffix_id}")
        if not os.path.exists(path):
            break
        suffix_id += 1
    return {
        'success': 1,
        'msg': path
    }
