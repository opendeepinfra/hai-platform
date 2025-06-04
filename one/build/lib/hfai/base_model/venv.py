import os
import aiosqlite


__db_path = None
__path_prefix = None


def get_path_prefix():
    global __path_prefix
    if __path_prefix is None:  # 从 hfai config 中获取
        from hfai.client.api.api_config import get_mars_venv_path as mars_venv_path
        __path_prefix = mars_venv_path()
    return __path_prefix


def get_db_path():
    global __db_path
    if __db_path is None:
        path_prefix = get_path_prefix()
        try:
            if not os.path.exists(path_prefix):
                os.makedirs(path_prefix)
            __db_path = os.path.join(path_prefix, 'venv.db')
        except:
            pass
    return __db_path


def set_path_prefix(path_prefix):
    global __path_prefix
    __path_prefix = path_prefix


def create_if_not_exist(func):
    async def run(*argv, **kwargs):
        sql = f'''
        SELECT *  FROM `sqlite_master`
        WHERE `type` = 'table' AND `name` = 'venv'
        '''
        async with aiosqlite.connect(get_db_path()) as db:
            result = await db.execute(sql)
            if not await result.fetchone():
                await Venv.create()
        ret = await func(*argv, **kwargs)
        return ret
    return run


class Venv:
    @classmethod
    async def create(cls):
        sql = '''
        CREATE TABLE IF NOT EXISTS `venv` (
          `venv_name` varchar(255) NOT NULL DEFAULT '',
          `path` varchar(255) NOT NULL DEFAULT '',
          `extend` varchar(255) NOT NULL DEFAULT '',
          `extend_env` varchar(255) NOT NULL DEFAULT '',
          `py` varchar(255) NOT NULL DEFAULT '',
          PRIMARY KEY (`venv_name`)
        )
        '''

        async with aiosqlite.connect(get_db_path()) as db:
            await db.execute(sql)

    @classmethod
    @create_if_not_exist
    async def insert(cls, venv_name, path, extend, extend_env, py):
        sql = '''
        INSERT INTO `venv` (`venv_name`, `path`, `extend`, `extend_env`, `py`)
        VALUES (?, ?, ?, ?, ?)
        '''
        async with aiosqlite.connect(get_db_path()) as db:
            await db.execute(sql, (venv_name, path, extend, extend_env, py))
            await db.commit()

    @classmethod
    @create_if_not_exist
    async def select(cls, where, args=(), outside_db_path=None, find_one=False):
        sql = f'''
        SELECT * FROM `venv`
        WHERE {where}
        '''
        async with aiosqlite.connect(outside_db_path if outside_db_path else get_db_path()) as db:
            results = await db.execute(sql, args)
            return await results.fetchone() if find_one else await results.fetchall()

    @classmethod
    @create_if_not_exist
    async def delete(cls, where, args=()):
        sql = f'''
        DELETE FROM `venv`
        WHERE {where}
        '''
        async with aiosqlite.connect(get_db_path()) as db:
            await db.execute(sql, args)
            await db.commit()


async def get_venv_path(venv_name):
    if not os.path.exists(get_path_prefix()):
        try:
            os.makedirs(get_path_prefix())
        except:
            return {
                'success': 0,
                'msg': '权限有问题，请在hfai init 时设置 venv_path 或联系管理员'
            }
    if await Venv.select(where='venv_name = ?', args=(venv_name, ), find_one=True):
        return {
            'success': 0,
            'msg': f'虚拟环境{venv_name}已存在'
        }
    suffix_id = 0
    while True:
        path = os.path.join(get_path_prefix(), f"{venv_name}_{suffix_id}")
        if not os.path.exists(path):
            break
        suffix_id += 1
    return {
        'success': 1,
        'msg': path
    }
