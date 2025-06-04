
from fastapi import Depends

from api.depends import get_api_user, request_limitation
from api.app import app
from server_model.user import User
from server_model.selector import AioUserSelector
from k8s.async_v1_api import get_storage_quota
import os
from base_model.venv import set_path_prefix, Venv, get_venv_path, get_db_path
import munch
import aiofiles
from conf.venv_script import ACTIVATE
import traceback


async def get_user_storage_list(user: User = Depends(get_api_user)):
    """获取用户的可用存储路径"""
    storages = user.storage.personal_storage()
    storage_quota = await get_storage_quota()

    for s in storages:
        s['quota'] = storage_quota.get(s['host_path'], {})
        # 外部用户，移除掉host_path字段
        if not user.is_internal:
            del s['host_path']
        del s['name']

    return {
        'success': 1,
        'storages': storages
    }


async def get_user_group(user: User = Depends(get_api_user)):
    """
    获取用户的 group信息
    """
    return {
            'success': 1,
            'data': {
                'user_name': user.user_name,
                'user_shared_group': user.shared_group,
                'user_group': user.group_list
            }
        }


async def get_user_storage_usage(user: User = Depends(get_api_user)):
    """
    返回用户在 weka 上的 usage 和 limit
    写死
    internal 返回:
    permanent:
      limit_bytes: xxx
      used_bytes: xxx
      sub_paths:
        jupyter:
          used_bytes: xxx
        containers:
          used_bytes: xxx
        private:
          used_bytes: xxx
    gc_long_term:
      limit_bytes: xxx
      used_bytes: xxx
    gc_short_term:
      limit_bytes: xxx
      used_bytes: xxx

    external 返回:
    group:
      limit_bytes: xxx
      used_bytes: xxx
      shared_used_bytes: xxx
      user_used_bytes:
        user1: xxx
        ...
    """
    storage_quota = await get_storage_quota()
    if user.is_internal:
        permanent_path = f'/weka-jd/prod/permanent/{user.user_name}'
        gc_long_term_path = f'/weka-jd/prod/gc_long_term/{user.user_name}'
        gc_short_term_path = f'/weka-jd/prod/gc_short_term/{user.user_name}'
        return {
            'success': 1,
            'result': {
                'permanent': {
                    'limit_bytes': storage_quota.get(permanent_path, {}).get('limit_bytes', 0),
                    'used_bytes': storage_quota.get(permanent_path, {}).get('used_bytes', 0),
                    'sub_paths': {
                        'jupyter': {
                            'used_bytes': storage_quota.get(f'{permanent_path}/jupyter', {}).get('used_bytes', 0),
                        },
                        'containers': {
                            'used_bytes': storage_quota.get(f'{permanent_path}/containers', {}).get('used_bytes', 0),
                        },
                        'private': {
                            'used_bytes': storage_quota.get(f'{permanent_path}/private', {}).get('used_bytes', 0),
                        }
                    }
                },
                'gc_long_term': {
                    'limit_bytes': storage_quota.get(gc_long_term_path, {}).get('limit_bytes', 0),
                    'used_bytes': storage_quota.get(gc_long_term_path, {}).get('used_bytes', 0),
                },
                'gc_short_term': {
                    'limit_bytes': storage_quota.get(gc_short_term_path, {}).get('limit_bytes', 0),
                    'used_bytes': storage_quota.get(gc_short_term_path, {}).get('used_bytes', 0),
                },
                'weka_prod': {
                    'limit_bytes': storage_quota.get('/weka-jd/prod', {}).get('limit_bytes', 0),
                    'used_bytes': storage_quota.get('/weka-jd/prod', {}).get('used_bytes', 0),
                }
            }
        }
    else:
        group_path = f'/weka-jd/prod/public/permanent/{user.shared_group}'
        group_users = await AioUserSelector.where('"shared_group"=%s and "role"=%s', (user.shared_group, user.role))
        return {
            'success': 1,
            'result': {
                'group': {
                    'limit_bytes': storage_quota.get(group_path, {}).get('limit_bytes', 0),
                    'used_bytes': storage_quota.get(group_path, {}).get('used_bytes', 0),
                    'shared_used_bytes': storage_quota.get(f'{group_path}/shared', {}).get('used_bytes', 0),
                    'user_used_bytes': {
                        u.user_name: storage_quota.get(f'{group_path}/{u.user_name}', {}).get('used_bytes', 0)
                        for u in group_users
                    }
                }
            }
        }


async def update_cluster_venv(venv_name: str, py: str, user: User = Depends(get_api_user)):
    """
    更新集群的venv信息
    """
    if user.is_internal:
        return {
            'success': 0,
            'msg': '目前只对外部用户开放update_cluster_venv api',
            'path': ''
        }
    try:
        dir_path = f'/weka-jd/prod/public/permanent/{user.shared_group}/shared/hfai_envs/{user.user_name}'
        mount_path = lambda x: x.replace(f'/weka-jd/prod/public/permanent/{user.shared_group}/shared', '/hf_shared')
        if not os.path.exists(dir_path):  # 落到这里肯定是创建用户的脚本出问题了
            os.makedirs(dir_path, exist_ok=True)
            os.chown(dir_path, user.uid, user.uid)
        set_path_prefix(dir_path)
        item = await Venv.select(where='venv_name = ?', args=(venv_name, ), find_one=True)
        if item:  # 集群环境本身就有这个venv，啥都不用做
            return {
                'success': 1,
                'msg': '集群本身已有该venv，返回path',
                'path': f'{user.shared_group}/shared/hfai_envs/{user.user_name}/{os.path.basename(item[1])}'
            }
        # 接下来要新建venv
        rst = await get_venv_path(venv_name=venv_name)
        if rst['success'] == 0:
            raise Exception(f"发生错误: {rst['msg']}，请联系管理员")
        path = rst['msg']
        pip_conf_path = os.path.join(path, 'pip.conf')
        activate_path = os.path.join(path, 'activate')
        os.makedirs(path, exist_ok=True)

        # 插入数据库
        await Venv.insert(venv_name=venv_name, path=mount_path(path), extend='False', extend_env='', py=py)

        # 生成 PIP_CONF
        pip_configs = munch.Munch.fromYAML(open('marsv2/scripts/pip_conf.yaml'))
        async with aiofiles.open(pip_conf_path, "w") as fp:
            await fp.write('[global]\n')
            for k, v in pip_configs.items():
                await fp.write(f'{k} = {v}\n')

        # 生成 ACTIVATE
        activate_content = ACTIVATE.replace('__PIP_PATH__', mount_path(pip_conf_path)).replace('__HF_ENV_NAME__', venv_name)\
            .replace('__PATH__', mount_path(path)).replace('__HF_ENV_OWNER__', user.user_name)\
            .replace('__NAME__', f"[{venv_name}]").replace('__EXTEND_HF_ENV__', '')
        async with aiofiles.open(activate_path, "w") as fp:
            await fp.write(activate_content)
        for p in [path, pip_conf_path, activate_path, get_db_path()]:
            os.chown(p, user.uid, user.uid)
    except Exception as e:  # 创建的时候发生异常，整个操作不是原子的了，但在这里不好确定是哪个烂掉的，需要人工介入
        traceback.print_exc()
        return {
            'success': 0,
            'msg': f'发生错误 {str(e)} 请联系管理员',
            'path': ''
        }

    return {
        'success': 1,
        'msg': '集群本身没有该venv，新建了venv，返回path',
        'path': f'{user.shared_group}/shared/hfai_envs/{user.user_name}/{os.path.basename(path)}'
    }
