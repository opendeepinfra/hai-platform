import logging
import os

import sqlalchemy
from fastapi import Depends, HTTPException

from api.depends import get_api_user
from api.operation import create_task_base_queue
from conf.flags import TASK_TYPE, QUE_STATUS
from db import a_db_engine, sql_params
from server_model.selector import AioUserSelector, TrainImageSelector, \
    AioBaseTaskSelector
from server_model.user import User

image_loader_name = 'image_loader'


async def hfai_image_load(tar_path: str, user: User = Depends(get_api_user)):
    # 这里先不加 quota，因为 tar 和 image 都是会占用用户的存储 quota 的，用户可以选择把他们删除掉
    loader = await AioUserSelector.find_one(user_name=image_loader_name)
    image_instance = await TrainImageSelector.a_find_one(shared_group=user.shared_group, image_tar=tar_path)
    if image_instance and image_instance.status == 'loading':
        task = await AioBaseTaskSelector.find_one(None, id=image_instance.task_id)
        if task and task.queue_status != QUE_STATUS.FINISHED:
            return {
                'success': 0,
                'msg': '镜像的加载/删除任务正在运行，不能重复提交'
            }
    await loader.quota.create_quota_df()
    workspace = '/marsv2/scripts'
    res = await create_task_base_queue(
        nb_name=f'load-image-{os.path.basename(tar_path).replace(":", "_").replace("@", "_")}',
        user=loader,
        code_file=f'{workspace}/load_image.sh {user.shared_group} {tar_path}',
        workspace=workspace,
        group=list(loader.quota.node_quota.keys())[0].split('-')[1],
        nodes=1, restart_count=0, task_type=TASK_TYPE.TRAINING_TASK,
        template='docker_ubuntu2004',
        priority=50,
        override_node_resource={
            'cpu': 0, 'memory': 0
        }
    )
    task = res.get('task', None)
    if not task:
        return {
            'success': 0,
            'msg': '创建加载任务失败'
        }
    # insert into train image
    sql_ = f'''
        insert into "train_image" ("image_tar", "shared_group", "status", "task_id")
        values (%s, %s, %s, %s) on conflict ("image_tar") do update set ("status", "task_id") = (%s, %s)
    '''
    sql, params = sql_params.format(sql_, (tar_path, user.shared_group, 'loading', task.id,
                                           'loading', task.id))
    try:
        async with a_db_engine.begin() as conn:
            await conn.execute(sqlalchemy.text(sql), params)
    except Exception as e:
        logging.error(e)
        return {
            'success': 0,
            'msg': '记录加载任务失败'
        }
    return {
        'success': 1,
        'msg': '启动了任务加载任务'
    }


async def update_image_status_by_task_id(task_id, status, image='', image_path=''):
    _sql = f'''
        update "train_image" set "status" = %s, "image" = %s, "path" = %s
        where "task_id" = %s
    '''
    sql, params = sql_params.format(_sql, (status, image, image_path, int(task_id)))
    try:
        async with a_db_engine.begin() as conn:
            await conn.execute(sqlalchemy.text(sql), params)
    except Exception as e:
        logging.error(e)
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': f'更新状态失败，请联系管理员'
        })


async def update_image_status_by_image_name(image_name, shared_group, task_id, status):
    _sql = f'''
        update "train_image" set "status" = %s, "task_id" = %s 
        where "image" = %s and shared_group = %s
    '''
    sql, params = sql_params.format(_sql, (status, int(task_id), image_name, shared_group))
    try:
        async with a_db_engine.begin() as conn:
            await conn.execute(sqlalchemy.text(sql), params)
    except Exception as e:
        logging.error(e)
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': f'更新状态失败，请联系管理员'
        })


async def hfai_image_update_status(task_id: str, user: User = Depends(get_api_user),
                                   status: str = None, image_name: str = None):
    if user.user_name != image_loader_name:
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': '该用户没有操作 hfai image 的权限'
        })
    image_path = None
    if image_name:
        fixed_base = '/weka-jd/prod/permanent/shared/hfai_images/'
        image_path = os.path.join(fixed_base, user.shared_group, image_name, 'blobs/sha256')
    await update_image_status_by_task_id(task_id=task_id, status=status, image=image_name, image_path=image_path)
    return {
        'success': 1,
        'msg': f'更新镜像任务 {task_id} 状态为 {status} 成功'
    }


async def hfai_image_list(user: User = Depends(get_api_user)):
    images = await TrainImageSelector.a_find_user_group_images(shared_group=user.shared_group)
    return {
        'success': 1,
        'data':  [dict(**i) for i in images]
    }


async def hfai_image_delete(user: User = Depends(get_api_user), image: str = None):
    loader = await AioUserSelector.find_one(user_name=image_loader_name)
    await loader.quota.create_quota_df()
    workspace = '/marsv2/scripts'
    image_name = os.path.basename(image)
    image_instance = await TrainImageSelector.a_find_one(shared_group=user.shared_group, image=image_name)
    if image_instance is None:
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': f'不存在要删除的镜像'
        })
    if image_instance.shared_group != user.shared_group:
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': f'只能删除自己组内的镜像，用户组: ${user.shared_group}'
        })
    task = await AioBaseTaskSelector.find_one(None, id=image_instance.task_id)
    if task and task.queue_status != QUE_STATUS.FINISHED:
        return {
            'success': 0,
            'msg': '镜像的加载/删除任务正在运行，不能重复提交'
        }
    res = await create_task_base_queue(
        nb_name=f'delete-image-{image_name.replace(":", "_").replace("@", "_")}',
        user=loader,
        code_file=f'{workspace}/delete_image.sh {image_name} {image_instance.path}',
        workspace=workspace,
        group=list(loader.quota.node_quota.keys())[0].split('-')[1],
        nodes=1, restart_count=0, task_type=TASK_TYPE.TRAINING_TASK,
        template='docker_ubuntu2004',
        priority=50,
        override_node_resource={
            'cpu': 0, 'memory': 0
        },
    )
    task = res.get('task', None)
    if not task:
        return {
            'success': 0,
            'msg': '创建删除任务失败'
        }

    await update_image_status_by_image_name(image_name=image_name,
                                            shared_group=user.shared_group,
                                            status="deleting", task_id=task.id)

    return {
        'success': 1,
        'msg': '启动了镜像删除任务'
    }
