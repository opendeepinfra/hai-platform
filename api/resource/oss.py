import aiohttp
import enum
from fastapi import Depends, HTTPException, Body
from typing import Optional

from api.depends import get_api_user, request_limitation
from api.app import app
import conf
from conf.utils import FileType, FileList, FileInfoList, FilePrivacy, DatasetType, SyncStatus, SyncDirection
from db import a_redis as redis
from server_model.user import User

OSS_SERVICE_TOKEN = None
OSS_SERVICE_ENDPOINT = conf.POLYAXON_SETTING.oss_service.endpoint
from logm import logger

class RequestMethod(enum.Enum):
    GET = 0
    POST = 1


async def async_requests(method: RequestMethod,
                         url: str,
                         assert_success: list = None,
                         **kwargs):
    """async request"""
    if assert_success is None:
        assert_success = [1]
    async with aiohttp.ClientSession() as session:
        action = session.get if method == RequestMethod.GET else session.post
        async with action(url=url, **kwargs) as response:
            result = await response.json()
            # 先assert拿到正确的返回，再assert success字段
            assert result.get('success', None), result
            assert result['success'] in assert_success, result['msg']
            return result


class UserTaskQueue(object):
    '''
    进行中用户任务队列，简单限制用户并发操作，避免文件读写冲突
    '''

    def __init__(self, ttl=900):
        # 限制过期时间，默认15m
        self._ttl = ttl

    def key(self, username):
        return f'oss:busy_users:{username}'

    async def _busy(self, username):
        exist = await redis.exists(self.key(username))
        return exist

    async def enqueue(self, username):
        if await self._busy(username):
            return False
        await redis.set(self.key(username), self._ttl, ex=self._ttl)
        return True

    async def finish(self, username):
        await redis.delete(self.key(username))


UserTasks = UserTaskQueue()


async def get_sts_token(
    file_type,
    name,
    file_privacy: Optional[FilePrivacy] = FilePrivacy.PRIVATE,
    dataset_type: Optional[DatasetType] = DatasetType.MINI,
    ttl_seconds: Optional[int] = 1800,
    user: User = Depends(get_api_user)):
    '''
    获取aliyun sts token
    Args:
        name: str, 标识
        file_type: str, 文件类型, 可选值workspace/env/dataset
        file_privacy: str, 只对dataset类型必要, 标识数据集隐私类别, 可选值public/group_shared/private
        dataset_type: str, 只对dataset类型必要, 标识数据集类型, 可选值mini/full
        ttl_seconds: int, 可选参数, token的过期时间, 取值范围900~21600, 默认1800

    Returns:
        {
            "oss" : {
                "access_key_id" : "STS.xxxxxx",
                "access_key_secret" : "xxxxxxxx",
                "bucket" : "yinghuoai",
                "endpoint" : "http://oss-cn-hangzhou.aliyuncs.com",
                "expiration" : "2022-05-05T06:41:56Z",
                "request_id" : "F9725C1F-408B-5879-93E7-9CA681B51D4F",
                "security_token" : "xxxxxxxx",
                "authorized_path": "datasets/public/mini/test"
            },
            "success" : 1
        }
    '''
    resp = await get_sts_token_request(user.user_name, user.shared_group, name,
                                       file_type, file_privacy, dataset_type,
                                       ttl_seconds)
    return resp


async def check_oss_quota(request_mb: int, user: User = Depends(get_api_user)):
    """
    校验oss quoa
    # TODO: 该接口改用 weka 限额
    @param request_mb int: 新申请的MB
    """
    await user.quota.create_quota_df()
    limit = int(user.quota.quota('oss:storage:upload'))
    if request_mb >= limit:
        return {
            'success':
            0,
            'msg':
            f'quota exceed, request size: {request_mb}MB, limit size: {limit}MB'
        }
    return {'success': 1}


async def set_sync_status(file_type: FileType,
                          name: str,
                          direction: SyncDirection,
                          status: SyncStatus,
                          local_path: str,
                          cluster_path: str,
                          user: User = Depends(get_api_user)):
    '''
    记录同步任务状态到数据库
    '''
    try:
        await user.aio_db.set_sync_status(file_type, name, direction, status,
                                      local_path, cluster_path)
    except Exception as e:
        return {'success': 0, 'msg': str(e)}
    return {'success': 1}


async def get_sync_status(file_type: FileType, name: str, user: User = Depends(get_api_user)):
    '''从数据库获取同步任务状态
    Args:
        file_type: 文件类型，可选值为 workspace / env / dataset
        name: workspace / env 标识，如为''或'*'，表示列举所有

    Returns:
        data: json格式，返回同步任务列表，如
        [
            {
                "user_name" : "gwj",
                "user_role" : "external",
                "cluster_path" : "default/gwj/workspaces/myws-1",
                "local_path" : "/code/hfai",
                "file_type" : "workspace",
                "name" : "myws-1",
                "pull_status" : "finished",
                "last_pull" : "2022-04-08T23:36:47",
                "push_status" : "finished",
                "last_push" : "2022-04-09T23:36:47",
                "created_at" : "2022-04-08T23:36:47.600582",
                "updated_at" : "2022-04-12T15:41:50.317655",
                "deleted_at" : None
            }
        ]
    '''
    await user.sync_status.create_sync_status()
    status_list = user.sync_status.sync_status_of_type(file_type, name)
    return {'success': 1, 'data': status_list}


async def delete_files(file_type: FileType, name: str, user: User = Depends(get_api_user)):
    '''
    删除文件
    '''
    url = f'{OSS_SERVICE_ENDPOINT}/delete_files?username={user.user_name}&group={user.shared_group}&name={name}&file_type={file_type}'
    await _do_request(RequestMethod.POST, url, 60)
    await user.aio_db.delete_sync_status(file_type, name)
    return {'success': 1}


async def list_cluster_files(name: str,
                             file_type: FileType,
                             no_checksum: bool,
                             recursive: bool,
                             page: int,
                             size: int,
                             file_list: FileList = Body(...),
                             user: User = Depends(get_api_user)):
    """
    返回用户集群内工作区的文件列表
    """
    return await list_cluster_files_request(user.user_name, user.shared_group,
                                            name, file_type, no_checksum,
                                            recursive, file_list, page, size)


async def sync_to_cluster(name: str,
                          file_type: FileType,
                          no_zip: bool,
                          file_list: FileList = Body(...),
                          user: User = Depends(get_api_user)):
    """
    同步用户已上传文件到集群内工作区目录
    """
    if not await UserTasks.enqueue(user.user_name):
        logger.info(f'{user.user_name} 有其他同步任务正在执行，请稍后重试')
        return {'success': 0, 'msg': f'{user.user_name} 有其他同步任务正在执行，请稍后重试'}
    try:
        url = f'{OSS_SERVICE_ENDPOINT}/sync_to_cluster?token={user.token}&username={user.user_name}&userid={user.user_id}&group={user.shared_group}&name={name}&file_type={file_type}&no_zip={no_zip}'
        resp = await _do_request(RequestMethod.POST, url, 3600, file_list)
        await UserTasks.finish(user.user_name)
    except Exception as e:
        logger.error(f'sync to cluster failed: {str(e)}')
        await UserTasks.finish(user.user_name)
        raise HTTPException(status_code=500,
                            detail=f'sync to cluster failed: {str(e)}')
    return resp


async def sync_to_cluster_status(index, user: User = Depends(get_api_user)):
    try:
        url = f'{OSS_SERVICE_ENDPOINT}/sync_to_cluster/status?index={index}'
        resp = await _do_request(RequestMethod.GET, url, 60)
    except Exception as e:
        logger.error(f'get sync_to_cluster status failed: {str(e)}')
        raise HTTPException(
            status_code=500,
            detail=f'get sync_to_cluster status failed: {str(e)}')
    return resp


async def sync_to_cluster_queue(user: User = Depends(get_api_user)):
    """
    查询任务队列状态
    """
    try:
        url = f'{OSS_SERVICE_ENDPOINT}/sync_to_cluster/queue'
        resp = await _do_request(RequestMethod.GET, url, 60)
    except Exception as e:
        logger.error(f'get sync_to_cluster queue failed: {str(e)}')
        raise HTTPException(
            status_code=500,
            detail=f'get sync_to_cluster queue failed: {str(e)}')
    return resp


async def sync_from_cluster(
    name: str = None,
    file_type: FileType = None,
    file_privacy: Optional[FilePrivacy] = FilePrivacy.PRIVATE,
    dataset_type: Optional[DatasetType] = DatasetType.MINI,
    file_infos: FileInfoList = Body(...),
    user: User = Depends(get_api_user)):
    """
    同步集群内工作区目录到 oss
    """
    if not await UserTasks.enqueue(user.user_name):
        logger.info(f'{user.user_name} 有其他同步任务正在执行，请稍后重试')
        return {'success': 0, 'msg': f'{user.user_name} 有其他同步任务正在执行，请稍后重试'}
    try:
        url = f'{OSS_SERVICE_ENDPOINT}/sync_from_cluster?token={user.token}' \
              f'&username={user.user_name}&group={user.shared_group}&name={name}' \
              f'&file_type={file_type}&file_privacy={file_privacy}'
        if file_type == FileType.DATASET:
            url += f'&dataset_type={dataset_type}'
        resp = await _do_request(RequestMethod.POST, url, 3600, file_infos)
        await UserTasks.finish(user.user_name)
    except Exception as e:
        logger.error(f'sync from cluster failed: {str(e)}')
        await UserTasks.finish(user.user_name)
        raise HTTPException(status_code=500,
                            detail=f'sync from cluster failed: {str(e)}')
    return resp


#          dependencies=[Depends(request_limitation)])
async def sync_from_cluster_status(index, user: User = Depends(get_api_user)):
    try:
        url = f'{OSS_SERVICE_ENDPOINT}/sync_from_cluster/status?index={index}'
        resp = await _do_request(RequestMethod.GET, url, 60)
    except Exception as e:
        logger.error(f'get sync_from_cluster status failed: {str(e)}')
        raise HTTPException(
            status_code=500,
            detail=f'get sync_from_cluster status failed: {str(e)}')
    return resp


#          dependencies=[Depends(request_limitation)])
async def sync_from_cluster_queue(user: User = Depends(get_api_user)):
    """
    查询任务队列状态
    """
    try:
        url = f'{OSS_SERVICE_ENDPOINT}/sync_from_cluster/queue'
        resp = await _do_request(RequestMethod.GET, url, 60)
    except Exception as e:
        logger.error(f'get sync_from_cluster queue failed: {str(e)}')
        raise HTTPException(
            status_code=500,
            detail=f'get sync_from_cluster queue failed: {str(e)}')
    return resp


#           dependencies=[Depends(request_limitation)])
async def sync_from_cluster_cancel(
    name: str = None,
    file_type: FileType = None,
    file_privacy: Optional[FilePrivacy] = FilePrivacy.PRIVATE,
    dataset_type: Optional[DatasetType] = DatasetType.MINI,
    file_infos: FileInfoList = Body(...),
    user: User = Depends(get_api_user)):
    """
    停止指定任务
    """
    try:
        url = f'{OSS_SERVICE_ENDPOINT}/sync_from_cluster/cancel?username={user.user_name}&group={user.shared_group}&name={name}&file_type={file_type}&file_privacy={file_privacy}'
        if file_type == FileType.DATASET:
            url += f'&dataset_type={dataset_type}'
        resp = await _do_request(RequestMethod.POST, url, 60, file_infos)
    except Exception as e:
        logger.error(f'cancel sync from cluster failed: {str(e)}')
        raise HTTPException(
            status_code=500,
            detail=f'cancel sync from cluster failed: {str(e)}')
    return resp


async def get_oss_service_token_inner():
    global OSS_SERVICE_TOKEN
    # 暂时不设置token expired，不刷新
    if OSS_SERVICE_TOKEN is not None:
        return OSS_SERVICE_TOKEN
    username = conf.POLYAXON_SETTING.oss_service.username
    password = conf.POLYAXON_SETTING.oss_service.password

    url = f'{OSS_SERVICE_ENDPOINT}/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    body = f'username={username}&password={password}'
    timeout = aiohttp.ClientTimeout(total=60)
    try:
        result = await async_requests(RequestMethod.POST,
                                      url,
                                      headers=headers,
                                      data=body,
                                      timeout=timeout)
    except Exception as e:
        logger.error(f'request oss service token failed: {str(e)}')
        raise HTTPException(status_code=500, detail='internel service failed')
    OSS_SERVICE_TOKEN = result.get('access_token')
    return OSS_SERVICE_TOKEN


async def list_cluster_files_request(username, group, name, file_type,
                                     no_checksum, recursive,
                                     file_list: FileList, page: int,
                                     size: int):
    token = await get_oss_service_token_inner()
    url = f'{OSS_SERVICE_ENDPOINT}/list_cluster_files?username={username}&group={group}&name={name}&file_type={file_type}&no_checksum={no_checksum}&recursive={recursive}&page={page}&size={size}'
    headers = {
        'Authorization': f'Bearer {token}',
        'WWW-Authenticate': 'Bearer',
        'Content-Type': 'application/json',
    }
    timeout = aiohttp.ClientTimeout(total=900)
    async with aiohttp.ClientSession() as session:
        async with session.post(url=url,
                                headers=headers,
                                timeout=timeout,
                                data=file_list.json()) as response:
            result = await response.json()
            if response.status != 200:
                logger.error(f'list cluster files failed: {result}')
                raise HTTPException(
                    status_code=response.status,
                    detail=f'list cluster files failed: {result}')

    return dict(result, **{'success': 1, 'msg': '获取成功'})


async def get_sts_token_request(username, group, name, file_type: FileType,
                                file_privacy: FilePrivacy,
                                dataset_type: DatasetType, ttl_seconds: int):
    url = f'{OSS_SERVICE_ENDPOINT}/get_sts_token?username={username}&group={group}&name={name}&file_type={file_type}&ttl_seconds={ttl_seconds}&file_privacy={file_privacy}'
    if file_type == FileType.DATASET:
        url += f'&dataset_type={dataset_type}'
    try:
        result = await _do_request(RequestMethod.GET, url, 60)
    except Exception as e:
        logger.error(f'request sts token failed: {str(e)}')
        raise HTTPException(status_code=500, detail='request sts token failed')
    return result


async def _do_request(method, url, timeout=60, data=None):
    oss_service_token = await get_oss_service_token_inner()
    headers = {
        'Authorization': f'Bearer {oss_service_token}',
        'WWW-Authenticate': 'Bearer',
    }
    if data is None:
        return await async_requests(
            method,
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout))
    headers['Content-Type'] = 'application/json'
    return await async_requests(method,
                                url,
                                headers=headers,
                                timeout=aiohttp.ClientTimeout(total=timeout),
                                data=data.json())
