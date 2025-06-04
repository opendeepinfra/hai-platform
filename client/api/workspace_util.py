import os
import stat
from asyncio import sleep
from datetime import datetime, timedelta, timezone
from typing import List

import oss2
from oss2.exceptions import NotFound
from rich.box import ASCII2
from rich.console import Console
from rich.table import Table
from rich.progress import Progress

from .api_config import get_mars_token as mars_token
from .api_config import get_mars_url as mars_url
from .api_utils import async_requests, RequestMethod
from hfai.conf.utils import FileType, SyncDirection, SyncStatus, FileInfo, FileList, FileInfoList, bytes_to_human, \
    get_file_info, list_local_files_inner, slice_bytes, hashkey, zip_dir, tz_utc_8, resumable_download_with_retry, resumable_upload_with_retry
from itertools import chain


async def diff_local_cluster(local_path: str,
                             name: str,
                             file_type: str,
                             subpath_list: List[str],
                             no_checksum: bool = False,
                             no_diff: bool = False,
                             exclude_list: List = [],
                             timeout: int = 3600):
    """
    比较本地和远端目录diff
    @param local_path: 本地工作区目录
    @param name: 工作区或env的名字
    @param file_type: 类型
    @param subpath_list: diff的子目录
    @param no_checksum: 是否禁用checksum
    @param no_diff: 是否禁用差量上传
    @param exclude_list: 排除在外的路径
    @return local_only_files       list[FileInfo]: 本地未上传文件
            cluster_only_files     list[FileInfo]：远端未下载文件
            local_changed_files    list[FileInfo]: 本地与远端有差异文件（本地角度）
            cluster_changed_files  list[FileInfo]: 本地与远端有差异文件（远端角度）
    """
    local_only_files,  cluster_only_files, local_changed_files, cluster_changed_files = [], [], [], []
    print(f'开始遍历本地工作区目录...')
    local_files = list(chain(*[await list_local_files_inner(local_path, path, no_checksum) for path in subpath_list]))
    if no_diff:
        return local_files, cluster_only_files, local_changed_files, cluster_changed_files
    print(f'开始遍历集群工作区目录...')
    cluster_files = await list_cluster_files(name, file_type, subpath_list,
                                             no_checksum, timeout)
    local_files = [l_file for l_file in local_files if l_file.path not in exclude_list]
    cluster_files = [c_file for c_file in cluster_files if c_file.path not in exclude_list]
    local_files_map = {l_file.path: l_file for l_file in local_files}
    cluster_files_map = {c_file.path: c_file for c_file in cluster_files}
    for local_file in local_files:
        if local_file.path not in cluster_files_map:
            local_only_files.append(local_file)
        else:
            if local_file.md5 != cluster_files_map[local_file.path].md5 or \
                    local_file.size != cluster_files_map[local_file.path].size:
                local_changed_files.append(local_file)
                cluster_changed_files.append(cluster_files_map[local_file.path])
    for cluster_file in cluster_files:
        if cluster_file.path not in local_files_map:
            cluster_only_files.append(cluster_file)
    return local_only_files, cluster_only_files, local_changed_files, cluster_changed_files


def print_diff(local_only_files,
               cluster_only_files,
               changed_files,
               focus_list=['本地', '集群']):
    console = Console()
    table = Table(box=ASCII2, style='dim', show_header=False)
    for column in ['type', 'filename']:
        table.add_column(column)
    for note, files in zip(
        ["[green]本地未上传文件", "[yellow]集群未下载文件", "[red]本地与集群有差异文件"],
        [local_only_files, cluster_only_files, changed_files]):
        if any([word in note for word in focus_list]):
            table.add_row(
                note,
                '\t'.join([f'{f.path}({f.size // 1024}KB)'
                           for f in files]) if files else 'None',
                end_section=True)
    console.print(table)


############################################ 访问 server #################################################


async def get_sts_token(name, file_type, token_expires, **kwargs):
    """
    获取云端存储的临时token
    """
    token = kwargs.get('token', mars_token())
    timeout = 60
    url = f'{mars_url()}/ugc/get_sts_token?token={token}&name={name}&file_type={file_type}&ttl_seconds={token_expires}'
    result = await async_requests(RequestMethod.POST,
                                  url,
                                  retries=3,
                                  timeout=timeout)
    return result['oss']


async def check_oss_quota(request_mb, **kwargs):
    """
    @param request_mb int: 新申请的MB
    """
    token = kwargs.get('token', mars_token())
    timeout = 60
    url = f'{mars_url()}/ugc/check_oss_quota?token={token}&request_mb={request_mb}'
    await async_requests(RequestMethod.POST, url, retries=3, timeout=timeout)
    return


async def set_sync_status(file_type: FileType, workspace_name,
                          direction: SyncDirection, status: SyncStatus,
                          local_path, cluster_path, **kwargs):
    token = kwargs.get('token', mars_token())
    timeout = 60
    url = f'{mars_url()}/ugc/set_sync_status?token={token}&file_type={file_type}&name={workspace_name}&direction={direction}&status={status}&local_path={local_path}&cluster_path={cluster_path}'
    await async_requests(RequestMethod.POST, url, retries=3, timeout=timeout)
    return


async def get_sync_status(file_type: FileType, workspace_name='*', **kwargs):
    token = kwargs.get('token', mars_token())
    timeout = 60
    url = f'{mars_url()}/ugc/get_sync_status?token={token}&file_type={file_type}&name={workspace_name}'
    result = await async_requests(RequestMethod.POST,
                                  url,
                                  retries=3,
                                  timeout=timeout)
    return result['data']


async def delete_workspace(workspace_name, **kwargs):
    token = kwargs.get('token', mars_token())
    timeout = 60
    url = f'{mars_url()}/ugc/delete_files?token={token}&name={workspace_name}&file_type={FileType.WORKSPACE}'
    await async_requests(RequestMethod.POST, url, retries=3, timeout=timeout)
    return


async def list_cluster_files(name: str, file_type: str, subpaths: List[str],
                             no_checksum: bool, timeout: int, **kwargs):
    """
    获取集群文件详情列表
    @param name:
    @param file_type:
    @param subpaths: 子目录列表
    @param no_checksum: 是否禁用checksum
    @return: list[FileInfo], int: FileInfo列表
    """
    file_list = FileList(files=subpaths)
    data = f'{{"file_list": {file_list.json()}}}'
    token = kwargs.get('token', mars_token())
    base_url = f'{mars_url()}/ugc/list_cluster_files?token={token}&name={name}&file_type={file_type}&no_checksum={no_checksum}&recursive=True'

    ret: List[FileInfo] = []
    default_page_size = 100
    page = 1
    while True:
        url = f'{base_url}&page={page}&size={default_page_size}'
        result = await async_requests(RequestMethod.POST,
                                      url,
                                      retries=3,
                                      data=data,
                                      timeout=timeout)
        ret.extend([FileInfo(**d) for d in result['items']])
        if page * default_page_size >= result['total']:
            return ret
        page += 1
        await sleep(0.1)


async def _do_poll_status(batch_size, completed_size, url, progress, task):
    while True:
        result = await async_requests(RequestMethod.GET,
                                      url,
                                      retries=5,
                                      timeout=10)
        if result['status'] == 'finished':
            return
        if result['status'] == 'failed':
            raise Exception(result['msg'])
        if result['status'] == 'running':
            current = int(result['msg'])
            if current > 0:
                # server端同步正在进行中
                progress.update(task, completed=current + completed_size)
            if current >= batch_size:
                return
        await sleep(2)


async def sync_to_cluster(name: str, file_type: str, files: List[FileInfo],
                          total_size: int, no_zip: bool, timeout: int,
                          **kwargs):
    """
    同步文件到集群
    @param name: 文件标识
    @param file_type: 文件类型
    @param files: 文件列表
    @param total_size: 总共上传的大小
    """
    token = kwargs.get('token', mars_token())
    completed_size = 0
    batch = 50

    with Progress() as progress:
        task = progress.add_task('syncing', total=total_size)
        for current_idx in range(0, len(files), batch):
            paths = [f.path for f in files[current_idx:current_idx + batch]]
            batch_size = sum([f.size for f in files[current_idx:current_idx + batch]])
            file_list = FileList(files=paths)
            url = f'{mars_url()}/ugc/sync_to_cluster?token={token}&name={name}&file_type={file_type}&no_zip={no_zip}'
            result = await async_requests(
                RequestMethod.POST,
                url,
                retries=3,
                timeout=timeout,
                data=f'{{"file_list": {file_list.json()}}}')
            index = hashkey(mars_token(), name, file_type, *paths)
            await _do_poll_status(
                batch_size, completed_size,
                f'{mars_url()}/ugc/sync_to_cluster/status?token={token}&index={index}',
                progress, task)
            completed_size += batch_size
            progress.update(task, completed=completed_size)


async def sync_from_cluster(name: str, file_type: str, files: List[FileInfo],
                            total_size: int, timeout: int, **kwargs):
    """
    从集群同步文件
    @param name: 文件标识
    @param file_type: 文件类型
    @param files: 文件列表
    @param total_size: 总共下载的大小
    """
    batch = 50
    token = kwargs.get('token', mars_token())
    completed_size = 0

    with Progress() as progress:
        task = progress.add_task('syncing', total=total_size)
        for current_idx in range(0, len(files), batch):
            file_infos = FileInfoList(files=files[current_idx:current_idx + batch])
            batch_size = sum([f.size for f in file_infos.files])
            url = f'{mars_url()}/ugc/sync_from_cluster?token={token}&name={name}&file_type={file_type}'
            result = await async_requests(
                RequestMethod.POST,
                url,
                retries=3,
                timeout=timeout,
                data=f'{{"file_infos": {file_infos.json()}}}')
            index = hashkey(mars_token(), name, file_type,
                            *[f.path for f in file_infos.files])
            await _do_poll_status(
                batch_size, completed_size,
                f'{mars_url()}/ugc/sync_from_cluster/status?token={token}&index={index}',
                progress, task)
            completed_size += batch_size
            progress.update(task, completed=completed_size)


############################################ 访问 oss ####################################################


async def get_oss_bucket(name, file_type, token_expires):
    auth_token = await get_sts_token(name, file_type, token_expires)
    auth = oss2.StsAuth(auth_token['access_key_id'],
                        auth_token['access_key_secret'],
                        auth_token['security_token'])
    bucket = oss2.Bucket(auth, auth_token['endpoint'], auth_token['bucket'])
    return bucket


async def push_to_cluster(local_path: str,
                          remote_path: str,
                          name: str,
                          file_type: str,
                          force: bool = False,
                          no_checksum: bool = False,
                          exclude_list: List = [],
                          no_zip: bool = False,
                          no_diff: bool = False,
                          list_timeout: int = 300,
                          sync_timeout: int = 300,
                          token_expires: int = 1800):
    """
    上传本地文件目录到oss bucket，并删除远端孤儿目录，保持本地和远端目录一致
    @param local_path: 本地工作区目录
    @param remote_path: 远端工作区目录
    @param name: 标识
    @param file_type: 文件类型
    @param force: 是否强制推送并覆盖远端文件
    @param no_checksum: 是否禁用checksum
    """
    local_only_files, cluster_only_files, changed_files, _ = await diff_local_cluster(
        local_path,
        name,
        file_type,
        subpath_list=['./'],
        no_checksum=no_checksum,
        no_diff=no_diff,
        exclude_list=exclude_list,
        timeout=list_timeout)
    # 校验是否有未下载数据
    if len(changed_files) != 0:  # 暂时忽略集群侧新文件 or len(cluster_only_files) != 0:
        print_diff(local_only_files,
                   cluster_only_files,
                   changed_files,
                   focus_list=['本地'])
        if not force:
            print('集群中存在差异文件，请确认，可增加 --force 参数强制覆盖!')
            return False
        print('集群中存在差异文件，将被本次操作覆盖!')
    target_files = local_only_files + changed_files
    if len(target_files) == 0:
        print('数据已同步，忽略本次操作')
        return True

    if not no_zip:
        print('开始打包本地工作区目录...')
        zip_file_path = f'/tmp/{os.path.basename(local_path)}.zip'
        zip_dir(local_path, target_files, zip_file_path, exclude_list)
        target_files = [await get_file_info(zip_file_path, '/tmp', no_checksum)]

    upload_size = 0
    for f in target_files:
        upload_size += f.size

    bucket = await get_oss_bucket(name, file_type, token_expires)

    # 上传本地文件，注：需要在删除之后，避免误删
    # 小文件直接上传，大文件分片上传，阈值100MB
    await set_sync_status(file_type, name, SyncDirection.PUSH,
                          SyncStatus.STAGE1_RUNNING, local_path, remote_path)
    print(f'(1/2) 开始同步本地目录 {local_path} 到远端，共{bytes_to_human(upload_size)}...')
    with Progress() as progress:
        task = progress.add_task('pushing', total=upload_size)
        completed_size = 0

        def percentage(consumed_bytes, total_bytes):
            if total_bytes:
                progress.update(task,
                                completed=consumed_bytes + completed_size)

        # 默认一天后过期
        expire_at = (datetime.utcnow().replace(
            tzinfo=timezone.utc).astimezone(tz_utc_8) +
                     timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        for f in target_files:
            src_file = f'{local_path}/{f.path}' if no_zip else f'/tmp/{f.path}'
            dst_file = f'{remote_path}/{f.path}'
            try:
                # 先校验oss上是否有，避免打断情况下导致的重复上传，浪费带宽资源
                skip = False
                source = 'client'
                try:
                    tagging_rst = bucket.get_object_tagging(dst_file)
                    if not no_checksum and 'md5' in tagging_rst.tag_set.tagging_rule.keys():
                        skip = tagging_rst.tag_set.tagging_rule.get('md5', '') == f.md5
                        source = tagging_rst.tag_set.tagging_rule.get('source', 'client')
                except NotFound:
                    skip = False
                if skip:
                    # print(f'  {src_file} 之前已上传，跳过')
                    completed_size += f.size
                    progress.update(task, completed=completed_size)
                    continue
                src_file_mode = oct(stat.S_IMODE(os.lstat(src_file).st_mode))
                tagging = f'size={f.size}&source={source}&expire_at={expire_at}&filemode={src_file_mode}'
                if not no_checksum:
                    tagging += f'&md5={f.md5}'
                header = {oss2.headers.OSS_OBJECT_TAGGING: tagging}
                resumable_upload_with_retry(bucket,
                                            dst_file,
                                            src_file,
                                            headers=header,
                                            multipart_threshold=slice_bytes,
                                            part_size=slice_bytes,
                                            progress_callback=percentage,
                                            num_threads=4)
                completed_size += f.size
            except Exception as e:
                print(f'上传 {f.path} 失败: {e}')
                await set_sync_status(file_type, name, SyncDirection.PUSH,
                                      SyncStatus.STAGE1_FAILED, local_path,
                                      remote_path)
                if not no_zip:
                    os.remove(f'/tmp/{os.path.basename(local_path)}.zip')
                return False
    await set_sync_status(file_type, name, SyncDirection.PUSH,
                          SyncStatus.STAGE1_FINISHED, local_path,
                          remote_path)

    if not no_zip:
        os.remove(f'/tmp/{os.path.basename(local_path)}.zip')

    print('(2/2) 上传成功，开始同步到集群，请等待...')
    try:
        await sync_to_cluster(name, file_type, target_files, completed_size,
                              no_zip, sync_timeout)
    except Exception as e:
        print(str(e))
        return False
    return True


async def pull_from_cluster(local_path: str,
                            remote_path: str,
                            name: str,
                            file_type: str,
                            force: bool = False,
                            no_checksum: bool = False,
                            subpath='./',
                            list_timeout: int = 300,
                            sync_timeout: int = 300,
                            token_expires: int = 1800):
    """
    下载oss bucket文件到本地
    @param local_path: 本地工作区目录
    @param remote_path: 远端工作区目录
    @param name:
    @param file_type:
    @param force: 是否强制推送并覆盖远端文件
    @param no_checksum: 是否禁用checksum
    @param subpath: pull的子目录
    """
    local_only_files, cluster_only_files, _, changed_files = await diff_local_cluster(
        local_path, name, file_type, [subpath], no_checksum, timeout=list_timeout)

    if len(changed_files) != 0:
        print_diff(local_only_files,
                   cluster_only_files,
                   changed_files,
                   focus_list=['集群'])
        if not force:
            print('集群中存在差异文件，请确认，可增加 --force 参数强制覆盖!')
            return False
        print('集群中存在差异文件，将被本次操作覆盖!')

    if len(cluster_only_files + changed_files) == 0:
        print('数据已同步，忽略本次操作')
        return True

    paths = []
    download_size = 0
    for f in cluster_only_files + changed_files:
        paths.append(f.path)
        download_size += f.size
    print(f'(1/2) 开始同步集群数据，目录{paths}\n共{bytes_to_human(download_size)}, 请等待...')
    try:
        await sync_from_cluster(name, file_type,
                                cluster_only_files + changed_files,
                                download_size, sync_timeout)
    except Exception as e:
        print(str(e))
        return False

    print(f'(2/2) 集群同步成功，开始下载远端目录到本地...')
    bucket = await get_oss_bucket(name, file_type, token_expires)
    await set_sync_status(file_type, name, SyncDirection.PULL,
                          SyncStatus.STAGE2_RUNNING,
                          local_path, remote_path)
    # 遍历oss文件所用的目录前缀，需要严格字符串匹配，不支持'//', '\\'等路径
    with Progress() as progress:
        task = progress.add_task('pulling', total=download_size)
        completed_size = 0

        def percentage(consumed_bytes, total_bytes):
            if total_bytes:
                progress.update(task,
                                completed=consumed_bytes + completed_size)

        for fi in cluster_only_files + changed_files:
            local = f'{local_path}/{fi.path}'
            remote = f'{remote_path}/{fi.path}'
            try:
                dirname = os.path.dirname(local)
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                resumable_download_with_retry(bucket,
                                              remote,
                                              local,
                                              multiget_threshold=slice_bytes,
                                              part_size=slice_bytes,
                                              progress_callback=percentage,
                                              num_threads=4)
                completed_size += fi.size

            except Exception as e:
                print(f'下载 {remote} 失败：{e}')
                await set_sync_status(file_type, name, SyncDirection.PULL,
                                      SyncStatus.STAGE2_FAILED,
                                      local_path, remote_path)
                return False
    await set_sync_status(file_type, name, SyncDirection.PULL,
                          SyncStatus.FINISHED, local_path, remote_path)
    return True
