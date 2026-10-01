'''
领域层传输执行体（在 ProcessPoolExecutor 子进程里运行）。

从 cloud_storage/api.py 的 resumable_*_with_retry / *_callback 迁移而来，
签名与语义保持一致（设计 §5.5），但不再依赖宿主层（FastAPI / 路由）。

注意：本模块的函数会通过 `pool.submit(...)` 提交到 **spawn** 进程池，
因此必须定义在模块顶层，且通过 `cloud_storage.utils.cloud_api`（惰性代理）
在子进程内自行构造 provider —— 不要在父进程把 provider 实例传进来。

同理，**不要把 User 对象传进进程池**（User 会持有 access/db 等组件，且其构造依赖
k8s client 等重资源，pickle 不可靠）。记账只传 user_name / user_role 两个字符串，
SQL 在本模块内直接执行。
'''

import os
import stat
import time
from datetime import datetime, timedelta, timezone

from logm import logger

from cloud_storage.metrics import (RUNNING_TASKS_GAUGE, Failed_TASKS_COUNTER,
                                   SYNCED_FILESIZE_COUNTER, SYNCED_FILENUM_COUNTER,
                                   SYNCING_FILESIZE_GAUGE)
from cloud_storage.utils import cloud_api, status_recorder, status_key, record_metrics
from conf.utils import FileType, SyncStatus, tz_utc_8, unzip_dir
from db import MarsDB


# ---------------------------------------------------------------- 记账（子进程序内执行）
# 与 server_model/user_impl/user_db 的同步版 SQL 保持一致；这里用 user_name/user_role
# 两个字符串作为参数，避免把 User 对象 pickle 进进程池。

_INSERT_DOWNLOADED_FILE_SQL = '''
    insert into "user_downloaded_files" (
        "user_name", "user_role", "file_type", "file_path",
        "file_size", "file_mtime", "file_md5", "status"
    )
    values (%s, %s, CAST(%s AS file_type), %s, %s, %s, %s, CAST(%s AS sync_status))
    on conflict ("file_path", "file_md5") do update set
        "status" = excluded."status",
        "file_size" = excluded."file_size",
        "file_mtime" = excluded."file_mtime"
'''

_UPDATE_DOWNLOADED_FILE_STATUS_SQL = '''
    update "user_downloaded_files"
    set "status" = CAST(%s AS sync_status)
    where "file_path" = %s
      and "file_md5" = %s
'''


def _db_insert_downloaded_file(user_name, user_role, file_type, file_path,
                               file_size, file_mtime, file_md5, status):
    params = (user_name, user_role, file_type.value if hasattr(file_type, 'value') else file_type,
              (file_path or '')[:2047], file_size, str(file_mtime or '')[:255],
              str(file_md5 or '')[:255], status.value if hasattr(status, 'value') else status)
    MarsDB().execute(_INSERT_DOWNLOADED_FILE_SQL, params)


def _db_update_downloaded_file_status(file_path, file_md5, status):
    params = (status.value if hasattr(status, 'value') else status,
              (file_path or '')[:2047], str(file_md5 or '')[:255])
    MarsDB().execute(_UPDATE_DOWNLOADED_FILE_STATUS_SQL, params)



def resumable_download_with_retry(bucket_name,
                                  key,
                                  filename,
                                  file_type,
                                  multiget_threshold=None,
                                  part_size=None,
                                  num_threads=None,
                                  index=None,
                                  username=None,
                                  userid=None,
                                  use_zip=None,
                                  retries=3):
    '''把对象下载到集群路径；成功后按 tagging 恢复 filemode，zip 则解压后删除临时包。'''

    def percentage(consumed_bytes, total_bytes):
        if total_bytes:
            status_recorder.hset(status_key(index, 'progress', False), key, consumed_bytes)

    download_succeed = False
    is_dataset = file_type == FileType.DATASET
    for i in range(1, retries + 1):
        try:
            filemode = None
            tagging = dict()
            try:
                tagging = cloud_api.get_object_tagging(bucket_name, key)
                filemode = tagging.get('filemode', None)
            except Exception as e:
                logger.info(f'获取文件tagging {key}失败, 忽略: {str(e)}')

            logger.info(f'开始下载 {key}')
            if not download_succeed:
                cloud_api.resumable_download(bucket_name, key, filename, multiget_threshold,
                                             part_size, percentage, num_threads)
            download_succeed = True
            logger.info(f'下载 {key} 完成')
            if not is_dataset:
                os.chown(filename, int(userid), int(userid))
                if filemode:
                    os.chmod(filename, int(filemode, 8))

        except Exception as e:
            if i == retries:
                raise Exception({'key': key, 'size': 0, 'username': username,
                                 'file_type': file_type, 'msg': str(e)})
            logger.info(f'第{i}次下载{key}失败: {str(e)}, 尝试重试...')
            time.sleep(1)
            continue

    size = os.path.getsize(filename)
    if use_zip:
        dirname = os.path.dirname(filename).split('/.hfai')[0]
        logger.info(f'开始解压 {filename}')
        try:
            unzipped_files = unzip_dir(filename, dirname)
            if not is_dataset:
                for unzipped_file in unzipped_files:
                    f_path = os.path.join(dirname, unzipped_file)
                    os.chown(f_path, int(userid), int(userid))
        except FileNotFoundError as fe:
            if fe.filename and fe.filename.endswith('cap_bin'):
                logger.info('忽略cap_bin目录错误')
            else:
                raise Exception({'key': key, 'size': size, 'username': username,
                                 'file_type': file_type, 'msg': str(fe)})
        except Exception as e:
            logger.info(f'解压文件{filename}失败： {str(e)}')
            raise Exception({'key': key, 'size': size, 'username': username,
                             'file_type': file_type, 'msg': str(e)})
        finally:
            if os.path.exists(filename):
                os.remove(filename)

    if is_dataset and download_succeed:
        try:
            # 标记dataset回收时间为7d
            expire_at = (datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(tz_utc_8) +
                         timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            tagging['expire_at'] = expire_at
            cloud_api.set_object_tagging(bucket_name, key, tagging)
        except Exception as e:
            logger.info(f'记录dataset {key} tagging expire_at error: {str(e)}')

    return {'key': key, 'size': size, 'username': username, 'file_type': file_type}


def resumable_upload_with_retry(bucket_name,
                                key,
                                filename,
                                multipart_threshold=None,
                                part_size=None,
                                num_threads=None,
                                index=None,
                                user_name=None,
                                user_role=None,
                                file_type=None,
                                file_info=None,
                                filtered=False,
                                retries=3):
    '''把集群文件上传到对象存储，并写 tagging（size/md5/source/filemode）+ 记账。'''

    def percentage(consumed_bytes, total_bytes):
        if total_bytes:
            status_recorder.hset(status_key(index, 'progress', True), key, consumed_bytes)

    upload_succeed = False
    for i in range(1, retries + 1):
        try:
            if not filtered:
                # 校验云端是否有，避免打断情况下导致的重复上传，浪费带宽资源
                try:
                    tagging = cloud_api.get_object_tagging(bucket_name, key)
                    md5 = tagging.get('md5', None)
                    if md5 == file_info.md5:
                        logger.info(f'  {filename} 之前已上传, md5: {md5}, 跳过')
                        status_recorder.hset(status_key(index, 'progress', True), key, file_info.size)
                        return {'key': key, 'size': file_info.size,
                                'username': user_name, 'file_type': file_type}
                except Exception:
                    pass

            if not upload_succeed:
                with record_metrics('insert_downloaded_file'):
                    _db_insert_downloaded_file(user_name, user_role, file_type, filename,
                                               file_info.size, file_info.last_modified,
                                               file_info.md5, SyncStatus.RUNNING)
                src_file_mode = oct(stat.S_IMODE(os.lstat(filename).st_mode))
                tagging = f'size={file_info.size}&md5={file_info.md5}&source=cluster&filemode={src_file_mode}'
                logger.info(f'开始上传 {key}')
                cloud_api.resumable_upload(bucket_name, key, filename, multipart_threshold,
                                           part_size, percentage, num_threads, tagging)
                logger.info(f'上传 {key} 完成')
                upload_succeed = True
            with record_metrics('update_downloaded_file_status'):
                _db_update_downloaded_file_status(filename, file_info.md5, SyncStatus.FINISHED)
            return {'key': key, 'size': file_info.size,
                    'username': user_name, 'file_type': file_type}
        except Exception as e:
            if i == retries:
                logger.info(f'上传 {filename} 失败')
                with record_metrics('update_downloaded_file_status'):
                    _db_update_downloaded_file_status(filename, file_info.md5, SyncStatus.FAILED)
                raise Exception({'key': key, 'size': file_info.size, 'username': user_name,
                                 'file_type': file_type, 'msg': str(e)})
            logger.info(f'第{i}次上传{key}失败: {str(e)}, 尝试重试...')
            time.sleep(1)
            continue


def download_callback(future):
    rst = None
    try:
        rst = future.result()
        logger.debug(f'download callback discard {rst["key"]}')
        SYNCED_FILESIZE_COUNTER.labels('push', rst["username"], rst["file_type"]).inc(rst['size'])
        SYNCED_FILENUM_COUNTER.labels('push', rst["username"], rst["file_type"]).inc()
    except Exception as e:
        try:
            rst = e.args[0]
        except Exception:
            rst = {'username': 'unknown', 'file_type': 'unknown'}
        logger.debug(f'download callback exception with key {rst.get("key")}: {str(e)}')
        Failed_TASKS_COUNTER.labels('push', rst.get("username"), rst.get("file_type")).inc()
    finally:
        # 用 try/finally 保证 gauge 不会悬挂（修 F9）
        RUNNING_TASKS_GAUGE.labels('push', rst.get("username"), rst.get("file_type")).dec()


def upload_callback(future):
    rst = None
    try:
        rst = future.result()
        logger.debug(f'upload callback discard {rst["key"]}')
        SYNCED_FILESIZE_COUNTER.labels('pull', rst["username"], rst["file_type"]).inc(rst['size'])
        SYNCED_FILENUM_COUNTER.labels('pull', rst["username"], rst["file_type"]).inc()
    except Exception as e:
        try:
            rst = e.args[0]
        except Exception:
            rst = {'username': 'unknown', 'file_type': 'unknown'}
        logger.debug(f'upload callback exception with key {rst.get("key")}: {str(e)}')
        Failed_TASKS_COUNTER.labels('pull', rst.get("username"), rst.get("file_type")).inc()
    finally:
        RUNNING_TASKS_GAUGE.labels('pull', rst.get("username"), rst.get("file_type")).dec()
        size = rst.get('size') or 0
        SYNCING_FILESIZE_GAUGE.labels('pull', rst.get("username"), rst.get("file_type")).dec(size)


def batch_delete_objects_with_retry(bucket_name, files, retries=3):
    for i in range(1, retries + 1):
        try:
            cloud_api.batch_delete_objects(bucket_name, files)
            return
        except Exception as e:
            if i == retries:
                msg = f'删除bucket {files}失败'
                logger.info(msg)
                raise Exception(msg)
            logger.info(f'第{i}次删除bucket {files}失败: {str(e)}, 尝试重试...')
            continue
