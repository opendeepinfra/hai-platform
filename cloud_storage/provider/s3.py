'''
S3 兼容对象存储 provider（用于 RustFS / MinIO 等自建对象存储）。

与 OSSApi 的差异：
- 使用 boto3（S3 协议 + SigV4），而不是阿里云 oss2（OSS 私有协议）
- 对象 tagging 用 S3 原生的 `x-amz-tagging` / PutObjectTagging，而不是 `x-oss-tagging`
- 没有阿里云 STS 角色扮演：`get_access_token` 直接下发配置里的静态 AK/SK
  （见设计文档 §16 开放问题；P0 最小闭环接受该降级，生产应改用 STS 或 bucket policy）

本模块会被 `plugins/haiworkspace/install.sh` 一起打包进 hai-cli 客户端插件，
因此**只允许依赖 boto3 与标准库**，不要 import 服务端专有的模块。
'''

import os
import stat
import threading
from dataclasses import dataclass
from typing import List, Dict, Callable, Tuple, Optional

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

from .interface import CloudObjectStorageInterface, CloudApiException


DEFAULT_REGION = 'us-east-1'


@dataclass
class FileInfo:
    path: str
    size: Optional[int] = None
    last_modified: Optional[str] = None
    md5: Optional[str] = None
    ignored: Optional[bool] = None


def _parse_tagging(tagging) -> Dict[str, str]:
    '''
    兼容两种入参：
      - dict: {'size': '12', 'md5': '...'}
      - str : 'size=12&md5=...&source=cluster&filemode=0640'
    '''
    if tagging is None:
        return {}
    if isinstance(tagging, dict):
        return {str(k): str(v) for k, v in tagging.items() if v is not None}
    result = {}
    for item in str(tagging).split('&'):
        if not item or '=' not in item:
            continue
        key, _, value = item.partition('=')
        result[key.strip()] = value.strip()
    return result


class _ProgressReporter:
    '''
    boto3 的 Callback 回调的是「本次已传输字节」，这里换算成累计值并限频上报。
    '''

    def __init__(self, percentage: Callable, total_bytes: Optional[int] = None):
        self.percentage = percentage
        self.total_bytes = total_bytes
        self.seen = 0
        self._last_reported = -1
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self.seen += int(bytes_amount)
            current = self.seen
        if self.percentage is None:
            return
        # 限频：进度变化 >= 1% 或首次才上报（FR-20）
        if self.total_bytes:
            if current < self.total_bytes and (current - self._last_reported) * 100 < self.total_bytes:
                return
        self._last_reported = current
        try:
            self.percentage(current, self.total_bytes)
        except Exception:
            pass


class S3Api(CloudObjectStorageInterface):

    def __init__(self,
                 endpoint: str,
                 access_key_id: str,
                 access_key_secret: str,
                 security_token: str = '',
                 breakpoint_info_path: str = None,
                 proxies: Dict[str, str] = None,
                 connect_timeout: int = 120,
                 region: str = DEFAULT_REGION,
                 addressing_style: str = 'path',
                 uid: str = None,
                 role_arn: str = None) -> None:
        assert not (endpoint is None or access_key_id is None
                    or access_key_secret is None), 'missing cloud config'
        self.endpoint = endpoint
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.security_token = security_token or ''
        self.breakpoint_info_path = breakpoint_info_path
        self.proxies = proxies
        self.connect_timeout = connect_timeout
        self.region = region
        self.addressing_style = addressing_style
        self._client = None

    # ------------------------------------------------------------------ 基础

    def _get_client(self):
        # 注意：不要在 __init__ 里建 client —— provider 会在 ProcessPoolExecutor
        # 的子进程（spawn）里被重新导入并构造，惰性创建可以避开 pickle 问题。
        if self._client is None:
            session = boto3.session.Session()
            self._client = session.client(
                's3',
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.access_key_secret,
                aws_session_token=self.security_token or None,
                region_name=self.region,
                verify=False,
                config=Config(
                    signature_version='s3v4',
                    s3={'addressing_style': self.addressing_style},
                    proxies=self.proxies or None,
                    connect_timeout=self.connect_timeout,
                    read_timeout=max(self.connect_timeout, 300),
                    retries={'max_attempts': 3, 'mode': 'standard'},
                ))
        return self._client

    def _get_bucket_handler(self, bucket_name: str, **kwargs):
        return bucket_name

    # ------------------------------------------------------------------ 列举

    def list_bucket(self,
                    bucket_name: str,
                    prefix: str,
                    recursive: bool = True,
                    max_keys: int = 1000,
                    max_retries: int = 3,
                    **kwargs) -> Tuple[List[FileInfo], List[FileInfo]]:
        client = self._get_client()
        files, folders = list(), list()
        paginator_kwargs = {'Bucket': bucket_name, 'Prefix': prefix, 'MaxKeys': max_keys}
        if not recursive:
            paginator_kwargs['Delimiter'] = '/'
        try:
            paginator = client.get_paginator('list_objects_v2')
            for page in paginator.paginate(**paginator_kwargs):
                for item in page.get('CommonPrefixes', []) or []:
                    folders.append(FileInfo(path=item.get('Prefix')))
                for obj in page.get('Contents', []) or []:
                    last_modified = obj.get('LastModified')
                    files.append(FileInfo(
                        path=obj.get('Key'),
                        size=int(obj.get('Size', 0)),
                        last_modified=last_modified.strftime('%Y-%m-%d %H:%M:%S') if last_modified else None,
                    ))
        except Exception as e:
            raise CloudApiException(f'list bucket {bucket_name}/{prefix} failed: {str(e)}')
        return files, folders

    # ------------------------------------------------------------------ 传输

    def resumable_download(self, bucket_name: str, key: str, filename: str,
                           multipart_threshold: int, part_size: int,
                           percentage: Callable, num_threads: int,
                           **kwargs) -> None:
        client = self._get_client()
        total_bytes = None
        try:
            total_bytes = client.head_object(Bucket=bucket_name, Key=key).get('ContentLength')
        except Exception:
            total_bytes = None
        transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold or 100 * 1024 * 1024,
            multipart_chunksize=part_size or 100 * 1024 * 1024,
            max_concurrency=num_threads or 4,
            use_threads=bool(num_threads),
        )
        dirname = os.path.dirname(filename)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname, exist_ok=True)
        reporter = _ProgressReporter(percentage, total_bytes)
        try:
            client.download_file(bucket_name, key, filename,
                                 Config=transfer_config,
                                 Callback=reporter)
        except Exception as e:
            raise CloudApiException(f'download {key} failed: {str(e)}')

    def resumable_upload(self, bucket_name: str, key: str, filename: str,
                         multipart_threshold: int, part_size: int,
                         percentage: Callable, num_threads: int,
                         tagging: Dict[str, str], **kwargs) -> None:
        client = self._get_client()
        tags = _parse_tagging(tagging)
        extra_args = {}
        if tags:
            # S3 用 Tagging 查询串：urlencode 后由 botocore 处理
            extra_args['Tagging'] = '&'.join(f'{k}={v}' for k, v in tags.items())
        try:
            total_bytes = os.path.getsize(filename)
        except OSError:
            total_bytes = None
        transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold or 100 * 1024 * 1024,
            multipart_chunksize=part_size or 100 * 1024 * 1024,
            max_concurrency=num_threads or 4,
            use_threads=bool(num_threads),
        )
        reporter = _ProgressReporter(percentage, total_bytes)
        try:
            client.upload_file(filename, bucket_name, key,
                               ExtraArgs=extra_args or None,
                               Config=transfer_config,
                               Callback=reporter)
        except Exception as e:
            raise CloudApiException(f'upload {key} failed: {str(e)}')

    # ------------------------------------------------------------------ tagging

    def get_object_tagging(self, bucket_name: str, key: str, **kwargs) -> Dict[str, str]:
        client = self._get_client()
        try:
            resp = client.get_object_tagging(Bucket=bucket_name, Key=key)
        except Exception as e:
            # 对象不存在 / 无 tagging 都按「无元数据」降级处理（FR-09）
            code = ''
            try:
                code = e.response['Error']['Code']
            except Exception:
                pass
            if code in ('NoSuchKey', '404', 'NoSuchTagSet', 'AccessDenied'):
                return {}
            raise CloudApiException(f'get tagging failed {key}: {str(e)}')
        return {item['Key']: item['Value'] for item in resp.get('TagSet', []) or []}

    def set_object_tagging(self, bucket_name: str, key: str,
                           tag: Dict[str, str], **kwargs) -> None:
        client = self._get_client()
        tag_set = [{'Key': str(k), 'Value': str(v)} for k, v in _parse_tagging(tag).items()]
        if not tag_set:
            return
        try:
            client.put_object_tagging(Bucket=bucket_name, Key=key,
                                      Tagging={'TagSet': tag_set})
        except Exception as e:
            raise CloudApiException(str(e))

    # ------------------------------------------------------------------ 删除

    def batch_delete_objects(self, bucket_name: str, files: List[str], **kwargs) -> None:
        client = self._get_client()
        files = list(files)
        try:
            for i in range(0, len(files), 1000):
                batch = files[i:i + 1000]
                client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': [{'Key': k} for k in batch], 'Quiet': True})
        except Exception as e:
            raise CloudApiException(str(e))

    # ------------------------------------------------------------------ 凭证

    def get_access_token(self, bucket_name: str, prefix: str, ttl_seconds,
                         **kwargs) -> Dict[str, str]:
        '''
        P0 降级实现：S3 兼容存储（RustFS/MinIO）没有阿里云 STS 角色扮演，
        这里直接下发配置中的静态 AK/SK（security_token 为空）。

        安全提示：这意味着客户端拿到的凭据不受 prefix 限制，仅适用于自建/内网测试环境。
        生产环境应改为：RustFS/MinIO 的 STS（AssumeRole + inline policy）或 bucket policy。
        '''
        return {
            'access_key_id': self.access_key_id,
            'access_key_secret': self.access_key_secret,
            'security_token': self.security_token or '',
            'expiration': '',
            'bucket': bucket_name,
            'endpoint': self.endpoint,
            'authorized_path': prefix,
        }


def _unused_stat_import_guard():
    # 保持与 oss.py 一致的导入面，避免 lint 报未使用
    return stat
