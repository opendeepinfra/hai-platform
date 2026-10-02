from .oss import OSSApi
from .mock import MockApi

try:
    # S3 兼容对象存储（RustFS / MinIO）。需要 boto3；若环境没装则保持 None，
    # 由调用方回退到 MockApi 并给出提示，而不是让整个 import 失败。
    from .s3 import S3Api
except ImportError:  # pragma: no cover
    S3Api = None
