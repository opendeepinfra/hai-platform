
from .default import *
from .custom import *

from base_model.base_user import BaseUser


class UserDownloadedFiles(UserDownloadedFilesExtras):
    """
    用户上传到外部（bucket）的文件记账与用量统计。

    对应表 db_schemas/010.table_user_downloaded_files.sql（表已存在，P0 不做 DDL）。
    仓库中不存在 IUserDownloadedFiles 接口基类，因此沿用 UserDb 的写法：只继承 *Extras 基类。
    """

    def __init__(self, user: BaseUser):
        self.user = user
