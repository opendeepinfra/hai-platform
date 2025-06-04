
from cached_property import cached_property

from base_model.base_user import BaseUser
from conf import POLYAXON_SETTING
from conf.flags import USER_ROLE
from server_model.user_impl import UserDb, UserStorage, UserQuota, UserConfig, \
    UserMessage, AioUserDb, UserImage, UserEnvironment, UserDownloadedFiles, UserSyncStatus
from server_model.user_impl.user_nodeport import UserNodePort


class User(BaseUser):
    def __init__(self, user_name, user_id, token, role, **kwargs):
        super().__init__(user_name, user_id, token, role, **kwargs)
        self.group_list_str = kwargs.get('group_list_str', '')
        # 用户默认属于自己的group 和 public
        self.group_list = self.group_list_str.split(',') if self.group_list_str else []
        self.group_list += [self.user_name, 'public', role]

    def __repr__(self):
        self_dict = self.__dict__
        return '\n'.join([f'{k}: {self_dict[k]}' for k in self_dict])

    @cached_property
    def config(self) -> UserConfig:
        return UserConfig(self)

    @cached_property
    def db(self) -> UserDb:
        return UserDb(self)

    @cached_property
    def aio_db(self) -> AioUserDb:
        return AioUserDb(self)

    @cached_property
    def storage(self) -> UserStorage:
        return UserStorage(self)

    @cached_property
    def quota(self) -> UserQuota:
        return UserQuota(self)

    @cached_property
    def nodeport(self) -> UserNodePort:
        return UserNodePort(self)

    @cached_property
    def message(self) -> UserMessage:
        return UserMessage(self)

    @cached_property
    def image(self) -> UserImage:
        return UserImage(self)

    @cached_property
    def downloaded_files(self) -> UserDownloadedFiles:
        return UserDownloadedFiles(self)

    @cached_property
    def sync_status(self) -> UserSyncStatus:
        return UserSyncStatus(self)

    @cached_property
    def environment(self) -> UserEnvironment:
        return UserEnvironment(self)

    @property
    def is_internal(self):
        return self.role == USER_ROLE.INTERNAL

    @property
    def uid(self):
        return self.user_id

    @cached_property
    def db_str_group_list(self) -> str:
        """
        返回用户在数据库用来 select WHERE user_name in ({db_str_group_list})
        @return:
        """
        return ','.join(map(lambda n: f"'{n}'", self.group_list))

