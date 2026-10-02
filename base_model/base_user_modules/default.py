
class IUserModule:
    def __init__(self, user):
        self.user = user

    def get(self):
        """
        返回用户与此组件相关的常用信息, 对应 HTTP API 的 GET 接口.
        """
        raise NotImplementedError

    async def async_get(self):
        return self.get()


class IUserStorage(IUserModule):
    pass


class IUserImage(IUserModule):
    async def async_get(self):
        raise NotImplementedError

    async def async_load(self, image_tar, image=None, force=False):
        """ 加载镜像 tar 包（FR-01：客户端与服务端各自实现，修 C-3） """
        raise NotImplementedError

    async def async_delete(self, image):
        """ 删除镜像（按 registry/shared_group/image 三段名） """
        raise NotImplementedError

