from hfai.base_model.base_user_modules import IUserImage
from ...api.api_config import get_mars_url as mars_url
from ...api.api_utils import async_requests, RequestMethod


class UserImage(IUserImage):
    async def async_get(self):
        url = f'{mars_url()}/ugc/user/train_image/list?token={self.user.token}'
        return await async_requests(RequestMethod.POST, url, retries=3, timeout=60)

    async def async_load(self, image_tar, image=None, force=False):
        # 变更型调用：retries 保持默认 1（幂等由服务端按 image_tar upsert 保证，不主动重试，FR-06/I12）
        url = f'{mars_url()}/ugc/user/train_image/load?token={self.user.token}'
        payload = {'image_tar': image_tar}
        if image:
            payload['image'] = image
        if force:
            payload['force'] = 1
        return await async_requests(RequestMethod.POST, url, json=payload, allow_unsuccess=True)

    async def async_delete(self, image):
        url = f'{mars_url()}/ugc/user/train_image/delete?token={self.user.token}'
        return await async_requests(RequestMethod.POST, url, json={'image': image}, allow_unsuccess=True)

    async def async_push_precheck(self, file, image=None, file_size=None):
        # 只读接口（API-19）：返回 name / image / image_tar / cloud_path / cluster_path /
        # exists / registered / max_tar_bytes，供客户端决定是否跳过上传与本地快速失败
        url = f'{mars_url()}/ugc/user/train_image/push_precheck?token={self.user.token}'
        payload = {'file': file}
        if image:
            payload['image'] = image
        if file_size:
            payload['file_size'] = int(file_size)
        return await async_requests(RequestMethod.POST, url, json=payload, allow_unsuccess=True)
