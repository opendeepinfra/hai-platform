
from __future__ import annotations

from typing import TYPE_CHECKING

from base_model.base_user_modules import IUserImage

if TYPE_CHECKING:
    from .implement import UserImage


class UserImageExtras(IUserImage):
    async def async_get(self: UserImage):
        return {
            'mars_images': await self.async_get_train_images(),
            # 由硬编码 [] 改为真实查询（FR-02 / 修 I2）：本组全部状态行，updated_at DESC
            'user_images': await self.async_get_user_images(),
        }
