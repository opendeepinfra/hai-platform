from typing import List

import pandas as pd
import sqlalchemy

from db import db_engine, a_db_engine
from base_model.base_user import BaseUser


class UserImage:
    def __init__(self, user: BaseUser):
        self.user = user
        self._image_df = None

    @property
    def _sql(self):
        # 暂时只支持以 user_name 保存，而非 group_name
        return f"""
        select "user_name", "description", "image_ref", "created_at", "updated_at" 
        from "user_image" 
        where "user_name" = '{self.user.user_name}'
        """

    async def create_image_df(self):
        async with a_db_engine.begin() as conn:
            result = await conn.execute(sqlalchemy.text(self._sql))
        self._image_df = pd.DataFrame(result, columns=['usage_name', 'description', 'image_ref', 'created_at', 'updated_at'])

    @property
    def image_df(self):
        if self._image_df is None:
            self._image_df = pd.read_sql(self._sql, db_engine)
        return self._image_df

    def image_list(self, description=None):
        return self.image_df[self.image_df.description == description].to_dict('records') if description else self.image_df.to_dict('records')

    def find_one(self, description):
        images = self.image_df[self.image_df.description == description].sort_values('updated_at', ascending=False).to_dict('records')
        if len(images):
            return images[0]
        return None
