
import math
from datetime import date, datetime
from typing import Optional

import munch
import pandas as pd

from db import MarsDB
from logm import logger
from server_model.user_data import TrainImageTable


def _json_scalar(value):
    '''
    出口归一化（FR-11 / 修 I8）：numpy 标量 → 原生类型，时间 → ISO 字符串，NaN → None。

    背景：`df.to_dict('records')` 会把 `task_id` 变成 `np.int64`，FastAPI 的 jsonable_encoder
    无法编码 → 接口 500。必须在这里统一转换，而不是依赖调用方。
    '''
    if value is None:
        return None
    # numpy scalar（np.int64 / np.float64 / np.bool_ ...）都有 .item()
    if not isinstance(value, (str, bytes, dict, list, tuple, set)) and hasattr(value, 'item'):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    return value


def _normalize_row(row: dict) -> dict:
    return {key: _json_scalar(value) for key, value in row.items()}


def _refresh_train_image_cache():
    '''
    写后刷新进程内表缓存（设计 §7.4 / DEV-12）。

    - 无议会时 `AutoBaseTable → DBBaseTable`：每次访问都重查 DB，这里是空操作；
    - 有议会时 `RoamingBaseTable` 会缓存 `_df`，必须 reload 才能读到刚写的行。
    刷新失败只告警，不能影响写入结果。
    '''
    try:
        private_table = getattr(TrainImageTable, 'private_table', None)
        if private_table is not None and hasattr(private_table, 'reload'):
            private_table.reload()
    except Exception as e:
        logger.warning(f'刷新 train_image 缓存失败（不影响写入）: {e}')


class TrainImageSelector:
    @classmethod
    def _convert_to_datetime(cls, df: pd.DataFrame):
        if len(df) == 0:
            return df
        df.created_at = pd.Series(df.created_at.dt.to_pydatetime(), index=df.index, dtype='object')
        df.updated_at = pd.Series(df.updated_at.dt.to_pydatetime(), index=df.index, dtype='object')
        return df

    @classmethod
    def add_url(cls, image: munch.Munch):
        if image.get('registry') and image.get('shared_group') and image.get('image'):
            image.image_url = '/'.join([image.registry, image.shared_group, image.image])
        return image

    @classmethod
    def find_one(cls, image) -> Optional[munch.Munch]:
        df = TrainImageTable.df
        df = df[df.image == image]
        if len(df) > 0:
            return cls.add_url(munch.Munch.fromDict(df.iloc[0].to_dict()))
        return None

    @classmethod
    async def a_find_one(cls, shared_group, image: str = None, image_tar: str = None) -> Optional[munch.Munch]:
        df = await TrainImageTable.async_df
        df = df[df.shared_group == shared_group]
        df = df[df.image == image] if image is not None else df
        df = df[df.image_tar == image_tar] if image_tar is not None else df
        if len(df) > 0:
            return cls.add_url(munch.Munch.fromDict(df[df.updated_at == df.updated_at.max()].iloc[0].to_dict()))
        return None

    @classmethod
    async def a_find_user_group_images(cls, shared_group: str) -> list[dict]:
        '''
        本组全部状态行（含 `deleted`），**updated_at DESC**（FR-11 / 修 I7：客户端取首个为基准，
        只有 DESC 才等于「以最新为准」），出口已归一化（修 I8）。
        '''
        df = await TrainImageTable.async_df
        df = df[df.shared_group == shared_group].sort_values('updated_at', ascending=False)
        return [_normalize_row(row) for row in cls._convert_to_datetime(df).to_dict('records')]

    @classmethod
    async def a_find_user_group_image_urls(cls, shared_group: str, status: str = None):
        df = await TrainImageTable.async_df
        df = df[df.shared_group == shared_group]
        df = df[df.status == status] if status is not None else df
        if len(df) == 0:
            return []
        return (df.registry + '/' + df.shared_group + '/' + df.image).tolist()

    @classmethod
    async def a_find_by_group_and_tar(cls, shared_group: str, image_tar: str) -> Optional[dict]:
        '''按 (shared_group, image_tar) 取一行（幂等判定用），出口归一化。'''
        df = await TrainImageTable.async_df
        df = df[(df.shared_group == shared_group) & (df.image_tar == image_tar)]
        if len(df) == 0:
            return None
        row = df.sort_values('updated_at', ascending=False).iloc[0].to_dict()
        return _normalize_row(row)

    @classmethod
    async def a_upsert_image(cls, image_tar: str, image: str, path: str, shared_group: str,
                             registry: str, status: str, task_id: int = 0,
                             message: str = '', user_name: str = '') -> None:
        '''
        幂等 upsert（FR-03/FR-06，HC-01）。

        `where "train_image"."status" in ('failed', 'deleted')` 保证**已 loaded 的行不被覆盖**
        （`on conflict ... do update` 的 where 不满足时该次 insert 静默不生效，也不报错）。
        `image_tar` 与 `path` 是**两个独立字段**（设计 §3.1 实现修正 I18），此处分别赋值。
        '''
        sql = '''
            insert into "train_image"
                ("image_tar", "image", "path", "shared_group", "registry", "status",
                 "task_id", "message", "user_name")
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict ("shared_group", "image_tar") do update set
                "image"      = excluded."image",
                "registry"   = excluded."registry",
                "path"       = excluded."path",
                "status"     = excluded."status",
                "task_id"    = excluded."task_id",
                "message"    = excluded."message",
                "user_name"  = excluded."user_name",
                "updated_at" = current_timestamp
            where "train_image"."status" in ('failed', 'deleted')
        '''
        params = (image_tar, image, path, shared_group, registry, status,
                  int(task_id), message, user_name)
        await MarsDB().a_execute(sql, params)
        _refresh_train_image_cache()

    @classmethod
    async def a_report_status(cls, shared_group: str, image_tar: str, status: str,
                              path: str = None, message: str = None, task_id: int = None) -> int:
        '''
        状态回报（API-16）：只更新 (shared_group, image_tar) 命中且在 `from_status` 白名单内的行。

        `path` / `message` 传 None 表示不改动该列（用 `coalesce(%s, "col")`），返回影响行数。
        '''
        sql = '''
            update "train_image"
            set "status" = %s,
                "path" = coalesce(%s, "path"),
                "message" = coalesce(%s, "message"),
                "task_id" = coalesce(%s, "task_id"),
                "updated_at" = current_timestamp
            where "shared_group" = %s and "image_tar" = %s
              and ("status" = %s or "status" = %s)
        '''
        params = (status, path, message, task_id, shared_group, image_tar,
                  _REPORTABLE_FROM[0], _REPORTABLE_FROM[1])
        result = await MarsDB().a_execute(sql, params)
        rowcount = int(getattr(result, 'rowcount', 0) or 0)
        if rowcount:
            _refresh_train_image_cache()
        return rowcount

    @classmethod
    async def a_delete_by_group_image(cls, shared_group: str, image: str) -> int:
        '''组内按镜像名软删（FR-05 / DEV-10），返回影响行数；重复删除返回 0（幂等）。'''
        sql = '''
            update "train_image"
            set "status" = %s, "updated_at" = current_timestamp
            where "shared_group" = %s and "image" = %s and "status" <> %s
        '''
        params = ('deleted', shared_group, image, 'deleted')
        result = await MarsDB().a_execute(sql, params)
        rowcount = int(getattr(result, 'rowcount', 0) or 0)
        if rowcount:
            _refresh_train_image_cache()
        return rowcount


#: 允许被状态回报覆盖的起始状态（其余一律 ILLEGAL_TRANSITION，设计 §7.3）
_REPORTABLE_FROM = ('processing', 'loading')
