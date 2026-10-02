from __future__ import annotations

import os
import re

from .default import *
from .custom import *

from base_model.base_user_modules import IUserImage
from logm import logger
from server_model.selector import AioTrainEnvironmentSelector, TrainImageSelector


# ---------------------------------------------------------------------------
# 状态常量单点（设计 docs/haiplatform/images/images-server-design.md §7.3 / DEV-09）
#
# 三方口径（ADR-I6 / HC-03 / HC-04）：
#   · 任务侧只看**精确** 'loaded'（api/operation/default.py 的白名单，字面量不可改）
#   · 客户端只按**子串** 'deleted' 过滤（client/commands/hfai_image.py）
#   · 服务端是唯一写入方 → 词表必须同时满足上面两者
# ---------------------------------------------------------------------------
STATUS_PROCESSING = 'processing'
STATUS_LOADING = 'loading'
STATUS_LOADED = 'loaded'
STATUS_FAILED = 'failed'
STATUS_DELETED = 'deleted'
ALL_STATUSES = (STATUS_PROCESSING, STATUS_LOADING, STATUS_LOADED, STATUS_FAILED, STATUS_DELETED)
#: 状态回报允许的起始状态（其余一律 ILLEGAL_TRANSITION）
REPORTABLE_FROM = (STATUS_PROCESSING, STATUS_LOADING)
#: 已收敛的状态：重复 load 时原样返回，不重置（FR-06 幂等）
STABLE_LOADED_STATUSES = (STATUS_PROCESSING, STATUS_LOADING, STATUS_LOADED)


class UserImage(UserImageExtras, IUserImage):
    async def async_get_train_images(self):
        """ 获取内建的 train_images """
        mars_images = await AioTrainEnvironmentSelector.find_all()  # 萤火内建镜像
        await self.user.quota.create_quota_df()
        user_mars_images = []
        for mi in mars_images:
            if mi['env_name'] in self.user.quota.train_environments:
                mi['quota'] = int(self.user.quota.quota(f'train_environment:{mi["env_name"]}'))  # 这个是 np.int64
                user_mars_images.append(mi)
        return user_mars_images

    # ------------------------------------------------------------------ 工具

    @staticmethod
    def _errors():
        '''
        惰性 import（分层纪律 / NFR-04）：领域层不在导入期拉起 cloud_storage 的
        fastapi / oss2 / boto3 依赖链，单测可以只 import 本模块。
        '''
        from cloud_storage.service.errors import WorkspaceError, ErrorCode
        return WorkspaceError, ErrorCode

    @property
    def shared_group(self) -> str:
        '''镜像的归属组**只**来自服务端解析的 token（SEC-02），任何方法都不接受调用方传组。'''
        return self.user.shared_group

    def _normalize_image_tar(self, image_tar) -> str:
        '''
        归一化并校验 tar 路径：必须是 `get_image_root()` 之下的真实文件（FR-13 / SEC-01）。

        路径校验唯一入口是 `cloud_storage.utils.check_is_subpath`（禁止自造，设计 §3.4）。
        '''
        from conf.utils import get_image_root
        WorkspaceError, ErrorCode = self._errors()
        if image_tar is None or not str(image_tar).strip():
            raise WorkspaceError(ErrorCode.INVALID_PARAM,
                                 '缺少参数 image_tar（镜像 tar 包在共享盘上的路径）')
        raw = str(image_tar).strip()
        if '\x00' in raw:
            raise WorkspaceError(ErrorCode.INVALID_PARAM, 'image_tar 含非法字符')
        image_root = get_image_root()
        try:
            from cloud_storage.utils import check_is_subpath, ClientException
            check_is_subpath(image_root, raw)
        except ImportError:
            raise
        except Exception as e:
            raise WorkspaceError(ErrorCode.PATH_ESCAPE,
                                 f'image_tar 必须位于共享根 {image_root} 之下: {raw}（{e}）')
        normalized = os.path.normpath(os.path.abspath(raw))
        if not os.path.isfile(normalized):
            raise WorkspaceError(ErrorCode.IMAGE_TAR_NOT_FOUND,
                                 f'共享盘上不存在镜像包: {normalized}')
        try:
            if os.path.getsize(normalized) <= 0:
                raise WorkspaceError(ErrorCode.IMAGE_TAR_NOT_FOUND, f'镜像包为空: {normalized}')
        except OSError as e:
            raise WorkspaceError(ErrorCode.IMAGE_TAR_NOT_FOUND,
                                 f'镜像包不可读: {normalized}（{e}）')
        return normalized

    def _resolve_image_name(self, image, image_tar: str) -> str:
        '''
        解析并校验镜像名（FR-07）。

        · 缺省由 `basename(image_tar)` 派生（I6：旧客户端只发 tar 路径）
        · **不自动补 tag**（设计 §4.1 实现修正 I6b：任务侧是逐字节比较）
        · 必须不含 '/'（HC-05 / K1：三段 URL 的第三段）
        '''
        from conf.utils import derive_image_name, is_valid_image_name
        from cloud_storage.service.context import get_image_name_regex
        WorkspaceError, ErrorCode = self._errors()
        name = str(image).strip() if image else derive_image_name(image_tar)
        if not name or '/' in name:
            raise WorkspaceError(ErrorCode.INVALID_PARAM,
                                 f'镜像名非法: {image!r}（不能为空且不能含 "/"）')
        pattern = get_image_name_regex()
        if not is_valid_image_name(name) or not re.match(pattern, name):
            raise WorkspaceError(ErrorCode.INVALID_PARAM,
                                 f'镜像名非法: {name}（应形如 name[:tag]，仅允许字母数字与 . _ -）')
        return name

    @staticmethod
    def _load_result(row: dict, backend: str, reused: bool = False) -> dict:
        image_name = row.get('image')
        registry = row.get('registry') or ''
        shared_group = row.get('shared_group') or ''
        # 契约（Checklist 附录 A.1）：响应里的 `image` 是**完整三段 URL** —— 用户应当把它原样
        # 传给 `-i`；裸镜像名（train_image.image 列）另用 image_name 给出（字段只增不改，CMP-05）。
        image_url = '/'.join([registry, shared_group, image_name]) \
            if (registry and shared_group and image_name) else image_name
        return {
            'image': image_url,
            'image_name': image_name,
            'image_tar': row.get('image_tar'),
            'status': row.get('status'),
            'task_id': row.get('task_id') or 0,
            'path': row.get('path') or '',
            'backend': backend,
            'reused': reused,
        }

    # ------------------------------------------------------------------ 领域方法（设计 §5.2）

    async def async_get_user_images(self) -> list[dict]:
        ''' 列表：本组全部状态行，updated_at DESC，字段已归一化（FR-02 / FR-11）。 '''
        return await TrainImageSelector.a_find_user_group_images(self.shared_group)

    async def async_load(self, image_tar: str, image: str = None, force: bool = False) -> dict:
        '''
        加载登记（API-15 的业务主体，FR-03 / FR-06 / FR-07 / FR-13）。

        `register` 后端（P0 默认）：只校验 + 登记，状态直接 `loaded`，`path = image_tar`；
        真正的 import 推迟到 pod 启动时由 `link_hfai_image.sh` 完成（ADR-I2）。
        '''
        from cloud_storage.service.context import (check_image_enabled, get_image_loader_backend,
                                                   get_image_registry)
        WorkspaceError, ErrorCode = self._errors()
        check_image_enabled(self.user)
        shared_group = self.shared_group
        if not shared_group:
            raise WorkspaceError(ErrorCode.FORBIDDEN,
                                 f'用户 {self.user.user_name} 没有所属用户组，无法加载镜像')
        image_tar = self._normalize_image_tar(image_tar)
        image = self._resolve_image_name(image, image_tar)
        backend = get_image_loader_backend()
        registry = get_image_registry()

        existing = await TrainImageSelector.a_find_by_group_and_tar(shared_group, image_tar)
        if existing is not None:
            current = existing.get('status')
            if current in STABLE_LOADED_STATUSES:
                # 幂等：原样返回当前行，不重置状态、不新建（FR-06）
                logger.info(f'[IMAGE] load 幂等命中 user={self.user.user_name} '
                            f'image_tar={image_tar} status={current}')
                return self._load_result(existing, backend=backend, reused=True)
            if current == STATUS_DELETED and not force:
                raise WorkspaceError(
                    ErrorCode.IMAGE_NAME_CONFLICT,
                    f'该 tar 的镜像记录已被删除（{image}），如需重新加载请加 --force')

        if backend == 'register':
            status, path = STATUS_LOADED, image_tar
        else:
            # task / registry 后端（P1）：异步执行，processing 阶段**不得**把 tar 写进 path（I18）
            status, path = STATUS_PROCESSING, ''
            logger.warning(f'[IMAGE] loader_backend={backend} 的数据面执行体尚未实现（P0 仅支持 '
                           f'register），本次仅登记为 {status}: image_tar={image_tar}')

        await TrainImageSelector.a_upsert_image(
            image_tar=image_tar, image=image, path=path, shared_group=shared_group,
            registry=registry, status=status, task_id=0, message='', user_name=self.user.user_name)
        row = await TrainImageSelector.a_find_by_group_and_tar(shared_group, image_tar) or {
            'image': image, 'image_tar': image_tar, 'status': status, 'task_id': 0, 'path': path,
            'registry': registry, 'shared_group': shared_group}
        logger.info(f'[IMAGE] load 成功 user={self.user.user_name} shared_group={shared_group} '
                    f'image={image} image_tar={image_tar} status={row.get("status")} '
                    f'path={row.get("path")} backend={backend}')
        return self._load_result(row, backend=backend, reused=False)

    async def async_report_image_status(self, image_tar: str, status: str, path: str = None,
                                        message: str = '', task_id: int = None) -> dict:
        '''
        状态回报（API-16，FR-10 / SEC-04）：只有被登记的 `task_id` 可回报，非法迁移被拒。
        '''
        from cloud_storage.service.context import check_image_enabled
        WorkspaceError, ErrorCode = self._errors()
        check_image_enabled(self.user)
        if not image_tar or not str(image_tar).strip():
            raise WorkspaceError(ErrorCode.INVALID_PARAM, '缺少参数 image_tar')
        if not status or status not in ALL_STATUSES:
            raise WorkspaceError(ErrorCode.INVALID_PARAM,
                                 f'status 取值非法: {status!r}，合法取值为 {list(ALL_STATUSES)}')
        shared_group = self.shared_group
        normalized = os.path.normpath(os.path.abspath(str(image_tar).strip()))
        row = await TrainImageSelector.a_find_by_group_and_tar(shared_group, normalized)
        if row is None:
            raise WorkspaceError(ErrorCode.IMAGE_NOT_FOUND,
                                 f'镜像记录不存在: {normalized}（组 {shared_group}）')
        if task_id is None:
            raise WorkspaceError(ErrorCode.INVALID_PARAM, '回报状态必须携带 task_id')
        if int(task_id) != int(row.get('task_id') or 0):
            raise WorkspaceError(ErrorCode.FORBIDDEN,
                                 f'task_id 与该镜像记录不一致: {task_id} != {row.get("task_id")}')
        current = row.get('status')
        if current != status and current not in REPORTABLE_FROM:
            raise WorkspaceError(ErrorCode.ILLEGAL_TRANSITION,
                                 f'非法状态迁移: {current} -> {status}')
        if status == STATUS_LOADED and not (path or row.get('path')):
            raise WorkspaceError(ErrorCode.INVALID_PARAM, '回报 loaded 必须携带 path')

        updated = await TrainImageSelector.a_report_status(
            shared_group=shared_group, image_tar=normalized, status=status,
            path=path, message=(message or None), task_id=int(task_id))
        if updated == 0:
            raise WorkspaceError(ErrorCode.ILLEGAL_TRANSITION,
                                 f'非法状态迁移（并发更新）: {current} -> {status}')
        logger.info(f'[IMAGE] update_status user={self.user.user_name} shared_group={shared_group} '
                    f'image_tar={normalized} from_status={current} status={status} task_id={task_id}')
        return {'status': status, 'image_tar': normalized, 'from_status': current}

    async def async_delete(self, image: str) -> dict:
        '''
        删除（API-18，FR-05 / SEC-02 / SEC-05）：3 段解析 + 组校验 + 软删 + 幂等。
        '''
        from cloud_storage.service.context import check_image_enabled
        WorkspaceError, ErrorCode = self._errors()
        check_image_enabled(self.user)
        if not image or not str(image).strip():
            raise WorkspaceError(ErrorCode.INVALID_PARAM, '缺少参数 image')
        parts = str(image).strip().split('/')
        if len(parts) != 3 or not all(parts):
            raise WorkspaceError(ErrorCode.INVALID_PARAM,
                                 f'镜像名必须恰好 3 段 registry/shared_group/image: {image}')
        registry, group, name = parts
        if group != self.shared_group:
            raise WorkspaceError(ErrorCode.FORBIDDEN,
                                 f'禁止跨组删除镜像: {group} != {self.shared_group}')
        deleted = await TrainImageSelector.a_delete_by_group_image(self.shared_group, name)
        logger.info(f'[IMAGE] delete user={self.user.user_name} shared_group={self.shared_group} '
                    f'image={image} deleted={deleted}')
        return {'deleted': deleted, 'image': image, 'image_name': name}
