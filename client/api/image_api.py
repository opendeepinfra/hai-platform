
from .api_config import get_mars_token as mars_token
from ..model import User

# 服务端业务失败时的统一提示前缀（FR-12：不再向用户抛裸异常栈，I10）
_ERROR_PREFIX = '\033[1;35m ERROR: \033[0m'


def _result_payload(result):
    '''`images list` 的一致性兜底（R-8）：缺 result / 非 dict 时告警并按空处理，不崩。'''
    data = result.get('result') if isinstance(result, dict) else None
    if not isinstance(data, dict):
        print('\033[1;33m WARNING: \033[0m',
              '服务端返回缺少 result 字段，按空列表处理（请升级服务端或检查接口）')
        data = {}
    return data


def _ensure_success(result):
    '''业务失败时打印服务端 msg 并以退出码 1 结束（不打印 Python 栈）。'''
    if not isinstance(result, dict):
        print(_ERROR_PREFIX, '服务端返回格式异常')
        raise SystemExit(1)
    if result.get('success') != 1:
        print(_ERROR_PREFIX, result.get('msg') or '操作失败')
        raise SystemExit(1)
    return result


async def fetch_images(**kwargs):
    """
    :param kwargs:
    :return: mars_images, user_images
    """
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.image.async_get()
    data = _result_payload(result)
    return data.get('mars_images') or [], data.get('user_images') or []


async def load_image_tar(tar, image=None, force=False, **kwargs):
    """
    :param tar: 镜像 tar 包的路径
    :param image: 可选镜像名 name:tag；缺省由服务端按 tar 文件名派生
    :param force: 该 tar 的镜像记录已被删除时是否强制重新加载
    :return:
    """
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.image.async_load(tar, image=image, force=force)
    _ensure_success(result)
    print(result.get('msg', '操作完成'))


async def delete_image_by_name(image_name, **kwargs):
    """
    :param image_name: 镜像的名字（registry/shared_group/image:tag）
    :return:
    """
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.image.async_delete(image_name)
    _ensure_success(result)
    print(result.get('msg', '操作完成'))
