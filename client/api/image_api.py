
import asyncio
import os
import shlex
import shutil
import sysconfig
import tempfile

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


# ---------------------------------------------------------------------------
# `images push`（S8-4）：本地 tar → 对象存储（stage1）→ 共享盘（stage2）→ 自动登记
#
# 复用 `haiworkspace` 的 push 实现（分片、断点续传、幂等键、状态轮询全在既有代码里），
# 本层只负责：预检拿落点 → 造一个「只含该 tar 的暂存目录」→ 调子进程 → 登记。
# ---------------------------------------------------------------------------

def _resolve_workspace_bin() -> str:
    '''
    解析 haiworkspace 可执行文件。

    优先复用 `client/api/venv_api.py` 的实现（惰性导入：haienv 未安装时不影响 images），
    失败时按同样的顺序兜底（$HAI_WORKSPACE_BIN → PATH → PLUGIN_LIST → scripts → /usr/local/bin）。
    '''
    try:
        from .venv_api import _resolve_workspace_bin as impl
        resolved = impl()
        if resolved:
            return resolved
    except Exception:
        pass
    import shutil as _shutil
    candidates = []
    env_bin = os.environ.get('HAI_WORKSPACE_BIN')
    if env_bin:
        candidates.append(env_bin)
    which = _shutil.which('haiworkspace')
    if which:
        candidates.append(which)
    try:
        from hfai.client.commands.utils import PLUGIN_LIST
        candidates.append(PLUGIN_LIST.get('haiworkspace'))
    except Exception:
        pass
    try:
        candidates.append(os.path.join(sysconfig.get_path('scripts'), 'haiworkspace'))
    except Exception:
        pass
    candidates.append('/usr/local/bin/haiworkspace')
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return next((c for c in candidates if c), 'haiworkspace')


def _build_image_push_cmd(local_path, remote_path, provider, force, no_checksum, no_diff,
                          list_timeout, sync_timeout, cloud_connect_timeout, token_expires,
                          part_mb_size, proxy):
    '''
    组装 `haiworkspace push` 命令（镜像专用）。

    `remote_path` 必须是**对象存储 key 前缀**（API-19 返回的 `cloud_path`），
    与服务端 `get_base_path(..., FileType.IMAGE)` 的 `cloud_base_path` 逐字节一致。

    `--no_zip` **恒为真**（ADR-I13）：服务端对 `*.zip` 会解包到 `{cluster}/.hfai/`，
    落盘就成了 `xxx.tar.zip`，而 `ctr images import` 要的是 tar 本身。
    '''
    from hfai.conf.utils import FileType

    workspace_bin = _resolve_workspace_bin()
    cmd = [workspace_bin]
    # `hai-cli workspace push` 会派发成 `haiworkspace push`；只有落到主 CLI 时才补 `workspace` 词
    if os.path.basename(workspace_bin) in ('hai-cli', 'hfai', 'hfai_cli.py'):
        cmd.append('workspace')
    cmd += [
        'push',
        '--file_type', FileType.IMAGE.value,
        '--image_provider', provider or 'oss',
        '--image_local_path', local_path,
        '--image_remote_path', remote_path,
        '--no_zip',
        '--list_timeout', str(list_timeout),
        '--sync_timeout', str(sync_timeout),
        '--cloud_connect_timeout', str(cloud_connect_timeout),
        '--token_expires', str(token_expires),
        '--part_mb_size', str(part_mb_size),
    ]
    if force:
        cmd.append('--force')
    if no_checksum:
        cmd.append('--no_checksum')
    if no_diff:
        cmd.append('--no_diff')
    if proxy:
        cmd.extend(['--proxy', proxy])
    return ' '.join(shlex.quote(str(item)) for item in cmd)


def _stage_image_tar(local_tar, filename):
    '''
    造一个**只含该 tar** 的暂存目录：push 是按目录 diff 的，直接传单个文件会被判成「空目录」。

    优先硬链接（同文件系统时不复制 GB 级数据），失败再退化为复制。
    '''
    stage_dir = tempfile.mkdtemp(prefix='hai-image-push-')
    target = os.path.join(stage_dir, filename)
    try:
        os.link(local_tar, target)
    except OSError:
        shutil.copy2(local_tar, target)
    return stage_dir


async def push_image_tar(local_tar, image=None, force=False, no_load=False, no_checksum=False,
                         no_diff=False, list_timeout=300, sync_timeout=1800,
                         cloud_connect_timeout=120, token_expires=1800, part_mb_size=100,
                         provider='', proxy='', **kwargs):
    '''
    把本地镜像 tar 送进集群（① API-19 预检 → ② workspace push → ③ API-15 登记）。

    :return dict: {'success': 0/1, 'msg': <可读结果>}
    '''
    if not os.path.isfile(local_tar):
        # 本地就不存在时**不发起任何请求**（与 `images load` 的既有行为一致，TC-UP-11③）
        return {'success': 0, 'msg': f'不存在这个镜像包：{local_tar}'}

    filename = os.path.basename(local_tar)
    file_size = os.path.getsize(local_tar)
    user = User(token=kwargs.get('token', mars_token()))

    # ---------------------------------------------------------------- ① API-19 预检
    try:
        pre = await user.image.async_push_precheck(file=filename, image=image, file_size=file_size)
    except Exception as e:
        return {'success': 0,
                'msg': f'预检失败：{e}（若为「接口不存在 / Not Found」，说明集群服务端还没上线 '
                       f'images 上传通道 —— 发布顺序必须是「先服务端，后客户端」）'}
    if not isinstance(pre, dict) or pre.get('success') != 1:
        msg = pre.get('msg') if isinstance(pre, dict) else pre
        return {'success': 0, 'msg': f'预检失败：{msg}'}

    # provider 以服务端返回的为准（服务端才是权威：103 上是 s3/rustfs，客户端默认 oss 会失败）
    upload_provider = str(pre.get('provider') or provider or 'oss')
    proc_name = pre.get('name')
    image_name = pre.get('image') or image or filename
    cloud_path = pre.get('cloud_path') or ''
    cluster_path = pre.get('cluster_path') or ''
    image_tar = pre.get('image_tar') or ''
    if not (proc_name and cloud_path and cluster_path and image_tar):
        return {'success': 0,
                'msg': '预检失败：集群服务端未返回完整落点（name / cloud_path / cluster_path / '
                       'image_tar），无法确定上传位置；请升级集群服务端后再试'}

    # 容量上限：本地先判一次（服务端也会在 API-19 判，双保险且失败更快，OPS-07 / TC-UP-10）
    try:
        max_bytes = int(pre.get('max_tar_bytes') or 0)
    except Exception:
        max_bytes = 0
    if max_bytes > 0 and file_size > max_bytes:
        return {'success': 0,
                'msg': f'镜像 tar 大小 {file_size} 超过上限 {max_bytes}（[image].max_tar_bytes），已中止'}

    # ---------------------------------------------------------------- ② 上传（stage1 + stage2）
    skipped_upload = False
    if pre.get('exists') and pre.get('registered') and not force:
        skipped_upload = True
        print(f'该 tar 已在集群且已登记（{proc_name}），跳过上传；如需重传请加 --force')
    else:
        stage_dir = _stage_image_tar(local_tar, filename)
        try:
            cmd = _build_image_push_cmd(stage_dir, cloud_path, upload_provider, force, no_checksum, no_diff,
                                        list_timeout, sync_timeout, cloud_connect_timeout,
                                        token_expires, part_mb_size, proxy)
            print(f'开始上传：{filename}（{file_size} 字节）→ {cloud_path}/{filename}')
            if os.system(cmd):
                return {'success': 0,
                        'msg': f'上传失败：`{cmd}` 退出码非 0（stage1/stage2 的失败原因见上方输出；'
                               f'可重试或加 --force）'}
        finally:
            shutil.rmtree(stage_dir, ignore_errors=True)

    # ---------------------------------------------------------------- ③ API-15 登记
    if no_load:
        return {'success': 1,
                'msg': f'已上传到集群共享盘（{image_tar}），按要求跳过登记（--no-load）；'
                       f'之后可执行 `hai-cli images load {image_tar}` 完成登记'}

    # stage2 的「进度到 100%」与「文件真的落在共享盘上」之间有窗口期（服务端进度是下载字节数，
    # 之后才做落盘与状态收尾）。实测踩过：立刻 load 会得到「共享盘上不存在镜像包」（D13）。
    # 因此登记前必须**用 API-19 复查 exists**，最多等 60s；仍不存在则按「已上传未登记」报错。
    ready = bool(pre.get('exists'))
    if not ready:
        for _ in range(30):
            await asyncio.sleep(2)
            try:
                chk = await user.image.async_push_precheck(file=filename, image=image_name,
                                                          file_size=file_size)
            except Exception:
                chk = None
            if isinstance(chk, dict) and chk.get('success') == 1 and chk.get('exists'):
                ready = True
                break
    if not ready:
        return {'success': 0,
                'msg': f'上传已受理，但共享盘上尚未出现该 tar（{image_tar}）；'
                       f'请稍后重试 `hai-cli images load {image_tar} --image {image_name}`'}

    try:
        result = await user.image.async_load(image_tar=image_tar, image=image_name, force=force)
    except Exception as e:
        return {'success': 0,
                'msg': f'镜像已上传但登记失败：{e}；可重试 `hai-cli images load {image_tar} '
                       f'--image {image_name}`'}
    if not isinstance(result, dict) or result.get('success') != 1:
        msg = result.get('msg') if isinstance(result, dict) else result
        # FR-20：**必须**把「上传成功但登记失败」与「上传失败」分开报，不得静默成功
        return {'success': 0,
                'msg': f'镜像已上传但登记失败：{msg}；可重试 `hai-cli images load {image_tar} '
                       f'--image {image_name}`'}
    prefix = '该 tar 已在集群，' if skipped_upload else '上传并登记成功：'
    return {'success': 1,
            'msg': f'{prefix}{result.get("msg", "操作完成")}（image={result.get("image") or image_name}）'}
