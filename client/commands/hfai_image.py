import os.path

import asyncclick as click
from rich.console import Console
from rich.box import ASCII2
from rich.table import Table

from hfai.client.api.image_api import (fetch_images, load_image_tar, delete_image_by_name,
                                      push_image_tar)
from .utils import HandleHfaiGroupArgs, HandleHfaiCommandArgs

# 与 image_api 一致的失败提示前缀（服务端业务失败不打印裸异常栈，FR-12 / I10）
_PUSH_ERROR_PREFIX = '\033[1;35m ERROR: \033[0m'


@click.group(cls=HandleHfaiGroupArgs)
def images():
    """
    用户自定义镜像的管理接口

    主路径：`hai-cli images push <本地 tar>`：把本地 `docker save` 出来的 tar 上传到集群共享盘，
    成功后自动登记；随后 `hai-cli images list` 查看状态，并可用 `-i registry/<组>/<镜像>:<tag>` 提交任务。

    兼容旁路：手工把 tar 放到集群共享目录之后，用 `hai-cli images load <集群上的 tar 路径>` 登记。
    """
    pass


class WorkspaceHandleHfaiCommandArgs(HandleHfaiCommandArgs):
    def format_options(self, ctx, formatter):
        pieces = self.collect_usage_pieces(ctx)
        with formatter.section("Arguments"):
            if 'image_tar' in pieces:
                formatter.write_dl(rows=[('image_tar', '用户要加载进萤火的镜像 TAR 包。'
                                                       '在用户本地调用为 workspace 中的路径，'
                                                       '在萤火上调用则为其共享存储中的路径')])
            if 'image' in pieces:
                formatter.write_dl(rows=[('image', '完整的镜像名，[registry]/image:<tag>')])
        super(WorkspaceHandleHfaiCommandArgs, self).format_options(ctx, formatter)


@images.command(cls=WorkspaceHandleHfaiCommandArgs, name='list')
@click.option('-a', '--all', 'show_all', required=False, is_flag=True, default=False, show_default=True, help='是否显示所有镜像（默认隐藏 status 含 deleted 的记录）')
async def list_images(show_all=False):
    """
    列举用户组在萤火二号上的镜像列表，以及镜像在萤火二号上的状态
    """
    mars_images, user_imgs = await fetch_images()

    last_img_status = {}

    console = Console()

    mars_table = Table(title='萤火二号内建镜像', title_justify=True, box=ASCII2, style='dim', show_header=True)
    for column in ['image', 'default_python', 'cuda', 'supported_hf_envs', 'environments']:
        mars_table.add_column(column)
    default_flag = True
    for i in sorted(mars_images, key=lambda x: x['quota'], reverse=True):
        image = f'{i["env_name"]}{"(default)" if default_flag else ""}'
        default_flag = False
        default_python = i.get('config', {}).get('python', 'unspecified')
        cuda_version = i.get('config', {}).get('cuda', 'unknown')
        supported_hf_envs = ';'.join(i.get('config', {}).get('hf_envs', []))
        environments = ';'.join([f'{k}={v}' for k, v in i.get('config', {}).get('environments', {}).items()])
        mars_table.add_row(image, default_python, cuda_version, supported_hf_envs, environments)
    console.print(mars_table)

    user_table = Table(title='用户自定义镜像', title_justify=True, box=ASCII2, style='dim', show_header=True)
    for column in ['image', 'status', 'shared_group', 'image_tar', 'updated_at']:
        user_table.add_column(column)
    for i in user_imgs:
        # 防御性取值（R-8）：服务端字段缺失时不再 KeyError
        registry = i.get('registry', '')
        shared_group = i.get('shared_group', '')
        image = i.get('image', '')
        status = i.get('status', '')
        image_tar = i.get('image_tar', '')
        i_name = os.path.join(registry, shared_group, image)
        if i_name not in last_img_status:
            last_img_status[i_name] = status
        # 以最新的为准（服务端按 updated_at DESC 返回，首个即最新）
        if i_name in last_img_status and status != last_img_status[i_name]:
            status = f"{last_img_status[i_name]} by new tar({status})"
        if 'deleted' in status and not show_all:
            continue
        user_table.add_row(i_name,
                      status,
                      shared_group, os.path.basename(image_tar),
                      i.get('updated_at', ''),
                      end_section=True)
    console.print(user_table)


@images.command(cls=WorkspaceHandleHfaiCommandArgs, name='push')
@click.argument('image_tar', required=True, metavar='image_tar')
@click.option('-i', '--image', 'image', required=False, default=None,
              help='镜像名 name:tag（不含 "/"）；缺省由 tar 文件名派生，服务端不会自动补 tag')
@click.option('--force', 'force', required=False, is_flag=True, default=False, show_default=True,
              help='忽略「已在集群且已登记」的判定强制重传（同名不同内容时必须使用）')
@click.option('--no-load', 'no_load', required=False, is_flag=True, default=False, show_default=True,
              help='只上传到集群共享盘，不自动登记（之后可手动执行 images load）')
async def push_image(image_tar, image=None, force=False, no_load=False):
    """
    把本地镜像 tar 包上传到集群共享盘（复用对象存储通道），上传成功后自动登记

    这是准备自定义镜像的**主路径**；tar 包在本地即可（不需要先手工放到集群共享目录）。
    """
    result = await push_image_tar(image_tar, image=image, force=force, no_load=no_load)
    if result.get('success') == 1:
        print(result.get('msg', '操作完成'))
        return
    print(f'{_PUSH_ERROR_PREFIX}{result.get("msg") or "上传失败"}')
    raise SystemExit(1)


@images.command(cls=WorkspaceHandleHfaiCommandArgs, name='load')
@click.argument('image_tar', required=True, metavar='image_tar')
@click.option('-i', '--image', 'image', required=False, default=None,
              help='镜像名 name:tag（不含 "/"）；缺省由 tar 文件名派生，服务端不会自动补 tag')
@click.option('--force', 'force', required=False, is_flag=True, default=False, show_default=True,
              help='该 tar 的镜像记录已被删除时，强制重新加载')
async def load_image(image_tar, image=None, force=False):
    """
    登记一个**已经在集群共享目录里**的镜像 tar 包（兼容旁路）

    常规做法是 `hai-cli images push <本地 tar>`（上传 + 登记一条命令完成）；
    只有在运维已经手工把 tar 放到共享盘、或需要修复登记时，才使用本命令。
    """
    if os.path.exists(image_tar):
        abs_image_tar = os.path.abspath(image_tar)
        await load_image_tar(abs_image_tar, image=image, force=force)
    else:
        print('不存在这个镜像包')


@images.command(cls=WorkspaceHandleHfaiCommandArgs, name='delete')
@click.argument('image', required=True, metavar='image')
async def delete_image(image):
    """
    删除萤火二号上的镜像记录
    注意: 1、该操作**只把记录标记为 deleted，不回收任何存储空间**（registry tag / 共享盘 tar / 节点镜像缓存均保留）
          2、该镜像的命名并不会被回收
          3、用户也可以删除自己组内的其他用户的镜像（禁止跨组）
    """
    await delete_image_by_name(image_name=image)
