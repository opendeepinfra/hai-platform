#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
把 hai-cli images（用户自定义镜像）的运行期配置幂等写入 103 的 override.toml。

涉及两个配置面（设计 docs/haiplatform/images/images-server-design.md §9.1）：
  ① [cloud.storage.service] image_path   镜像资产共享根（单点，conf.utils.get_image_root）
  ② [image] enabled / enabled_groups / loader_backend / load_helper_image /
     data_local_path / containerd_socket / runtime_bin_dir / image_mount_root

⚠️ 103 的环境约束（实测）：平台 StatefulSet 只把 /nfs-shared/hai-platform/workspace
   （以及 db/log/redis）挂进 pod，**没有挂** /nfs-shared/hai-platform/image，而 `images load`
   必须能在平台 pod 里 stat 到 tar（IMAGE_TAR_NOT_FOUND 是契约的一部分）。
   因此本环境把 image_path 取到 workspace 之下：{workspace}/image。
   换成 /nfs-shared/hai-platform/image 只需给 StatefulSet 加一个 hostPath 挂载后改这里。

用法（host 103）：
    sudo python3 patch_image_override.py [override.toml 路径]
可用环境变量覆盖：IMAGE_ROOT / IMAGE_ENABLED / IMAGE_GROUPS / LOADER_BACKEND /
LOAD_HELPER_IMAGE / DATA_LOCAL_PATH / CONTAINERD_SOCKET / RUNTIME_BIN_DIR
'''

import io
import os
import re
import sys

DEFAULT_OVERRIDE = '/nfs-shared/hai-platform/override.toml'

IMAGE_ROOT = os.environ.get('IMAGE_ROOT', '/nfs-shared/hai-platform/workspace/image')
IMAGE_KEYS = [
    ('enabled', os.environ.get('IMAGE_ENABLED', 'true')),
    ('enabled_groups', os.environ.get('IMAGE_GROUPS', "['hfai']")),
    ('enabled_users', '[]'),
    ('registry', "'registry.high-flyer.cn'"),
    ('loader_backend', "'%s'" % os.environ.get('LOADER_BACKEND', 'register')),
    ('name_regex', "'^[A-Za-z0-9][A-Za-z0-9._-]{0,63}(?::[A-Za-z0-9][A-Za-z0-9._-]{0,63})?$'"),
    ('load_helper_image', "'%s'" % os.environ.get('LOAD_HELPER_IMAGE', 'docker.io/library/busybox:latest')),
    ('data_local_path', "'%s'" % os.environ.get('DATA_LOCAL_PATH', '/data_local')),
    # R-2：link 脚本访问节点容器运行时的通路（MicroK8s）
    ('containerd_socket', "'%s'" % os.environ.get('CONTAINERD_SOCKET',
                                                  '/var/snap/microk8s/common/run/containerd.sock')),
    ('runtime_bin_dir', "'%s'" % os.environ.get('RUNTIME_BIN_DIR', '/snap/microk8s/current/bin')),
    ('runtime_lib_dir', "'%s'" % os.environ.get('RUNTIME_LIB_DIR', '/lib/x86_64-linux-gnu')),
    ('runtime_loader_file', "'%s'" % os.environ.get('RUNTIME_LOADER_FILE', '/lib64/ld-linux-x86-64.so.2')),
    ('image_mount_root', "'%s'" % IMAGE_ROOT),
]


def set_key(src, key, value, section):
    '''在指定 section 内幂等设置 key = value；section 不存在时在文件末尾新建。'''
    header = '[%s]' % section
    if header not in src:
        src = src.rstrip('\n') + '\n\n%s\n%s = %s\n' % (header, key, value)
        return src
    # 找到 section 的范围
    start = src.index(header) + len(header)
    m = re.search(r'^\[', src[start:], flags=re.M)
    end = start + (m.start() if m else len(src) - start)
    body = src[start:end]
    if re.search(r'^%s\s*=' % re.escape(key), body, flags=re.M):
        body = re.sub(r'^%s\s*=.*$' % re.escape(key), '%s = %s' % (key, value), body, count=1, flags=re.M)
    else:
        body = body.rstrip('\n') + '\n%s = %s\n' % (key, value)
    return src[:start] + body + src[end:]


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OVERRIDE
    src = io.open(path, encoding='utf-8').read()

    if '[cloud.storage.service]' not in src:
        print('缺少 [cloud.storage.service] 段，请先用 config_cloud_storage.sh 初始化：%s' % path)
        return 1

    src = set_key(src, 'image_path', "'%s'" % IMAGE_ROOT, 'cloud.storage.service')
    for key, value in IMAGE_KEYS:
        src = set_key(src, key, value, 'image')

    io.open(path, 'w', encoding='utf-8').write(src)
    print('override.toml 已更新: image_path=%s' % IMAGE_ROOT)
    print('  [image] ' + ', '.join('%s=%s' % (k, v) for k, v in IMAGE_KEYS))
    return 0


if __name__ == '__main__':
    sys.exit(main())
