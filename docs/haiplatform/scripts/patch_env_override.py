#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
把 haienv（`hai-cli env push`）相关配置幂等写入 103 的运行时配置 override.toml。

为什么需要：`env_path` 必须满足 env_root = {env_path}/hfai_envs == dirname(HAIENV_PATH)，
103 测试环境的共享盘只有 /nfs-shared/hai-platform/workspace 同时被平台 pod 与任务 pod 挂载，
因此 env_path 取该目录（env_root = …/workspace/hfai_envs）。

用法（host 103）：
    sudo python3 patch_env_override.py [override.toml 路径]
'''

import io
import os
import re
import sys

DEFAULT_OVERRIDE = '/nfs-shared/hai-platform/override.toml'
ENV_PATH = os.environ.get('ENV_PATH', '/nfs-shared/hai-platform/workspace')

EXTRA_KEYS = [
    ('env_push_enabled', 'true'),
    ('env_push_enabled_users', '[]'),
    ('env_push_enabled_groups', '[]'),
    ('env_name_regex', "'^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'"),
]


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OVERRIDE
    src = io.open(path, encoding='utf-8').read()

    if '[cloud.storage.service]' not in src:
        print('缺少 [cloud.storage.service] 段，请先用 config_cloud_storage.sh 初始化：%s' % path)
        return 1

    # 1) env_path 归一化
    if re.search(r"^env_path\s*=", src, flags=re.M):
        src = re.sub(r"^env_path\s*=.*$", "env_path = '%s'" % ENV_PATH, src, count=1, flags=re.M)
    else:
        src = src.replace('[cloud.storage.service]',
                          "[cloud.storage.service]\nenv_path = '%s'" % ENV_PATH, 1)

    # 2) env_push_* 开关幂等补齐（缺失才追加到 env_path 之后）
    missing = [(k, v) for k, v in EXTRA_KEYS if not re.search(r"^%s\s*=" % k, src, flags=re.M)]
    if missing:
        block = '\n'.join("%s = %s" % (k, v) for k, v in missing)
        src = re.sub(r"^(env_path\s*=.*)$", r"\1\n" + block, src, count=1, flags=re.M)

    io.open(path, 'w', encoding='utf-8').write(src)
    print('override.toml 已更新: env_path=%s，新增 %d 个 env_push_* 键' % (ENV_PATH, len(missing)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
