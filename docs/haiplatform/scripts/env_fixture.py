#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
haienv 测试 fixture —— 用例集 §2.5「替代 `haienv create`」的增强版。

为什么不用 `haienv create`：它硬校验 CUDA 11.1/11.3 与 conda（`command.py:41-43`、
`client/script.py` 的 ACTIVATE 会 `conda activate`），103 测试机上都没有。
本脚本用**真实 `haienv` 包**写本地注册表（保证 pickle 兼容，CMP-03），
并生成一个**功能等价**的 `activate`：把前缀的 site-packages 注入 PYTHONPATH，
使 `source haienv <name>` 之后能真正 import 到环境内独有的探针包（AC-03 的断言方式）。

用法（host 103，python3 已装 haienv/hai-cli）：
    python3 env_fixture.py --env-root /nfs-shared/.../hfai_envs --user haiadmin --name myenv --py 3.8
    python3 env_fixture.py ... --clean          # 清理该 env（目录 + 注册表记录）

输出：一行前缀路径（stdout），失败返回非 0。
'''

import argparse
import os
import shutil
import sys

PROBE_PKG = 'haienv_probe_unique'
PROBE_VALUE = 'env-push-ok'

ACTIVATE_TEMPLATE = '''#!/usr/bin/env bash
# haienv 测试 fixture 生成的 activate（功能等价于 conda prefix 的 activate 对本场景的作用）
# 真实环境由 `haienv create` + conda 生成；这里只需要保证 source 之后环境真正生效。
export HF_ENV_NAME="{name}"
export HF_ENV_OWNER="{user}"
export HAIENV_FIXTURE="1"
_HAIENV_FIXTURE_PREFIX="{prefix}"
export PYTHONPATH="{extra}{prefix}/lib/python{py}/site-packages:{prefix}/lib/python{py}:${{PYTHONPATH}}"
export PATH="{prefix}/bin:${{PATH}}"
echo "user haienv [{name}] loaded"
'''


def _try_chmod(path, mode):
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def _suffix_scan(user_env, name):
    used = set()
    if os.path.isdir(user_env):
        for entry in os.listdir(user_env):
            prefix = name + '_'
            if entry.startswith(prefix) and entry[len(prefix):].isdigit():
                used.add(int(entry[len(prefix):]))
    suffix = 0
    while suffix in used:
        suffix += 1
    return suffix


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env-root', required=True, help='env_root（{env_path}/hfai_envs）')
    parser.add_argument('--user', required=True)
    parser.add_argument('--name', required=True)
    parser.add_argument('--py', default='3.8')
    parser.add_argument('--suffix', type=int, default=None, help='不指定则取第一个空闲后缀')
    parser.add_argument('--extra-search-dir', action='append', default=[])
    parser.add_argument('--clean', action='store_true', help='删除该 env 的目录与注册表记录')
    args = parser.parse_args()

    from haienv.client.model import Haienv, HaienvConfig

    user_env = os.path.join(args.env_root, args.user)
    os.makedirs(user_env, exist_ok=True)
    _try_chmod(user_env, 0o777)
    db_path = os.path.join(user_env, 'venv.db')

    if args.clean:
        existing = Haienv.select(outside_db_path=db_path, haienv_name=args.name) \
            if os.path.exists(db_path) else None
        if existing is not None:
            shutil.rmtree(getattr(existing, 'path', ''), ignore_errors=True)
            Haienv.delete(haienv_name=args.name, outside_db_path=db_path)
        return 0

    suffix = args.suffix if args.suffix is not None else _suffix_scan(user_env, args.name)
    prefix = os.path.join(user_env, f'{args.name}_{suffix}')
    os.makedirs(os.path.join(prefix, 'lib', f'python{args.py}', 'site-packages'), exist_ok=True)

    probe_dir = os.path.join(prefix, 'lib', f'python{args.py}', 'site-packages', PROBE_PKG)
    os.makedirs(probe_dir, exist_ok=True)
    with open(os.path.join(probe_dir, '__init__.py'), 'w') as f:
        f.write("VALUE = '%s'\n" % PROBE_VALUE)

    extra = ''.join(f'{p}:' for p in args.extra_search_dir)
    with open(os.path.join(prefix, 'activate'), 'w') as f:
        f.write(ACTIVATE_TEMPLATE.format(name=args.name, user=args.user, prefix=prefix,
                                         py=args.py, extra=extra))
    with open(os.path.join(prefix, 'pip.conf'), 'w') as f:
        f.write('[global]\n')

    Haienv.insert(haienv_name=args.name,
                  haienv_config=HaienvConfig(path=prefix, extend='False', extend_env='',
                                             py=args.py,
                                             extra_search_dir=list(args.extra_search_dir)),
                  outside_db_path=db_path)
    # 共享盘可能是 NFS，非属主 chmod 会失败；权限只影响可写性，失败不致命
    _try_chmod(prefix, 0o777)
    for root, dirs, files in os.walk(prefix):
        for item in dirs:
            _try_chmod(os.path.join(root, item), 0o777)
        for item in files:
            _try_chmod(os.path.join(root, item), 0o666)
    print(prefix)
    return 0


if __name__ == '__main__':
    sys.exit(main())
