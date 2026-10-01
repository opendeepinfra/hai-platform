#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把 hai-platform 的 Dockerfile 打成本地可离线构建的版本。

背景：原始 Dockerfile 依赖 dl.k8s.io / github.com / releases 下载二进制，
而 103 上这些地址不可达（走代理），因此改为从 build-assets 目录
（docker buildx 的命名构建上下文 `assets`）复制。

用法：python3 patch_dockerfile.py <Dockerfile 路径>
"""

import io
import re
import sys

APT_RETRY = "-o Acquire::Retries=10 -o Acquire::http::Timeout=60"

# 每条规则：(唯一锚点正则, 替换文本)  —— 用正则是为了对空白不敏感
RULES = [
    # 1) apt 增加重试与超时
    (
        r"[ \t]*apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Asia/Shanghai apt-get -y install tzdata && \\\n",
        "  apt-get %s update && DEBIAN_FRONTEND=noninteractive TZ=Asia/Shanghai apt-get -y %s install tzdata && \\\n"
        % (APT_RETRY, APT_RETRY),
    ),
    (
        r"[ \t]*apt-get install -y python3\.8 python3-pip tzdata libcurl4-openssl-dev libssl-dev net-tools \\\n",
        "  apt-get install -y %s python3.8 python3-pip tzdata libcurl4-openssl-dev libssl-dev net-tools \\\n"
        % APT_RETRY,
    ),
    # 2) kubectl / decode-protobuf-camel 从 assets 复制
    (
        r"RUN curl -Lo /usr/local/bin/kubectl [^\n]*\n[ \t]*curl -Lo /usr/local/bin/decode-protobuf-camel [^\n]*\n",
        "RUN --mount=type=bind,from=assets,target=/tmp/assets \\\n"
        "  cp /tmp/assets/kubectl /usr/local/bin/kubectl && \\\n"
        "  cp /tmp/assets/decode-protobuf-camel /usr/local/bin/decode-protobuf-camel && \\\n",
    ),
    # 3) fountain / ambient 从 assets 复制
    (
        r"RUN mkdir -p /marsv2/scripts && cd /marsv2/scripts && \\\n"
        r"[ \t]*wget [^\n]*/ambient\.tar\.gz[^\n]*\n"
        r"[ \t]*wget [^\n]*/fountain\.tar\.gz[^\n]*\n",
        "RUN --mount=type=bind,from=assets,target=/tmp/assets \\\n"
        "  mkdir -p /marsv2/scripts && cd /marsv2/scripts && \\\n"
        "  cp /tmp/assets/ambient.tar.gz . && tar zxvf ambient.tar.gz && \\\n"
        "  cp /tmp/assets/fountain.tar.gz . &&  tar zxvf fountain.tar.gz\n",
    ),
    # 4) setuptools / setuptools_scm 钉死版本
    #    注意：103 的工作区 Dockerfile 可能已经打过这个补丁（本地未提交改动），
    #    这种情况直接跳过（幂等）。
    (
        r'RUN pip install "setuptools_scm[^\n]*\n',
        'RUN pip install "setuptools==62.6.0" "setuptools_scm==6.4.2" '
        '--index-url=https://pypi.tuna.tsinghua.edu.cn/simple '
        '--trusted-host=pypi.tuna.tsinghua.edu.cn && \\\n',
        r'setuptools==62\.6\.0',
    ),
    # 5) hai-studio 从 assets 复制（拆成两步，避免多行空白差异）
    (
        r"RUN pip install jupyterlab_hai_platform_ext && \\\n",
        "RUN --mount=type=bind,from=assets,target=/tmp/assets \\\n"
        "  pip install jupyterlab_hai_platform_ext && \\\n",
    ),
    (
        r"[ \t]*wget [^\n]*hai-studio-linux-x64[^\n]*\.tar\.gz && tar xzvf (hai-studio-linux-x64[^\n]*\.tar\.gz)\n",
        "  cp /tmp/assets/\\1 . && tar xzvf \\1\n",
    ),
]


def main():
    if len(sys.argv) < 2:
        print("usage: patch_dockerfile.py <Dockerfile>")
        return 2
    path = sys.argv[1]
    src = io.open(path, encoding="utf-8").read()

    failed = []
    for idx, rule in enumerate(RULES, 1):
        pattern, repl = rule[0], rule[1]
        skip_if = rule[2] if len(rule) > 2 else None
        if skip_if and re.search(skip_if, src):
            print("RULE %d already applied, skip" % idx)
            continue
        new_src, count = re.subn(pattern, repl, src, count=1)
        if count != 1:
            failed.append(idx)
            print("RULE %d FAILED, pattern=%r" % (idx, pattern[:80]))
        else:
            src = new_src

    if failed:
        print("PATCH_FAILED rules=%s" % failed)
        return 1

    io.open(path, "w", encoding="utf-8").write(src)
    print("PATCHED_OK rules=%d" % len(RULES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
