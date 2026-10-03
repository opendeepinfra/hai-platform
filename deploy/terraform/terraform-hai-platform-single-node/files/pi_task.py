#!/usr/bin/env python
"""Monte-Carlo 求 π 的任务脚本（冒烟用）。

注意：worker 镜像里有 numpy **但没有 torch**，所以这里必须只用 numpy，
否则任务会 ModuleNotFoundError 直接失败。

输出约定（terraform 用 grep 判定）：
  PI_RESULT <float>            —— π 估计值
  TASK_RUNNER:EXIT_OK / EXIT_ERR —— 误差是否在容差内
并在脚本同目录落一份 pi_output.txt（任务 Pod 结束即删，日志也可能被截断，文件更可靠）。
"""
import math
import os
import sys

import numpy as np

TOTAL = int(os.environ.get("PI_TOTAL_SAMPLES", "500000000"))
CHUNK = int(os.environ.get("PI_CHUNK", "5000000"))
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pi_output.txt")

rank = int(os.environ.get("RANK", "0"))
lines = []


def emit(s):
    print(s, flush=True)
    lines.append(s)


emit("PI_INFO rank=%d python=%s numpy=%s total=%d chunk=%d" % (
    rank, ".".join(map(str, sys.version_info[:3])), np.__version__, TOTAL, CHUNK))

inside = 0
done = 0
while done < TOTAL:
    n = min(CHUNK, TOTAL - done)
    x = np.random.random(n)
    y = np.random.random(n)
    inside += int(np.count_nonzero(x * x + y * y <= 1.0))
    done += n

pi = 4.0 * inside / TOTAL
err = abs(pi - math.pi)

if rank == 0:
    emit("PI_SAMPLES %d" % TOTAL)
    emit("PI_RESULT %.10f" % pi)
    emit("PI_ERROR %.10f" % err)
    emit("TASK_RUNNER:EXIT_OK" if err < 0.0005 else "TASK_RUNNER:EXIT_ERR")
emit("PI_DONE")

if rank == 0:
    try:
        with open(OUT, "w") as f:
            f.write("\n".join(lines) + "\n")
    except Exception as e:  # noqa: BLE001
        print("PI_INFO could not write %s: %s" % (OUT, e), flush=True)
