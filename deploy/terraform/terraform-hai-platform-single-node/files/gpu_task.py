#!/usr/bin/env python
"""GPU 探针任务：验证平台任务 Pod 真的能用上 V100。

Hai Platform 的分卡方式不是 k8s 的 nvidia.com/gpu 资源申请，而是：
  * 调度器按 DB 里 host.gpu_num 分配 assigned_gpus；
  * init_manager.py 把结果注入环境变量 NVIDIA_VISIBLE_DEVICES；
  * 容器运行时（本环境已把 containerd 默认运行时设为 nvidia）据此挂载 /dev/nvidia*。
所以本脚本同时检查：环境变量、/dev 设备节点、nvidia-smi 输出。

结论行（terraform 用 grep 判定）：
  GPU_RESULT <nvidia-smi 第一行>
  TASK_RUNNER:EXIT_OK / TASK_RUNNER:EXIT_ERR
"""
import glob
import os
import subprocess
import sys

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gpu_output.txt")
lines = []


def emit(s):
    print(s, flush=True)
    lines.append(s)


emit("GPU_INFO NVIDIA_VISIBLE_DEVICES=%r CUDA_VISIBLE_DEVICES=%r"
     % (os.environ.get("NVIDIA_VISIBLE_DEVICES"), os.environ.get("CUDA_VISIBLE_DEVICES")))
emit("GPU_ENV MARSV2_RANK=%r NODE_NAME=%r" % (os.environ.get("MARSV2_RANK"), os.environ.get("MARSV2_NODE_NAME")))

devs = sorted(glob.glob("/dev/nvidia*"))
emit("GPU_DEVS %s" % (",".join(devs) if devs else "<none>"))

smi = ""
try:
    smi = subprocess.check_output(["nvidia-smi", "-L"], text=True, stderr=subprocess.STDOUT).strip()
except Exception as exc:  # noqa: BLE001
    smi = "ERROR: %s" % exc
emit("GPU_SMI %s" % smi.replace("\n", " | "))

try:
    q = subprocess.check_output(
        ["nvidia-smi", "--query-gpu=name,memory.total,driver_version", "--format=csv,noheader"],
        text=True, stderr=subprocess.STDOUT).strip()
except Exception as exc:  # noqa: BLE001
    q = "ERROR: %s" % exc
emit("GPU_QUERY %s" % q.replace("\n", " | "))

ok = ("Tesla V100" in smi) or ("GPU 0:" in smi)
if not ok:
    emit("GPU_HINT 若 nvidia-smi 不可用，检查 containerd 默认运行时是否为 nvidia、任务 Pod 是否拿到 NVIDIA_VISIBLE_DEVICES")
    emit("GPU_RESULT <none>")
    emit("TASK_RUNNER:EXIT_ERR")
else:
    emit("GPU_RESULT %s" % smi.splitlines()[0])
    emit("TASK_RUNNER:EXIT_OK")

emit("GPU_DONE")

try:
    with open(OUT, "w") as f:
        f.write("\n".join(lines) + "\n")
except Exception as exc:  # noqa: BLE001
    print("GPU_INFO could not write %s: %s" % (OUT, exc), flush=True)

sys.exit(0)
