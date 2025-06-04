import asyncio
import hashlib
import os
import subprocess
import sys
import time
from functools import partial, wraps
from importlib.abc import MetaPathFinder, FileLoader
from importlib.util import spec_from_file_location, spec_from_loader
from subprocess import STDOUT, check_output


# note 发现多进程调用这个，会产生一堆的 Z+ 子进程，没时间去研究这个(有可能是超时后没有取消任务。下面已尝试修复)
async def run_cmd_aio(cmd, timeout=3):
    from logm import logger
    read_task = None
    try:
        create = asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE,
                                                 stderr=asyncio.subprocess.PIPE)
        proc = await create
        read_task = asyncio.Task(proc.communicate())
        stdout, stderr = await asyncio.wait_for(read_task, timeout)  # timeout=3s
        if proc.returncode != 0:
            logger.error(' '.join([cmd, '->', 'Run Failed']))
            raise subprocess.CalledProcessError(proc.returncode, cmd, stdout,
                                                stderr)
    except asyncio.TimeoutError:
        logger.error(' '.join([cmd, '->', 'Run Timeout']))
        if read_task is not None:
            read_task.cancel()
        raise
    return stdout, stderr


def run_cmd_new(cmd, timeout=3):
    from logm import logger
    start_time = time.time()
    try:
        output = check_output(cmd, stderr=STDOUT, timeout=timeout, shell=True)
    except subprocess.CalledProcessError as e:
        time_elapsed = time.time() - start_time
        log_str = f'time_elapsed: {time_elapsed}, {cmd} -> Run Failed with exit code {e.returncode}, output: {e.output.decode()}'
        logger.error(log_str)
        raise Exception(log_str)
    except subprocess.TimeoutExpired:
        logger.error(' '.join([cmd, '->', 'Run Timeout']))
        raise
    # print(cmd, '->', 'Run Success')
    return output


def convert_task_job_to_key(task, rank: int):
    return f'{hashlib.sha256(f"{task.user_name}{task.nb_name}".encode("utf-8")).hexdigest()[0:50]}-{rank}'


def convert_to_external_node(node, prefix, rank):
    return f'hfai-{prefix}-{rank}'


def convert_to_external_task(task):
    task.assigned_nodes = [convert_to_external_node(n, 'rank', rank) for rank, n in enumerate(task.assigned_nodes)]
    for rank, pod in enumerate(task._pods_):
        pod.node = convert_to_external_node(pod.node, 'rank', rank)
    return task


def asyncwrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_running_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


class EmptyFileLoader(FileLoader):

    def get_source(self, fullname: str):
        return ''


class CustomFinder(MetaPathFinder):

    def __init__(self, custom_file_name: str):
        super().__init__()
        self.custom_file_name = custom_file_name

    def find_spec(self, fullname, path, target=None):
        if path is None or path == "":
            path = [os.getcwd()]
        *parents, name = fullname.split('.')
        for entry in path:
            if not os.path.isdir(entry):
                continue
            if os.path.isdir(os.path.join(entry, name)):
                filename = os.path.join(entry, name, "__init__.py")
            else:
                filename = os.path.join(entry, name + ".py")
            # 对于平台代码目录的 import，找对应的 custom_file，找不到就提供一个空文件
            if \
                    entry.startswith(os.environ.get('SERVER_CODE_DIR', '/high-flyer/code/multi_gpu_runner_server')) and \
                    len(set(os.listdir(entry)) & {'implement.py', 'default.py'}) == 2 and \
                    name == 'custom':
                filename = os.path.join(entry, self.custom_file_name + ".py")
                if os.path.exists(filename):
                    return spec_from_file_location(fullname, filename)
                return spec_from_loader(name, EmptyFileLoader(fullname=fullname, path=entry))
            if not os.path.exists(filename):
                continue
            return spec_from_file_location(fullname, filename)
        return None


def setup_custom_finder():
    """
    提供了一个方便地测试 custom.py 的方法
    指定 CUSTOM_FILE_NAME 就可以将 implement.py / default.py 结构中的 custom.py 改为去找指定的文件
    不需要指定到 .py 结尾，只用不带后缀的文件名就行
    如果指定了不走 custom（例如设置 CUSTOM_FILE_NAME=xxx），如果不存在 xxx.py，会 fallback 到 default（提供一个空文件来 import）
    """
    default_custom_file_name = 'custom'
    custom_file_name = os.environ.get('CUSTOM_FILE_NAME', default_custom_file_name)
    if custom_file_name != default_custom_file_name or custom_file_name == 'custom':
        sys.meta_path = [m for m in sys.meta_path if not isinstance(m, CustomFinder)]
        sys.meta_path.insert(0, CustomFinder(custom_file_name=custom_file_name))

