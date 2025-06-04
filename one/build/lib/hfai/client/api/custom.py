import os
import sys
from datetime import datetime, timedelta
from .api_config import get_mars_token as mars_token
from .api_config import get_mars_url as mars_url
from .training_api import node_name, task_id, rank
import requests
from traceback import format_exception
from .experiment_api import create_experiment


try:
    import pynvml
    no_pynvml = False
except:
    no_pynvml = True
IS_SIMULATE = os.environ.get('HFAI_SIMULATE', '0') == '1'


def _check_ib():
    ib_state_path = '/sys/class/infiniband/mlx5_0/ports/1/state'
    if os.path.exists(ib_state_path):
        with open(ib_state_path) as isf:
            status = isf.read()
            if len(status) > 0 and status[0] == '4':
                return True
            print(f'ib check failed [{status}]')
    return False


def _check_ecc():
    # cpu 机器直接返回
    if no_pynvml or (not node_name().endswith('dl')):
        return True
    pynvml.nvmlInit()

    def ecc_count(_handle):
        return pynvml.nvmlDeviceGetTotalEccErrors(
            _handle,
            pynvml.NVML_MEMORY_ERROR_TYPE_UNCORRECTED,
            pynvml.NVML_AGGREGATE_ECC)

    device_count = pynvml.nvmlDeviceGetCount()
    if 'CUDA_VISIBLE_DEVICES' in os.environ:  # 这样会很快
        if os.environ['CUDA_VISIBLE_DEVICES']: # 如果没有设定
            for i in os.environ['CUDA_VISIBLE_DEVICES'].split(','):
                if int(i) < device_count:
                    handle = pynvml.nvmlDeviceGetHandleByIndex(int(i))
                    if ecc_count(handle) != 0:
                        return False
            return True
        return True
    else:
        for i in range(device_count):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            if ecc_count(handle) != 0:
                return False
        return True


def _parse_kern_time(line):
    try:
        return datetime.strptime(f"{datetime.now().year} {' '.join(line.split()[:3])}", "%Y %b %d %H:%M:%S")
    except:  # 如果解析不了，默认时间为一天之前，也就是忽略本条
        return datetime.now() - timedelta(days=1)


def _magic_filter(line):
    if 'Xid' not in line:
        return False
    for magic_n in ['63', '64', '45', '43', '31']:
        if f': {magic_n}' in line:
            return False
    return True


def _check_kern():
    with open('/var/log/kern/kern.log', 'rb') as f:
        total_len = f.seek(0, 2)
        offset = 50  # 本次看的长度
        while True:
            if total_len <= offset:  # 看的长度不应该超过总长度
                offset = total_len
            f.seek(-offset, 2)
            rst = f.read().decode().strip()
            if rst.count('\n') > 1:  # 存在完整的一行
                first_idx = rst.find('\n')
                second_idx = rst.find('\n', first_idx + 1)
                if (datetime.now() - _parse_kern_time(rst[first_idx + 1: second_idx])).total_seconds() > 5 * 60:  # 看的内容已经超过5分钟了
                    break
            if total_len == offset:  # 看完了
                break
            offset *= 2  # 下次看的长度翻倍

        for line in reversed(rst.split('\n')):  # 所有行都拎出来看下
            if 'NVRM' in line and _magic_filter(line) and (datetime.now() - _parse_kern_time(line)).total_seconds() < 5 * 60:
                print('该机器的kern log报了NVRM错误，尝试重启，kern 日志如下：')
                print(line)
                return {
                    'passed': False,
                    'msg': 'kern log存在NVRM; '
                }
            if 'check_quota_exceeded' in line and (datetime.now() - _parse_kern_time(line)).total_seconds() < 5 * 60:
                print('该机器的kern log报了ceph quota exceeded，尝试重启，kern 日志如下：')
                print(line)
                return {
                    'passed': False,
                    'msg': 'ceph quota exceeded; '
                }
    return {
        'passed': True,
        'msg': ''
    }


def self_health_check(pid=os.getpid()):
    """
    对当前机器做系统检查，检查通过会退出该任务，检查失败会重启该任务
    :param pid:
    :return:
    """
    if os.environ.get('MARSV2_TASK_TYPE', '') != 'training':
        return
    hw_check_pass = 1
    err_msg = ''
    if not _check_ib():
        hw_check_pass = 0
        err_msg += 'ib 报错; '
    try:
        if not _check_ecc():
            hw_check_pass = 0
            err_msg += 'ecc报错; '
    except Exception as e:
        hw_check_pass = 0
        err_msg += f'ecc检查出错，请管理员检查: {e}; '
    try:
        rst = _check_kern()
        if not rst['passed']:
            hw_check_pass = 0
            err_msg += rst['msg']
    except:
        pass
    res = requests.post(
        f'{mars_url()}/operating/fail_task?token={mars_token()}&id={task_id()}&rank={rank()}&hw_check_pass={hw_check_pass}&err_msg={err_msg}')
    err_msg += '重启' if err_msg else '关闭'

    print(f"[{pid}] 任务 {err_msg} 请求{'成功' if res.status_code == 200 and res.json()['success'] == 1 else '失败'}")


def bind_hf_except_hook(f):
    """
    该函数用于将 Process 类绑定异常 hook，在子进程发生异常时通知 server 将其强行关闭，并启动自我检查，发现硬件故障重启该任务

    Args:
        f (class): 传进来的 Process 类

    Examples:

        >>> from hfai.client import bind_hf_except_hook
        >>> from torch.multiprocessing import Process
        >>> bind_hf_except_hook(Process)

    """

    def my_except_hook(e: Exception, ttype, tvalue, ttraceback):
        pid = os.getpid()
        if IS_SIMULATE:
            print(f'[{pid}] 模拟调用到了 hfai 的异常绑定')
            raise
        if os.environ.get('MARSV2_TASK_TYPE', '') != 'training':
            raise

        sys.stderr.write(''.join(format_exception(ttype, tvalue, ttraceback)))
        self_health_check(pid)


    def try_except(f):
        def handle_problems(*args, **kwargs):
            try:
                f(*args, **kwargs)
            except Exception as e:
                my_except_hook(e, *sys.exc_info())

        return handle_problems

    setattr(f, 'run', try_except(getattr(f, 'run')))


create_experiment_v2 = create_experiment

__all__ = ['self_health_check', 'bind_hf_except_hook', 'create_experiment_v2']
