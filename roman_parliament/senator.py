import conf
from conf import PARLIAMENT
import os
from .utils import is_senator, send_data, generate_key, remove_socket
from db import redis_conn
import pickle
from datetime import datetime
import time
from conf.flags import PARLIAMENT_MEMBERS, PARLIAMENT_SOURCE_TYPE
import random

current_member = None
senator_dict = {}


class Member:
    def __init__(self, service_name, port, tp):
        self.service_name = service_name
        self.port = port
        self.type = tp

    def get_sons(self):
        raise NotImplementedError


class Mass(Member):
    def __init__(self, service_name, port, key=''):
        super().__init__(service_name=service_name, port=port, tp=PARLIAMENT_MEMBERS.MASS)
        self.key = key
        self.namespace = conf.POLYAXON_SETTING.k8s.namespace

    def get_sons(self):
        return []


class Senator(Member):
    def __init__(self, senator_id):
        """
        :param senator_id: 传入议员编号
        """
        self.mass_map = set()
        self.senator_id = senator_id
        self.namespace = os.environ.get('PARLIAMENT_SENATOR_NS', os.environ['NAMESPACE'])
        for senator in PARLIAMENT.senator_list:  # 获取对应service name以及监听的port
            if PARLIAMENT.senators[senator].start_index <= senator_id < PARLIAMENT.senators[senator].start_index + PARLIAMENT.senators[senator].processes:
                super().__init__(service_name=senator, port=PARLIAMENT.senators[senator].port[senator_id - PARLIAMENT.senators[senator].start_index], tp=PARLIAMENT_MEMBERS.SENATOR)
                break

    def get_sons(self):
        """
        找到所以儿子议员，以及符合条件的群众
        """
        return [get_senator(i) for i in range(3 * self.senator_id + 1, min(3 * self.senator_id + 4, PARLIAMENT.total_processes))] + self.get_mass_list()

    def add_mass(self, service_name, port, key, *args, **kwargs):
        self.mass_map.add((service_name, port, key))

    def remove_mass(self, service_name, port, key, *args, **kwargs):
        try:
            self.mass_map.remove((service_name, port, key))
        except:
            pass
        remove_socket(Mass(service_name, port, key))

    def get_mass_list(self):
        return [Mass(service_name=mass[0], port=mass[1], key=mass[2]) for mass in self.mass_map]


def get_senator(senator_id):  # 搞成单例
    if senator_id not in senator_dict:
        senator_dict[senator_id] = Senator(senator_id)
    return senator_dict[senator_id]


def get_current_member():
    global current_member
    if current_member is None:
        if is_senator():
            senator_id = PARLIAMENT.senators[os.environ['HOSTNAME']].start_index + int(os.environ.get('PARLIAMENT_PROCESS_ID', 0))
            current_member = get_senator(senator_id)
        else:
            current_member = Mass(os.environ['HOSTNAME'], int(os.environ['MASS_PORT']))
    return current_member


def register_mass(cls, sign, value, connected_senator_id=None, service_name=os.environ['HOSTNAME'], block=False):
    """
    把群众注册进会议中
    :param cls: 类
    :param sign: 唯一标识符
    :param value: 唯一标识符的值
    :param connected_senator_id: 连接的议员编号
    :param service_name:
    :param block: 是否等到成功建立连接才退出
    :return:
    """
    if connected_senator_id is None:
        connected_senator_id = random.randint(0, PARLIAMENT.total_processes - 1)
    key = generate_key(f'registered_{cls.__name__}', sign, value)
    print(datetime.now(), f'开始注册群众，连接的议员编号为{connected_senator_id}')
    assert 'MASS_PORT' in os.environ, '必须指定环境变量MASS_PORT用于监听'
    port = int(os.environ['MASS_PORT'])
    print(datetime.now(), '发送数据')
    data = {
        'connected_senator_id': connected_senator_id,
        'service_name': service_name,
        'port': port,
        'key': key
    }
    redis_conn.sadd(PARLIAMENT.mass_set, pickle.dumps(data))  # 在redis中记录这一信息
    success_list = send_data({'source': PARLIAMENT_SOURCE_TYPE.REGISTER_MASS, 'data': data}, [get_senator(connected_senator_id)])
    print(datetime.now(), '等待注册成功')
    while block and not success_list[0]:  # 发送失败
        time.sleep(10)
        success_list = send_data({'source': PARLIAMENT_SOURCE_TYPE.REGISTER_MASS, 'data': data}, [get_senator(connected_senator_id)])
    print(datetime.now(), '注册成功')


def cancel_mass(connected_senator_id, key, service_name, port):
    """
    把群众从会议中撤出
    :param connected_senator_id:
    :param key:
    :param service_name:
    :param port:
    :return: 
    """
    data = {
        'connected_senator_id': connected_senator_id,
        'service_name': service_name,
        'port': int(port),
        'key': key
    }
    try:
        redis_conn.srem(PARLIAMENT.mass_set, pickle.dumps(data))
    except:  # 兜个底
        pass
    send_data({'source': PARLIAMENT_SOURCE_TYPE.CANCEL_MASS, 'data': data}, [get_senator(connected_senator_id)])
