
try:
    from .server_flags import *
except Exception:
    pass
import collections
import os

class USER_ROLE():
    INTERNAL = 'internal'
    EXTERNAL = 'external'

class EXP_STATUS():
    QUEUED = 'queued'
    CREATED = 'created'
    BUILDING = 'building'
    RUNNING = 'running'
    UNSCHEDULABLE = 'unschedulable'
    TERMINATING = 'terminating'
    SUCCEEDED_TERMINATING = 'succeeded_terminating'
    FAILED_TERMINATING = 'failed_terminating'
    STOPPED_TERMINATING = 'stopped_terminating'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    STOPPED = 'stopped'
    MIXED = 'mixed'
    UNFINISHED = [CREATED, RUNNING, BUILDING, UNSCHEDULABLE, SUCCEEDED_TERMINATING, FAILED_TERMINATING, STOPPED_TERMINATING]
    ENDING = [STOPPED, FAILED, SUCCEEDED, SUCCEEDED_TERMINATING, FAILED_TERMINATING, STOPPED_TERMINATING]
    FINISHED = [STOPPED, FAILED, SUCCEEDED]
    UPGRADE_FINISHED = [FAILED, SUCCEEDED]

class QUE_STATUS():
    QUEUED = 'queued'
    SCHEDULED = 'scheduled'
    FINISHED = 'finished'
STATUS_COLOR_MAP = {EXP_STATUS.QUEUED: 'blue', EXP_STATUS.RUNNING: 'yellow', EXP_STATUS.STOPPED: 'gray', EXP_STATUS.SUCCEEDED: 'green', EXP_STATUS.FAILED: 'red', QUE_STATUS.QUEUED: 'blue', QUE_STATUS.SCHEDULED: 'yellow', QUE_STATUS.FINISHED: 'red'}

class STOP_CODE():
    NO_STOP = 0
    STOP = 1
    INTERRUPT = 16
    UNSCHEDULABLE = 32
    FAILED = 128
    INIT_FAILED = 256
    TIMEOUT = 512
    MANUAL_STOP = 1024
    HOOK_RESTART = 2048
    MANUAL_SUCCEEDED = 4096
    MANUAL_FAILED = 8192

    def __init__(self):
        self.action_map = collections.OrderedDict()
        for k in self.__class__.__dict__:
            if isinstance(self.__class__.__dict__[k], int):
                self.action_map[self.__class__.__dict__[k]] = k

    def name(self, action):
        for k in reversed(self.action_map.keys()):
            if (action >= k):
                return self.action_map[k]
        return 'NAN'

class SUSPEND_CODE():
    NO_SUSPEND = 0
    SUSPEND_SENT = 1
    SUSPEND_RECEIVED = 2
    CAN_SUSPEND = 3

class TASK_TYPE():
    UPGRADE_TASK = 'upgrade'
    TRAINING_TASK = 'training'
    JUPYTER_TASK = 'jupyter'
    VIRTUAL_TASK = 'virtual'
    VALIDATION_TASK = 'validation'
    BACKGROUND_TASK = 'background'

    @classmethod
    def all_task_types(cls):
        return [getattr(TASK_TYPE, t) for t in cls.__dict__.keys() if t.isupper()]

class CHAIN_STATUS():
    WAITING_INIT = 'waiting_init'
    RUNNING = 'running'
    SUSPENDED = 'suspended'
    FINISHED = 'finished'

def chain_status_to_queue_status(cs):
    if (cs in [CHAIN_STATUS.WAITING_INIT, CHAIN_STATUS.SUSPENDED]):
        return QUE_STATUS.QUEUED
    if (cs == CHAIN_STATUS.RUNNING):
        return QUE_STATUS.SCHEDULED
    if (cs == CHAIN_STATUS.FINISHED):
        return QUE_STATUS.FINISHED

class TASK_FLAG():
    SUSPEND_CODE = 3
    STAR = 4

class EXP_PRIORITY():
    EXTREME_HIGH = 50
    VERY_HIGH = 40
    HIGH = 30
    ABOVE_NORMAL = 20
    AUTO = (- 1)

    @classmethod
    def get_name_by_value(cls, value):
        for (k, v) in cls.__dict__.items():
            if (v == value):
                return k
        return 'AUTO'

class WARN_TYPE():
    COMPLETED = 1
    TERMINATING = 2
    LOG = 4
    INTERRUPT = 8

class PARLIAMENT_MEMBERS():
    SENATOR = 'senator'
    MASS = 'mass'

class PARLIAMENT_SOURCE_TYPE():
    CREATE_ARCHIVE = 'create_archive'
    CANCEL_MASS = 'cancel_mass'
    REGISTER_MASS = 'register_mass'
    UPDATE = 'update'
    CONNECT = 'connect'
    CANCEL_ARCHIVE = 'cancel_archive'
