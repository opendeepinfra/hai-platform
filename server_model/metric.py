from db import db_engine
import pandas as pd
import datetime
from dateutil import tz
from dateutil.parser import parse


class Metric:

    def __init__(self, task_id, attempt_num, success_num, GPU=None, CPU=None, MEM=None, IB_recv=None, IB_send=None,
                 created_at=datetime.datetime.now().isoformat()):
        self.task_id = task_id
        self.gpu = GPU
        self.cpu = CPU
        self.mem = MEM
        self.ib_recv = IB_recv
        self.ib_send = IB_send
        self.created_at = created_at
        self.attempt_num = attempt_num
        self.success_num = success_num


    def insert(self):
        """
        初始化插入数据
        @return:
        """
        try:
            created_at = parse(self.created_at).astimezone(tz.tzlocal()).strftime('%Y-%m-%d %H:%M:%S')
        except:
            created_at = parse(str(self.created_at)).astimezone(tz.tzlocal()).strftime('%Y-%m-%d %H:%M:%S')
        sql = f'''
            insert into "task_metrics" ("task_id", "gpu_rate", "cpu_rate", "mem_rate", "ib_recv_usage", "ib_send_usage", "created_at", "attempt_num", "success_num") 
            VALUES ({self.task_id},  
                    {self.gpu if not pd.isna(self.gpu) else 'null'}, 
                    {self.cpu if not pd.isna(self.cpu) else 'null'},  
                    {self.mem if not pd.isna(self.mem) else 'null'},  
                    {self.ib_recv  if not pd.isna(self.ib_recv) else 'null'},
                    {self.ib_send  if not pd.isna(self.ib_send) else 'null'},
                    "{created_at}", {self.attempt_num}, {self.success_num})
            on conflict do nothing;
        '''
        db_engine.execute(sql, ())

        return self
