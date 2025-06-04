

# 这个版本先合并 validation, training, jupyter 的 matcher


import time
import pandas as pd
from scheduler.base_model import Matcher
from scheduler.utils import apply_process
from .match_training_task import get_match_task_func as get_match_training_task_func
from .match_jupyter_task import match_task as match_jupyter_task
from .match_validation_task import match_task as match_validation_task


class TrainingMatcher(Matcher):

    def __init__(self, **kwargs):
        self.match_training_task = get_match_training_task_func()
        self.match_jupyter_task = match_jupyter_task
        self.match_validation_task = match_validation_task
        # 60 次 match 结束超时的次数
        self.delay_last_60_ticks = []
        super(TrainingMatcher, self).__init__(**kwargs)

    def process_match(self):
        # 这里等一下 training 的下次数据，重复数据 match 无意义
        training_result = self.waiting_for_upstream_data('training')
        self.set_tick_data(training_result)
        self.resource_df = self.resource_df[self.resource_df.active]
        self.update_metric('assigner_delay', time.time() * 1000 - self.seq)
        self.perf_counter()
        # 之后 validation 可以拆出去
        validation_result = self.get_upstream_data('validation')
        task_df = pd.DataFrame(columns=self.task_df.columns)
        r, t = self.match_validation_task(self.resource_df.copy(), validation_result.task_df)
        # 每次结束合并 resource_df，以 match 结束的为准
        self.resource_df = pd.concat([self.resource_df, r])
        self.resource_df.drop_duplicates(subset=['NAME'], keep='last', inplace=True)
        task_df = pd.concat([task_df, t])
        ques = []
        for group, grouped_task_df in training_result.task_df.groupby('group'):
            ques.append(apply_process(
                lambda: self.match_training_task(
                    self.resource_df[self.resource_df.group == group],
                    grouped_task_df,
                    self.valid
                )
            ))
        res = [q.get() for q in ques]
        r = pd.concat([r[0] for r in res]) if len(res) > 0 else pd.DataFrame(columns=self.task_df.columns)
        t = pd.concat([r[1] for r in res]) if len(res) > 0 else pd.DataFrame(columns=self.resource_df.columns)
        for q in ques:
            q.close()
        self.resource_df = pd.concat([self.resource_df, r])
        self.resource_df.drop_duplicates(subset=['NAME'], keep='last', inplace=True)
        task_df = pd.concat([task_df, t])
        # 全部 match 结束把 task_df 去重，理论上没有重复，出现重复以第一个 matcher 为准
        task_df.drop_duplicates(subset=['id'], keep='first', inplace=True)
        self.task_df = task_df
        self.update_metric('match', self.perf_counter())

    def apply_db(self, transaction):
        self.perf_counter()
        super(TrainingMatcher, self).apply_db(transaction)
        # self.update_metric('db', self.perf_counter())
        self.update_metric('db_max', self.perf_counter(kind='max', keep=60))

    def user_tick_process(self):
        super(TrainingMatcher, self).user_tick_process()
        # 超时了
        if time.time() * 1000 - self.seq > 1000:
            self.delay_last_60_ticks.append(1)
        else:
            self.delay_last_60_ticks.append(0)
        self.delay_last_60_ticks = self.delay_last_60_ticks[-60:]
        self.update_metric('delay_count_60_ticks', sum(self.delay_last_60_ticks))
        self.update_metric('matcher_delay', time.time() * 1000 - self.seq)
