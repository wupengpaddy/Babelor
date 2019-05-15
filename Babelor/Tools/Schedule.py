# coding=utf-8
# Copyright 2019 StrTrek Team Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# System Required
from datetime import datetime, timedelta
import time
# Outer Required
# import msgpack
# Inner Required
# Global Parameters
from Babelor.Config import CONFIG


class TASKS:
    def __init__(self, interval: int = CONFIG.TASK_BLOCK_TIME):
        self.tasks = []
        self.interval = interval
        self.active = True

    def run_tasks(self):
        runnable_tasks = [task for task in self.tasks if task.should_run()]
        for task in sorted(runnable_tasks):
            task.run()
        # --------------------
        next_run_tasks_cnt = 0
        for task in self.tasks:
            if task.next_should_run():
                next_run_tasks_cnt += 1
        if next_run_tasks_cnt == 0:
            self.active = False

    def add(self, start: datetime, delta: timedelta, func: callable,
            expired: int = CONFIG.TASK_MAX_RUN_TIMES, **kwargs):
        task = TASK(start, delta, expired, func, **kwargs)
        self.tasks.append(task)

    def start(self):
        while self.active:
            self.run_tasks()
            time.sleep(self.interval)
        else:
            self.active = True


class TASK:
    def __init__(self, start: datetime, delta: timedelta, expired: int, func: callable, **kwargs):
        self.task_func = func
        self.last_run_datetime = datetime.now()        # datetime of the last run
        self.next_run_datetime = start + delta                  # datetime of the next run
        self.timedelta = delta                                  # timedelta between runs
        self.start_datetime = start                             # specific datetime to start on
        self.expire_datetime = start + (delta * (expired + 1))  # specific datetime to expired
        self.run_times = 0                                      # run times
        self.kwargs = kwargs

    def run(self):
        if self.task_func is not None:
            self.task_func(**self.kwargs)
        self.last_run_datetime = datetime.now()
        self.schedule_next_run()

    def next_should_run(self):
        return self.next_run_datetime < self.expire_datetime

    def should_run(self):
        current_datetime = datetime.now()
        is_not_expired = current_datetime < self.expire_datetime
        is_not_run = current_datetime >= self.next_run_datetime
        return is_not_expired and is_not_run

    def schedule_next_run(self):
        self.run_times += 1
        self.next_run_datetime = self.start_datetime + (self.timedelta * self.run_times)
        if self.next_run_datetime < datetime.now():
            self.schedule_next_run()
