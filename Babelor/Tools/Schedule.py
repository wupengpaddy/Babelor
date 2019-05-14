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
import datetime
import time
# Outer Required
# import msgpack
# Inner Required
# Global Parameters
from Babelor.Config import CONFIG


class TASKS:
    def __init__(self):
        self.tasks = []
        self.active = True

    def run_tasks(self):
        runnable_tasks = [task for task in self.tasks if task.should_run]
        if len(runnable_tasks) < 1:
            self.active = False
        for task in sorted(runnable_tasks):
            task.run()

    def remove(self, task):
        try:
            self.tasks.remove(task)
        except ValueError:
            pass

    def add(self, start_datetime: datetime.datetime, timedelta: datetime.timedelta,
            func: callable, expire_run_times: int = CONFIG.TASK_MAX_RUN_TIMES):
        task = TASK(start_datetime, timedelta, expire_run_times, self, func)
        self.tasks.append(task)

    def start(self):
        while self.active:
            self.run_tasks()
            time.sleep(CONFIG.TASK_BLOCK_TIME)
        else:
            self.active = True


class TASK:
    def __init__(self, start_datetime: datetime.datetime, timedelta: datetime.timedelta,
                 expire_run_times: int, tasks: TASKS, func: callable):
        self.task_func = func
        self.last_run_datetime = None                       # datetime of the last run
        self.next_run_datetime = None                       # datetime of the next run
        self.timedelta = timedelta                          # timedelta between runs
        self.start_datetime = start_datetime                # specific datetime to start on
        self.expire_datetime = start_datetime + (timedelta * expire_run_times)   # specific datetime to expired
        self.run_times = 0                                  # run times
        self.tasks = tasks                                  # scheduler to register with

    def run(self):
        if self.task_func is not None:
            self.task_func()
        self.last_run_datetime = datetime.datetime.now()
        self.schedule_next_run()

    def should_run(self):
        current_datetime = datetime.datetime.now()
        is_expired = current_datetime < self.expire_datetime
        is_run = current_datetime >= self.next_run_datetime
        return is_expired and is_run

    def schedule_next_run(self):
        self.next_run_datetime = self.start_datetime + (self.timedelta * (self.run_times + 1))
        if self.next_run_datetime < datetime.datetime.now():
            self.schedule_next_run()
