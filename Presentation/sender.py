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
from multiprocessing import Queue, Process
from threading import Thread
import time
# Outer Required
import pandas as pd
# Inner Required
from Message import URL, MSG, CASE
from CONFIG.config import GLOBAL_CFG
from Process import MessageQueue
from DataBase import SQL
# Global Parameters
MSG_Q_MAX_DEPTH = GLOBAL_CFG["MSG_Q_MAX_DEPTH"]
CODING = GLOBAL_CFG["CODING"]
BlockingTime = GLOBAL_CFG["MSG_Q_BlockingTime"]


def queue_consumer(mq: MessageQueue, queue_in: Queue, queue_ctrl: Queue):
    is_active = queue_ctrl.get()            # 控制信号（初始化）
    while is_active:
        if queue_ctrl.empty():              # 控制信号（无变更），敏捷响应
            while queue_in.full():
                time.sleep(BlockingTime)
            else:
                queue_in.put(mq.pull())
        else:
            is_active = queue_ctrl.get()


def queue_producer():


class SENDER:
    def __init__(self, conn: URL):
        # conn = "tcp://*:port"
        self.me = MessageQueue(conn)
        self.active = True                                      # 服务端激活
        self.__queue_origination_in = Queue(MSG_Q_MAX_DEPTH)    # 来源队列（进站）
        self.__queue_origination_out = Queue(MSG_Q_MAX_DEPTH)   # 来源队列（出站）
        self.__queue_destination_in = Queue(MSG_Q_MAX_DEPTH)    # 目标队列（进站）
        self.__queue_destination_out = Queue(MSG_Q_MAX_DEPTH)   # 目标队列（出站）
        self.__queue_treatment_out = Queue(MSG_Q_MAX_DEPTH)     # 处理队列（出站）
        self.__queue_treatment_in = Queue(MSG_Q_MAX_DEPTH)      # 处理队列（进站）
        self.__queue_mine_in = Queue(MSG_Q_MAX_DEPTH)           # 监听队列（进站）
        self.__queue_mine_out = Queue(MSG_Q_MAX_DEPTH)          # 监听队列（出站）
        self.__queue_ctrl = Queue(MSG_Q_MAX_DEPTH)              # 控制队列
        self.mine = Process(target=queue_consumer, args=(self.me, ))

    def __listen(self):
        while self.__dict__["active"]:
            while self.__dict__["__queue_listen"].empty():
                time.sleep(BlockingTime)
            else:
                msg = self.__dict__["__queue_listen"].get()
        else:
            pass



