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


def queue_listen(mq: MessageQueue, queue_ctrl: Queue, func: callable):
    """
    # 队列消费者（先出后进）
    # 控制信号（启动）--> 输出队列（推出）--> 控制信号（需反馈）--> 输入队列（推入）
    :param mq: MessageQueue     # 消息队列
    :param queue_ctrl: Queue    # 控制 ("is_active",):(bool,)
    :param func: callable       # 函数
    :return: None
    """
    is_active = queue_ctrl.get()            # 控制信号（初始化）
    while is_active:
        if queue_ctrl.empty():              # 控制信号（无变更），敏捷响应
            msg = mq.pull()
            func(msg)
        else:
            is_active = queue_ctrl.get()
    else:
        pass


class ROOM:
    def __init__(self, conn: URL):
        # conn = "tcp://*:port"
        self.me = MessageQueue(conn)
        self.__queue_ctrl = Queue(MSG_Q_MAX_DEPTH)              # 控制队列
        self.__queue_ctrl.put(True)
        self.mine = Thread(target=queue_listen, args=(self.me, self.__queue_ctrl, queue_listen))
        self.mine.setDaemon(False)
        self.mine.start()

    def close(self):
        self.__queue_ctrl.put(False)





