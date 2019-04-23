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


def employer(mq: MessageQueue, queue_ctrl: Queue, employee: callable):
    """
    # 雇主
    :param mq: MessageQueue     # 消息队列
    :param queue_ctrl: Queue    # 控制 ("is_active",):(bool,)
    :param employee: callable   # 被雇者
    :return: None
    """
    is_active = queue_ctrl.get()            # 控制信号（初始化）
    while is_active:
        if queue_ctrl.empty():              # 控制信号（无变更），敏捷响应
            msg = mq.pull()
            employee(msg)
        else:
            is_active = queue_ctrl.get()
    else:
        queue_ctrl.close()
        mq.release()


class TOWER:
    def __init__(self, conn: URL):
        # conn = "tcp://*:port"
        self.me = conn
        self.mq = MessageQueue(conn)
        self.__queue_ctrl = Queue(MSG_Q_MAX_DEPTH)              # 控制队列
        self.__queue_ctrl.put((True, None))
        self.mine = None

    def start(self, func: callable):
        self.mine = Thread(target=employer, args=(self.mq, self.__queue_ctrl, func))
        self.mine.setDaemon(False)
        self.mine.start()

    def stop(self):
        self.__queue_ctrl.put(False)


def allocator(conn: URL):
    if conn.scheme in ["oracle", "mysql"]:
        return SQL(conn)
    if conn.scheme in ["tcp"]:
        return MessageQueue(conn)


def sender(msg: MSG):
    # employee
    origination = allocator(msg.origination)
    destination = allocator(msg.destination)
    if msg.treatment is None:
        treatment = None
    else:
        treatment = allocator(msg.treatment)
    if msg.encryption is None:
        encryption = None
    else:
        encryption = allocator(msg.encryption)
    # process
    msg_origination = origination.read(msg)
    if encryption is None:
        msg_encryption = msg_origination
    else:
        msg_encryption = encryption.request(msg_origination)
    if treatment is None:
        msg_treatment = msg_encryption
    else:
        msg_treatment = treatment.request(msg_encryption)
    destination.push(msg_treatment)


def treater(msg: MSG, func: callable):
    origination = allocator(msg.origination)
    if msg.destination is None:
    destination = allocator(msg.destination)
    pass
