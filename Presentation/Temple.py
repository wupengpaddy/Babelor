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
from queue import Queue
from threading import Thread
# Outer Required
# Inner Required
from Message import URL, MSG
from Config.CONFIG import GLOBAL_CFG
from Process import MessageQueue
from Session import TOMAIL, FTP, FTPD
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
        queue_ctrl.join()
        mq.release()


class TEMPLE:
    def __init__(self, conn: URL):
        # conn = "tcp://*:port"
        self.me = conn
        self.mq = MessageQueue(conn)
        self.__queue_ctrl = Queue(MSG_Q_MAX_DEPTH)              # 控制队列
        self.__queue_ctrl.put((True, None))
        self.mine = None

    def open(self, func: callable):
        self.mine = Thread(target=employer, args=(self.mq, self.__queue_ctrl, func))
        self.mine.setDaemon(False)
        self.mine.start()

    def close(self):
        self.__queue_ctrl.put(False)


def allocator(conn: URL):
    if conn is None:
        return None
    else:
        if conn.scheme in ["oracle", "mysql"]:
            return SQL(conn)
        if conn.scheme in ["tcp"]:
            return MessageQueue(conn)
        if conn.scheme in ["ftp"]:
            return FTP(conn)
        if conn.scheme in ["ftpd"]:
            return FTPD(conn)
        if conn.scheme in ["tomail"]:
            return TOMAIL(conn)


def sender(msg: MSG):
    # employee
    origination = allocator(msg.origination)
    destination = allocator(msg.destination)
    treatment = allocator(msg.treatment)
    encryption = allocator(msg.encryption)
    # process
    if origination is None:
        msg_origination = msg
    else:
        msg_origination = origination.read(msg)
    if encryption is None:
        msg_encryption = msg_origination
    else:
        msg_encryption = encryption.request(msg_origination)
    if treatment is None:
        msg_treatment = msg_encryption
    else:
        msg_treatment = treatment.request(msg_encryption)
    if destination is None:
        pass
    else:
        destination.push(msg_treatment)


def treater(msg: MSG, func: callable):
    origination = allocator(msg.origination)
    destination = allocator(msg.destination)
    treatment = allocator(msg.treatment)
    encryption = allocator(msg.encryption)
    if origination is None:
        msg_origination = msg
    else:
        msg_origination = origination.read(msg)
    if encryption is None:
        msg_encryption = msg_origination
    else:
        msg_encryption = encryption.request(msg_origination)
    if treatment is None:
        msg_treatment = msg_encryption
    else:
        msg_treatment = treatment.request(msg_encryption)
    if destination is None:
        pass
    else:
        destination.push(msg_treatment)


