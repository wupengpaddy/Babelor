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
from Babelor.Message import URL, MSG
from Babelor.Config import GLOBAL_CFG
from Babelor.Process import MessageQueue
from Babelor.Session import TOMAIL, FTP, FTPD
from Babelor.DataBase import SQL
# Global Parameters
MSG_Q_MAX_DEPTH = GLOBAL_CFG["MSG_Q_MAX_DEPTH"]
CODING = GLOBAL_CFG["CODING"]
BlockingTime = GLOBAL_CFG["MSG_Q_BlockingTime"]


class TEMPLE:
    def __init__(self, conn: URL):
        # conn = "tcp://*:port"
        self.me = conn
        self.mq = MessageQueue(conn)
        self.__queue_ctrl = Queue(MSG_Q_MAX_DEPTH)              # 控制队列
        self.__queue_ctrl.put((True, None))
        self.mine = None

    def open(self, role: str, func: callable = None):
        if role in ["sender"]:
            self.mine = Thread(target=sender, args=(self.mq, self.__queue_ctrl, func))
        elif role in ["treater"]:
            self.mine = Thread(target=treater, args=(self.mq, self.__queue_ctrl, func))
        elif role in ["encrypter"]:
            self.mine = Thread(target=encrypter, args=(self.mq, self.__queue_ctrl, func))
        elif role in ["receiver"]:
            self.mine = Thread(target=receiver, args=(self.mq, self.__queue_ctrl, func))
        else:       # default
            self.mine = Thread(target=receiver, args=(self.mq, self.__queue_ctrl, func))
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


def sender(msg: MSG, queue_ctrl: Queue, func: callable = None):
    """
    :param msg: MSG             # 消息报
    :param queue_ctrl: Queue    # 控制 ("is_active",):(bool,)
    :param func: callable       # 自定义处理过程
    :return: None
    """
    # employee
    origination = allocator(msg.origination)
    destination = allocator(msg.destination)    # MessageQueue
    treatment = allocator(msg.treatment)        # MessageQueue
    encryption = allocator(msg.encryption)      # MessageQueue

    def process_msg(msg_orig):
        if encryption is None:
            msg_encryption = msg_orig
        else:
            msg_encryption = encryption.request(msg_orig)
        if treatment is None:
            msg_treatment = msg_encryption
        else:
            msg_treatment = treatment.request(msg_encryption)
        if func is None:
            return msg_treatment
        else:
            return func(msg_treatment)

    is_active = queue_ctrl.get()            # 控制信号（初始化）
    if is_active:                           # 控制信号（启动）
        if origination is None:
            msg_origination = msg
        else:
            msg_origination = origination.read(msg)
        msg_out = process_msg(msg_origination)
        destination.push(msg_out)
    else:
        queue_ctrl.join()                   # 队列关闭
        del origination, encryption, treatment, destination


def treater(msg: MSG, queue_ctrl: Queue, func: callable = None):
    """
    :param msg: MSG             # 消息报
    :param queue_ctrl: Queue    # 控制 ("is_active",):(bool,)
    :param func: callable       # 自定义处理过程
    :return: None
    """
    # employee
    origination = allocator(msg.origination)    # MessageQueue
    treatment = allocator(msg.treatment)        # MessageQueue
    encryption = allocator(msg.encryption)      # MessageQueue
    destination = allocator(msg.encryption)     # MessageQueue

    def process_msg(msg_in):
        if encryption is None:
            msg_encryption = msg_in
        else:
            msg_encryption = encryption.request(msg_in)
        if treatment is None:
            msg_treatment = msg_encryption
        else:
            msg_treatment = treatment.request(msg_encryption)
        if func is None:
            msg_func = msg_treatment
        else:
            msg_func = func(msg_treatment)
        if destination is None:
            pass
        else:
            destination.push(msg_func)
        return msg_func

    is_active = queue_ctrl.get()                # 控制信号（初始化）
    while is_active:                            # 控制信号（启动）
        if queue_ctrl.empty():                  # 控制信号（无变更），敏捷响应
            origination.reply(process_msg)
        else:
            is_active = queue_ctrl.get()        # 控制信号（变更）
    else:
        queue_ctrl.join()                       # 队列关闭
        del origination, encryption, treatment, destination


def receiver(msg: MSG, queue_ctrl: Queue, func: callable = None):
    # employee
    origination = allocator(msg.origination)    # MessageQueue
    treatment = allocator(msg.treatment)        # MessageQueue
    encryption = allocator(msg.encryption)      # MessageQueue
    destination = allocator(msg.destination)

    def process_msg(msg_in):
        if encryption is None:
            msg_encryption = msg_in
        else:
            msg_encryption = encryption.request(msg_in)
        if treatment is None:
            msg_treatment = msg_encryption
        else:
            msg_treatment = treatment.request(msg_encryption)
        if func is None:
            return msg_treatment
        else:
            return func(msg_treatment)

    is_active = queue_ctrl.get()                # 控制信号（初始化）
    while is_active:                            # 控制信号（启动）
        if queue_ctrl.empty():                  # 控制信号（无变更），敏捷响应
            msg_origination = origination.pull()
            msg_out = process_msg(msg_origination)
            destination.write(msg_out)
        else:
            is_active = queue_ctrl.get()        # 控制信号（变更）
    else:
        queue_ctrl.join()                       # 队列关闭
        del origination, encryption, treatment, destination


def encrypter(msg: MSG, queue_ctrl: Queue, func: callable = None):
    """
    :param msg: MSG             # 消息报
    :param queue_ctrl: Queue    # 控制 ("is_active",):(bool,)
    :param func: callable       # 自定义处理过程
    :return: None
    """
    # employee
    origination = allocator(msg.origination)    # MessageQueue
    treatment = allocator(msg.treatment)        # MessageQueue
    encryption = allocator(msg.encryption)      # MessageQueue
    destination = allocator(msg.encryption)     # MessageQueue

    def process_msg(msg_in):
        if encryption is None:
            msg_encryption = msg_in
        else:
            msg_encryption = encryption.request(msg_in)
        if treatment is None:
            msg_treatment = msg_encryption
        else:
            msg_treatment = treatment.request(msg_encryption)
        if func is None:
            msg_func = msg_treatment
        else:
            msg_func = func(msg_treatment)
        if destination is None:
            pass
        else:
            destination.push(msg_func)
        return msg_func

    is_active = queue_ctrl.get()                # 控制信号（初始化）
    while is_active:                            # 控制信号（启动）
        if queue_ctrl.empty():                  # 控制信号（无变更），敏捷响应
            origination.reply(process_msg)
        else:
            is_active = queue_ctrl.get()        # 控制信号（变更）
    else:
        queue_ctrl.join()                       # 队列关闭
        del origination, encryption, treatment, destination
