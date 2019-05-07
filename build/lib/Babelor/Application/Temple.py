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
import time
from multiprocessing import Queue, Process
# Outer Required
# Inner Required
from Babelor.Presentation import URL, CASE, MSG
from Babelor.Config import GLOBAL_CFG
from Babelor.Session import MQ
from Babelor.Data import SQL, FTP, FTPD, TOMAIL, FILE
# Global Parameters
MSG_Q_MAX_DEPTH = GLOBAL_CFG["MSG_Q_MAX_DEPTH"]
CTRL_Q_MAX_DEPTH = GLOBAL_CFG["CTRL_Q_MAX_DEPTH"]
CODING = GLOBAL_CFG["CODING"]
BlockingTime = GLOBAL_CFG["MSG_Q_BlockingTime"]


def priest(conn: URL, queue_ctrl: Queue, queue_in: Queue):
    # Inner Required
    from Babelor.Session import MQ
    mq = MQ(conn)
    is_active = queue_ctrl.get()
    while is_active:
        if queue_ctrl.empty():
            while queue_in.full():
                time.sleep(BlockingTime)
            else:
                msg = mq.pull()
                print("priest:", conn, msg)
                queue_in.put(msg)
        else:
            is_active = queue_ctrl.get()
    else:
        queue_ctrl.close()
        queue_in.close()
        mq.close()
        del mq


class TEMPLE:
    def __init__(self, conn: URL):
        # conn = "tcp://*:port"
        self.me = conn
        self.priest_queue_in = Queue(MSG_Q_MAX_DEPTH)
        self.priest_queue_ctrl = Queue(CTRL_Q_MAX_DEPTH)
        self.priest_queue_ctrl.put(True)
        self.priest = Process(target=priest,
                              args=(self.me, self.priest_queue_ctrl, self.priest_queue_in))
        self.believer_queue_ctrl = Queue(CTRL_Q_MAX_DEPTH)
        self.believer = None

    def open(self, role: str, func: callable = None):
        self.priest.start()
        while self.priest_queue_in.empty():
            time.sleep(BlockingTime)
        else:
            msg = self.priest_queue_in.get()
            self.believer_queue_ctrl.put(True)
            if role in ["sender"]:
                self.believer = Process(target=sender, args=(msg, self.believer_queue_ctrl, func))
            elif role in ["treater", "encrypter"]:
                self.believer = Process(target=treater, args=(msg, self.believer_queue_ctrl, func))
            elif role in ["receiver"]:
                self.believer = Process(target=receiver, args=(msg, self.believer_queue_ctrl, func))
            else:       # default is treater
                self.believer = Process(target=treater, args=(msg, self.believer_queue_ctrl, func))
            self.believer.start()

    def close(self):
        self.believer_queue_ctrl.put(False)
        self.priest_queue_ctrl.put(False)
        try:
            self.priest.close()
        except ValueError:
            self.priest.kill()
        try:
            self.believer.close()
        except ValueError:
            self.believer.kill()


def allocator(conn: URL):
    if conn is None:
        return None
    else:
        if conn.scheme in ["oracle", "mysql"]:
            return SQL(conn)
        if conn.scheme in ["tcp"]:
            return MQ(conn)
        if conn.scheme in ["ftp"]:
            return FTP(conn)
        if conn.scheme in ["ftpd"]:
            return FTPD(conn)
        if conn.scheme in ["tomail"]:
            return TOMAIL(conn)
        if conn.scheme in ["file"]:
            return FILE(conn)


def sender(msg: MSG, queue_ctrl: Queue, func: callable = None):
    """
    :param msg: MSG             # 消息报
    :param queue_ctrl: Queue    # 控制 ("is_active",):(bool,)
    :param func: callable       # 自定义处理过程
    :return: None
    """
    # init
    print("sender:", msg)
    origination = allocator(msg.origination)    # Data.read(msg)
    treatment = allocator(msg.treatment)        # MessageQueue
    encryption = allocator(msg.encryption)      # MessageQueue
    destination = allocator(msg.destination)    # MessageQueue
    # Control Flow
    is_active = queue_ctrl.get()                # 控制信号（初始化）
    if is_active:                               # 控制信号（启动）
        if queue_ctrl.empty():                  # 控制信号（无变更），敏捷响应
            # --------- origination -----------------------------------------
            if isinstance(origination, MQ):
                msg_origination = origination.pull()
            else:
                msg_origination = origination.read(msg)
            # --------- encryption ------------------------------------------
            if encryption is None:
                msg_encryption = msg_origination
            else:
                msg_encryption = encryption.request(msg_origination)
            del msg_origination
            # --------- treatment -------------------------------------------
            if treatment is None:
                msg_treatment = msg_encryption
            else:
                msg_treatment = treatment.request(msg_encryption)
            del msg_encryption
            # --------- function --------------------------------------------
            if func is None:
                msg_function = msg_treatment
            else:
                msg_function = func(msg_treatment)
            del msg_treatment
            # --------- destination -----------------------------------------
            if isinstance(destination, MQ):
                destination.push(msg_function)
            else:
                destination.write(msg_function)
        else:
            is_active = queue_ctrl.get()
    else:
        queue_ctrl.close()                 # 队列关闭
        del origination, encryption, treatment, destination


def receiver(msg: MSG, queue_ctrl: Queue, func: callable = None):
    # init
    print("receiver:", msg)
    origination = allocator(msg.origination)    # MessageQueue
    treatment = allocator(msg.treatment)        # MessageQueue
    encryption = allocator(msg.encryption)      # MessageQueue
    destination = allocator(msg.destination)    # Data.write
    # Control Flow
    is_active = queue_ctrl.get()                # 控制信号（初始化）
    while is_active:                            # 控制信号（启动）
        if queue_ctrl.empty():                  # 控制信号（无变更），敏捷响应
            # --------- origination -----------------------------------------
            if isinstance(origination, MQ):
                msg_origination = origination.pull()
            else:
                msg_origination = origination.read(msg)
            # --------- encryption ------------------------------------------
            if encryption is None:
                msg_encryption = msg_origination
            else:
                msg_encryption = encryption.request(msg_origination)
            del msg_origination
            # --------- treatment -------------------------------------------
            if treatment is None:
                msg_treatment = msg_encryption
            else:
                msg_treatment = treatment.request(msg_encryption)
            del msg_encryption
            # --------- function --------------------------------------------
            if func is None:
                msg_function = msg_treatment
            else:
                msg_function = func(msg_treatment)
            del msg_treatment
            # --------- destination -----------------------------------------
            if isinstance(destination, MQ):
                destination.push(msg_function)
            else:
                destination.write(msg_function)
        else:
            is_active = queue_ctrl.get()        # 控制信号（变更）
    else:
        queue_ctrl.close()                       # 队列关闭
        del origination, encryption, treatment, destination


def treater(msg: MSG, queue_ctrl: Queue, func: callable = None):
    """
    :param msg: MSG             # 消息报
    :param queue_ctrl: Queue    # 控制 ("is_active",):(bool,)
    :param func: callable       # 自定义处理过程
    :return: None
    """
    # init
    origination = allocator(msg.origination)    # Data.read()
    treatment = allocator(msg.treatment)        # MessageQueue
    encryption = allocator(msg.encryption)      # MessageQueue
    destination = allocator(msg.destination)    # Data.write()

    def treat_msg(msg_orig):
        # --------- encryption ------------------------------------------
        if encryption is None:
            msg_encryption = msg_orig
        else:
            msg_encryption = encryption.request(msg_orig)
        # --------- treatment -------------------------------------------
        if treatment is None:
            msg_treatment = msg_encryption
        else:
            msg_treatment = treatment.request(msg_encryption)
        del msg_encryption
        # --------- function --------------------------------------------
        if func is None:
            msg_func = msg_treatment
        else:
            msg_func = func(msg_treatment)
        del msg_treatment
        # --------- destination -----------------------------------------
        if destination is None:
            pass
        else:
            if isinstance(destination, MQ):
                destination.push(msg_func)
            else:
                destination.write(msg_func)
        return msg_func

    # Control Flow
    is_active = queue_ctrl.get()                # 控制信号（初始化）
    while is_active:                            # 控制信号（启动）
        if queue_ctrl.empty():                  # 控制信号（无变更），敏捷响应
            # ------------------------------------------------------
            if isinstance(origination, MQ):
                origination.reply(treat_msg)
            else:
                msg_origination = origination.read(msg)
                treat_msg(msg_origination)
                del msg_origination
        else:
            is_active = queue_ctrl.get()        # 控制信号（变更）
    else:
        queue_ctrl.close()                      # 队列关闭
        del origination, encryption, treatment, destination
