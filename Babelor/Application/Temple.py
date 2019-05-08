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
import logging
from multiprocessing import Process, Pipe, Queue
# Outer Required
# Inner Required
from Babelor.Presentation import URL
from Babelor.Config import GLOBAL_CFG
from Babelor.Session import MQ
from Babelor.Data import SQL, FTP, FTPD, TOMAIL, FILE, EXCEL
# Global Parameters
MSG_Q_MAX_DEPTH = GLOBAL_CFG["MSG_Q_MAX_DEPTH"]
CTRL_Q_MAX_DEPTH = GLOBAL_CFG["CTRL_Q_MAX_DEPTH"]
CODING = GLOBAL_CFG["CODING"]
BlockingTime = GLOBAL_CFG["MSG_Q_BlockingTime"]


def priest(conn: URL, queue_ctrl: Queue, pipe_in: Pipe):
    mq = MQ(conn)
    is_active = queue_ctrl.get()
    while is_active:
        if queue_ctrl.empty():
            msg_in = mq.pull()
            logging.info("TEMPLE::{0} pull:{1}".format(conn, msg_in))
            pipe_in.send(msg_in)
            logging.debug("TEMPLE::{0}::PIPE IN send:{1}".format(conn, msg_in))
        else:
            is_active = queue_ctrl.get()
    else:
        queue_ctrl.close()


class TEMPLE:
    def __init__(self, conn: (URL, str)):
        # "tcp://*:<port>"
        self.me = conn
        self.priest_pipe_in = Pipe()
        self.priest_queue_ctrl = Queue(CTRL_Q_MAX_DEPTH)
        self.believer_queue_ctrl = Queue(CTRL_Q_MAX_DEPTH)
        self.priest = None
        self.believer = None

    def start(self):
        is_active = True
        self.priest_queue_ctrl.put(is_active)
        self.priest = Process(target=priest,
                              args=(self.me, self.priest_queue_ctrl, self.priest_pipe_in[0]))
        self.priest.start()

    def open(self, role: str, func: callable = None):
        is_active = True
        self.start()
        self.believer_queue_ctrl.put(is_active)
        if role in ["sender"]:
            self.believer = Process(target=sender, args=(self.priest_pipe_in[1], self.believer_queue_ctrl, func))
        elif role in ["treater", "encrypter"]:
            self.believer = Process(target=treater, args=(self.priest_pipe_in[1], self.believer_queue_ctrl, func))
        elif role in ["receiver"]:
            self.believer = Process(target=receiver, args=(self.priest_pipe_in[1], self.believer_queue_ctrl, func))
        else:       # default is treater
            self.believer = Process(target=treater, args=(self.priest_pipe_in[1], self.believer_queue_ctrl, func))
        self.believer.start()

    def close(self):
        is_active = False
        if isinstance(self.believer, Process):
            self.believer_queue_ctrl.put(is_active)
            time.sleep(BlockingTime)
            self.believer.terminate()
        while not self.believer_queue_ctrl.empty():
            self.believer_queue_ctrl.get()
        self.believer = None

    def stop(self):
        self.close()
        is_active = False
        if isinstance(self.priest, Process):
            self.priest_queue_ctrl.put(is_active)
            time.sleep(BlockingTime)
            self.priest.terminate()
        while not self.priest_queue_ctrl.empty():
            self.priest_queue_ctrl.get()
        self.priest = None


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
        if conn.scheme in ["excel"]:
            return EXCEL(conn)


def sender(pipe_in: Pipe, queue_ctrl: Queue, func: callable = None):
    """
    :param pipe_in: Pipe         # 消息管道 （MSG, ）
    :param queue_ctrl: Queue     # 控制 ("is_active",):(bool,)
    :param func: callable        # 自定义处理过程
    :return: None
    """
    is_active = queue_ctrl.get()
    while is_active:
        # ----------------------------------------- Queue
        if queue_ctrl.empty():
            try:
                msg_sender = pipe_in.recv()
                logging.info("TEMPLE::SENDER PIPE IN recv:{0}".format(msg_sender))
            except EOFError:
                is_active = False
                continue
            origination = allocator(msg_sender.origination)    # Data.read(msg)
            treatment = allocator(msg_sender.treatment)        # MessageQueue
            encryption = allocator(msg_sender.encryption)      # MessageQueue
            destination = allocator(msg_sender.destination)    # MessageQueue
            # --------- origination -----------------------------------------
            if isinstance(origination, MQ):
                msg_origination = origination.request(msg_sender)
                logging.debug("TEMPLE::SENDER::{0}::ORIG request:{1}".format(msg_sender.origination, msg_origination))
            else:
                msg_origination = origination.read(msg_sender)
                logging.debug("TEMPLE::SENDER::{0}::ORIG read:{1}".format(msg_sender.origination, msg_origination))
            # --------- encryption ------------------------------------------
            if encryption is None:
                msg_encryption = msg_origination
            else:
                msg_encryption = encryption.request(msg_origination)
                logging.debug("TEMPLE::SENDER::{0}::ENCRYPT request:{1}".format(msg_sender.encryption, msg_encryption))
            del msg_origination
            # --------- treatment -------------------------------------------
            if treatment is None:
                msg_treatment = msg_encryption
            else:
                msg_treatment = treatment.request(msg_encryption)
                logging.debug("TEMPLE::SENDER::{0}::TREAT request:{1}".format(msg_sender.treatment, msg_treatment))
            del msg_encryption
            # --------- function --------------------------------------------
            if func is None:
                msg_function = msg_treatment
            else:
                msg_function = func(msg_treatment)
                logging.debug("TEMPLE::SENDER func:{0}".format(msg_function))
            del msg_treatment
            # --------- destination -----------------------------------------
            if isinstance(destination, MQ):
                destination.push(msg_function)
                logging.info("TEMPLE::SENDER::{0}::DEST push:{1}".format(msg_sender.destination, msg_function))
            else:
                destination.write(msg_function)
                logging.info("TEMPLE::SENDER::{0}::DEST write:{1}".format(msg_sender.destination, msg_function))
        else:
            is_active = queue_ctrl.get()
    else:
        while not queue_ctrl.empty():
            queue_ctrl.get()


def receiver(pipe_in: Pipe, queue_ctrl: Queue, func: callable = None):
    """
    :param pipe_in: Pipe         # 消息管道 （MSG, ）
    :param queue_ctrl: Queue     # 控制 ("is_active",):(bool,)
    :param func: callable        # 自定义处理过程
    :return: None
    """
    is_active = queue_ctrl.get()
    while is_active:
        # ----------------------------------------- Queue
        if queue_ctrl.empty():
            try:
                msg_receiver = pipe_in.recv()
                logging.info("TEMPLE::RECEIVER PIPE IN recv:{0}".format(msg_receiver))
            except EOFError:
                is_active = False
                continue
            origination = allocator(msg_receiver.origination)    # MessageQueue
            treatment = allocator(msg_receiver.treatment)        # MessageQueue
            encryption = allocator(msg_receiver.encryption)      # MessageQueue
            destination = allocator(msg_receiver.destination)    # Data.write
            # --------- origination -----------------------------------------
            if isinstance(origination, MQ):
                msg_origination = origination.pull()
                logging.info("TEMPLE::RECEIVER::{0}::ORIG pull:{1}".format(msg_receiver.origination, msg_origination))
            else:
                msg_origination = origination.read(msg_receiver)
                logging.info("TEMPLE::RECEIVER::{0}::ORIG read:{1}".format(msg_receiver.origination, msg_origination))
            # --------- encryption ------------------------------------------
            if encryption is None:
                msg_encryption = msg_origination
            else:
                msg_encryption = encryption.request(msg_origination)
                logging.debug("TEMPLE::RECEIVER::{0}::ENCRYPT request:{1}".format(msg_receiver.encryption,
                                                                                  msg_encryption))
            del msg_origination
            # --------- treatment -------------------------------------------
            if treatment is None:
                msg_treatment = msg_encryption
            else:
                msg_treatment = treatment.request(msg_encryption)
                logging.debug("TEMPLE::RECEIVER::{0}::TREAT request:{1}".format(msg_receiver.treatment,
                                                                                msg_treatment))
            del msg_encryption
            # --------- function --------------------------------------------
            if func is None:
                msg_function = msg_treatment
            else:
                msg_function = func(msg_treatment)
                logging.debug("TEMPLE::RECEIVER func:{0}".format(msg_function))
            del msg_treatment
            # --------- destination -----------------------------------------
            if isinstance(destination, MQ):
                destination.request(msg_function)
                logging.info("TEMPLE::RECEIVER::{0}::DEST request:{1}".format(msg_receiver.destination,
                                                                              msg_function))
            else:
                destination.write(msg_function)
                logging.info("TEMPLE::RECEIVER::{0}::DEST write:{1}".format(msg_receiver.destination,
                                                                            msg_function))
        else:
            is_active = queue_ctrl.get()
    else:
        while not queue_ctrl.empty():
            queue_ctrl.get()


def treater(pipe_in: Pipe, queue_ctrl: Queue, func: callable = None):
    """
    :param pipe_in: Pipe         # 消息管道 （MSG, ）
    :param queue_ctrl: Queue     # 控制 ("is_active",):(bool,)
    :param func: callable        # 自定义处理过程
    :return: None
    """
    def treat_msg(msg_orig):
        # --------- encryption ------------------------------------------
        if encryption is None:
            msg_encryption = msg_orig
        else:
            msg_encryption = encryption.request(msg_orig)
            logging.debug("TEMPLE::TREATER::{0}::ENCRYPT request:{1}".format(msg_orig.encryption,
                                                                             msg_encryption))
        # --------- treatment -------------------------------------------
        if treatment is None:
            msg_treatment = msg_encryption
        else:
            msg_treatment = treatment.request(msg_encryption)
            logging.debug("TEMPLE::TREATER::{0}::TREAT request:{1}".format(msg_orig.treatment,
                                                                           msg_treatment))
        del msg_encryption
        # --------- function --------------------------------------------
        if func is None:
            msg_func = msg_treatment
        else:
            msg_func = func(msg_treatment)
            logging.debug("TEMPLE::TREATER func:{0}".format(msg_func))
        del msg_treatment
        # --------- destination -----------------------------------------
        if destination is None:
            logging.info("TEMPLE::TREATER::NONE::DEST return:{0}".format(msg_func))
        else:
            if isinstance(destination, MQ):
                destination.request(msg_func)
                logging.info("TEMPLE::TREATER::{0}::DEST request:{1}".format(msg_orig.destination, msg_func))
            else:
                destination.write(msg_func)
                logging.info("TEMPLE::TREATER::{0}::DEST write:{1}".format(msg_orig.destination, msg_func))
        return msg_func

    is_active = queue_ctrl.get()
    while is_active:
        if queue_ctrl.empty():
            try:
                msg_treater = pipe_in.recv()
                logging.info("TEMPLE::TREATER PIPE IN recv:{0}".format(msg_treater))
            except EOFError:
                is_active = False
                continue
            origination = allocator(msg_treater.origination)    # Data.read()
            treatment = allocator(msg_treater.treatment)        # MessageQueue
            encryption = allocator(msg_treater.encryption)      # MessageQueue
            destination = allocator(msg_treater.destination)    # Data.write()
            # --------- origination -----------------------------------------
            if isinstance(origination, MQ):
                origination.reply(func=treat_msg)
            else:
                msg_origination = origination.read(msg_treater)
                logging.info("TEMPLE::TREATER::{0}::ORIG read:{1}".format(msg_treater.origination, msg_origination))
                treat_msg(msg_origination)
        else:
            is_active = queue_ctrl.get()
    else:
        while not queue_ctrl.empty():
            queue_ctrl.get()
