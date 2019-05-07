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
from multiprocessing import Queue, Process
# Outer Required
import zmq
# Inner Required
from Babelor.Presentation import MSG, URL
from Babelor.Config import GLOBAL_CFG
# Global Parameters
MSG_Q_MAX_DEPTH = GLOBAL_CFG["MSG_Q_MAX_DEPTH"]
CTRL_Q_MAX_DEPTH = GLOBAL_CFG["CTRL_Q_MAX_DEPTH"]
CODING = GLOBAL_CFG["CODING"]
BlockingTime = GLOBAL_CFG["MSG_Q_BlockingTime"]
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def first_out_last_in(conn: str, me: str, queue_ctrl: Queue, queue_in: Queue, queue_out: Queue):
    """
    # 先出后进 / 只出
    # 控制信号 --> 输出队列 --> 输出队列 --> 反馈信号 --> 输入队列
    :param conn: str            # 套接字
    :param me: str              # 传输方式 ( "REQUEST", "PUBLISH", "PUSH")
    :param queue_ctrl: Queue    # 控制队列 ("is_active",):(bool,)
    :param queue_in: Queue      # 输出队列 ("msg_in",):(MSG,)
    :param queue_out: Queue     # 输入队列 ("msg_out",):(MSG,)
    :return: None
    """
    context = zmq.Context()
    # ------- REQUEST ----------------------------
    if me in ["REQUEST"]:
        socket = context.socket(zmq.REQ)
        socket.connect(conn)
        handshake, has_response = 0, True
        logging.debug("ZMQ FOLI:REQUEST connect:{0}".format(conn))
    # ------- SUBSCRIBE --------------------------
    elif me in ["SUBSCRIBE"]:
        socket = context.socket(zmq.SUB)
        socket.connect(conn)
        handshake, has_response = 1, True
        logging.debug("ZMQ FOLI:SUBSCRIBE connect:{0}".format(conn))
    # ------- PUSH ------------------------------
    elif me in ["PUSH"]:
        socket = context.socket(zmq.PUSH)
        socket.connect(conn)
        handshake, has_response = 0, False
        logging.debug("ZMQ FOLI:PUSH connect:{0}".format(conn))
    # ------- DEFAULT: PUSH ---------------------
    else:
        socket = context.socket(zmq.PUSH)
        socket.connect(conn)
        handshake, has_response = 0, False
        logging.debug("ZMQ FOLI::{0} connect:{1}".format(me, conn))
    # ------------------------------------- QUEUE
    is_active = queue_ctrl.get()
    while is_active:
        if queue_ctrl.empty():
            # SEND --------------------------------
            if handshake == 1:
                socket.setsockopt(zmq.SUBSCRIBE, '')
                logging.info("ZMQ FOLI::{0}::{1} send:{2}".format(me, conn, "zmq.SUBSCRIBE"))
            else:
                while queue_out.empty():
                    time.sleep(BlockingTime)
                else:
                    msg_out = queue_out.get()
                logging.debug("ZMQ FOLI::{0}::{1} QUEUE OUT:{2}".format(me, conn, msg_out))
                message_out = str(msg_out).encode(CODING)
                socket.send(message_out)
                logging.info("ZMQ FOLI::{0}::{1} send:{2}".format(me, conn, message_out))
            # RECV --------------------------------
            if has_response:
                while queue_in.full():
                    time.sleep(BlockingTime)
                else:
                    message_in = socket.recv()
                    logging.info("ZMQ FOLI:{0}:{1} receive:{2}".format(me, conn, message_in))
                    msg_in = MSG(message_in.decode(CODING))
                    queue_in.put(msg_in)
                    logging.debug("ZMQ FOLI::{0}::{1} QUEUE IN:{2}".format(me, conn, msg_in))
        else:
            is_active = queue_ctrl.get()
    else:
        queue_ctrl.close()
        queue_out.close()
        queue_in.close()


def first_in_last_out(conn: str, me: str, queue_ctrl: Queue, queue_in: Queue, queue_out: Queue):
    """
    # 先进后出 / 只进
    # 控制信号（启动）--> 输入队列（推入）--> 控制信号（需反馈）--> 输出队列（推出）
    :param conn: str            # 套接字
    :param me: str              # 传输方式 ( "REPLY", "SUBSCRIBE", "PULL")
    :param queue_ctrl: Queue    # 控制队列 ("is_active", "is_response"):(bool, bool)
    :param queue_in: Queue      # 输出队列 (MSG,)
    :param queue_out: Queue     # 输入队列 (MSG,)
    :return: None
    """
    context = zmq.Context()
    # ------- REPLY ----------------------------
    if me in ["REPLY"]:
        socket = context.socket(zmq.REP)
        socket.bind(conn)
        has_response = True
        logging.debug("ZMQ FILO:REPLY bind:{1}".format(me, conn))
    # ------- REPLY ----------------------------
    elif me in ["PUBLISH"]:
        socket = context.socket(zmq.PUB)
        socket.bind(conn)
        has_response = False
        logging.debug("ZMQ FILO:PUBLISH bind:{1}".format(me, conn))
    # ------- PULL -----------------------------
    elif me in ["PULL"]:
        socket = context.socket(zmq.PULL)
        socket.bind(conn)
        has_response = False
        logging.debug("ZMQ FILO:PULL bind:{1}".format(me, conn))
    # ------- DEFAULT: PULL -----------------------------
    else:
        socket = context.socket(zmq.PULL)
        socket.bind(conn)
        has_response = False
        logging.debug("ZMQ FILO::{0} bind:{1}".format(me, conn))
    # ------------------------------------- QUEUE
    is_active = queue_ctrl.get()
    while is_active:
        if queue_ctrl.empty():
            # RECV --------------------------------
            message_in = socket.recv()
            logging.info("ZMQ FILO:{0}:{1} receive:{2}".format(me, conn, message_in))
            msg_in = MSG(message_in.decode(CODING))
            while queue_in.full():
                time.sleep(BlockingTime)
            else:
                queue_in.put(msg_in)
            # SEND --------------------------------
            if has_response:
                while queue_out.empty():
                    time.sleep(BlockingTime)
                else:
                    msg_out = queue_out.get()
                    message_out = str(msg_out).encode(CODING)
                    socket.send(message_out)
        else:
            is_active = queue_ctrl.get()
    else:
        queue_ctrl.close()
        queue_out.close()
        queue_in.close()


class ZMQ:
    def __init__(self, conn: (URL, str)):
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        # Check
        if self.conn.scheme not in ["tcp", "pgm", "inproc"]:
            raise ValueError("Invalid scheme{0}.".format(self.conn.scheme))
        self.queue_in = Queue(MSG_Q_MAX_DEPTH)     # QUEUE IN
        self.queue_out = Queue(MSG_Q_MAX_DEPTH)    # QUEUE OUT
        self.queue_ctrl = Queue(CTRL_Q_MAX_DEPTH)  # QUEUE CTRL
        self.active = False                        # 激活状态
        self.initialed = None                      # 已初始化模式
        self.process = None                        # 队列控制进程

    def release(self):
        if self.active:                                    # 激活状态
            if self.initialed is None:                     # 未初始化模式
                self.active = False
            else:                                          # 已初始化模式
                self.queue_ctrl.put(False)                 # 控制信号（停止）
                time.sleep(BlockingTime)
                if isinstance(self.process, Process):      # 进程释放（软释放）
                    self.process.terminate()
                else:
                    self.process = None
                self.queue_in = Queue(MSG_Q_MAX_DEPTH)     # 进站队列（初始化）
                self.queue_out = Queue(MSG_Q_MAX_DEPTH)    # 出站队列（初始化）
                self.queue_ctrl = Queue(CTRL_Q_MAX_DEPTH)  # 控制队列（初始化）
                self.initialed = None                      # 已初始化模式（无模式，初始化）
                self.active = False                          # 激活状态（未激活，初始化）
        else:
            pass

    close = release

    def __start(self, me: str, func: callable):
        is_active = True
        if self.initialed is not None:    # 已初始化
            self.release()
        self.queue_ctrl.put(is_active)    # 控制信号
        self.process = Process(target=func, name="zmq.{0}".format(me),
                               args=(str(self.conn), me, self.queue_ctrl, self.queue_in, self.queue_out))
        self.process.start()
        self.initialed = me
        self.active = True

    def request(self, msg_out: MSG):        # 先出后进
        me = "REQUEST"
        if self.initialed not in [me, ]:
            self.__start(me=me, func=first_out_last_in)
        if self.active:
            while self.queue_out.full():                      # 出 / put
                time.sleep(BlockingTime)
            else:
                if isinstance(msg_out, MSG):                    # MSG 校验
                    self.queue_out.put(msg_out)               # 输出数据
                    while self.queue_in.empty():              # 进 / get
                        time.sleep(BlockingTime)
                    else:
                        msg_in = self.queue_in.get()          # 输入数据
                        if isinstance(msg_in, MSG):             # MSG 校验
                            return msg_in
                        del msg_in
        else:
            self.release()
            return None

    def reply(self, func: callable):    # 先进后出
        me = "REPLY"
        if self.initialed not in [me, ]:
            self.__start(me=me, func=first_in_last_out)
        if self.active:
            while self.queue_in.empty():
                time.sleep(BlockingTime)
            else:
                msg_in = self.queue_in.get()                  # 输入数据
                if isinstance(msg_in, MSG):                     # MSG 校验
                    msg_out = func(msg_in)
                    if isinstance(msg_out, MSG):                # MSG 校验
                        while self.queue_out.full():
                            time.sleep(BlockingTime)
                        else:
                            self.queue_out.put(msg_out)       # 输出数据
                    del msg_out
        else:
            self.release()

    def push(self, msg_out: MSG):       # 只出
        me = "PUSH"
        if self.initialed not in [me, ]:
            self.__start(me=me, func=first_out_last_in)
        if self.active:
            # print("MQ PUSH:", msg_out)
            while self.queue_out.full():
                time.sleep(BlockingTime)
            else:
                self.queue_out.put(msg_out)               # 输出数据
                time.sleep(BlockingTime)
        else:
            self.release()

    def pull(self):     # 只进
        me = "PULL"
        if self.initialed not in [me, ]:
            self.__start(me=me, func=first_in_last_out)
        if self.active:
            while self.queue_in.empty():
                time.sleep(BlockingTime)
            else:
                msg_in = self.queue_in.get()                 # 输入数据
                # print("MQ PULL:", msg_in)
                return msg_in
        else:
            self.release()
            return None

    def publish(self, msg_out: MSG):    # 只出
        me = "PUBLISH"
        if self.initialed not in [me]:
            self.__start(me=me, func=first_out_last_in)
        if self.active:
            if isinstance(msg_out, MSG):                    # MSG 校验
                while self.queue_out.full():
                    time.sleep(BlockingTime)
                else:
                    self.queue_out.put(msg_out)           # 输出数据
        else:
            self.release()

    def subscribe(self):    # 只进
        me = "SUBSCRIBE"
        if self.initialed not in [me]:
            self.__start(me=me, func=first_in_last_out)
        if self.active:
            while self.queue_in.empty():
                time.sleep(BlockingTime)
            else:
                msg_in = self.queue_in.get()        # 输入数据
                if isinstance(msg_in, MSG):
                    return msg_in
                else:
                    del msg_in
        else:
            self.release()
            return None
