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
from multiprocessing import Queue, Process, Pipe
# Outer Required
import zmq
# Inner Required
from Babelor.Presentation import MSG, URL
# Global Parameters
from Babelor.Config import CONFIG


def first_out_last_in(conn: str, me: str, queue_ctrl: Queue, pipe_in: Pipe, pipe_out: Pipe):
    """
    # 先出后进 / 只出
    # 控制信号 --> 输出队列 --> 输出队列 --> 反馈信号 --> 输入队列
    :param conn: str            # 套接字    "tcp://<hostname>:<port>"
    :param me: str              # 传输方式  ["REQUEST", "SUBSCRIBE", "PUSH"]
    :param queue_ctrl: Queue    # 控制队列  ("is_active",):(bool,)
    :param pipe_in: Pipe        # 输入队列  ("msg_in",):(MSG,)
    :param pipe_out: Pipe       # 输出队列  ("msg_out",):(MSG,)
    :return: None
    """
    context = zmq.Context()
    # ------- REQUEST ----------------------------
    if me in ["REQUEST"]:
        socket = context.socket(zmq.REQ)
        socket.connect(conn)
        handshake, has_response = 0, True
    # ------- SUBSCRIBE --------------------------
    elif me in ["SUBSCRIBE"]:
        socket = context.socket(zmq.SUB)
        socket.connect(conn)
        handshake, has_response = 1, True
    # ------- PUSH ------------------------------
    elif me in ["PUSH"]:
        socket = context.socket(zmq.PUSH)
        socket.connect(conn)
        handshake, has_response = 0, False
    # ------- DEFAULT: PUSH ---------------------
    else:
        socket = context.socket(zmq.PUSH)
        socket.connect(conn)
        handshake, has_response = 0, False
    logging.debug("ZMQ::FOLI::{0} connect:{1}".format(me, conn))
    # ------------------------------------- QUEUE
    is_active = queue_ctrl.get()
    while is_active:
        if queue_ctrl.empty():
            # SEND --------------------------------
            if handshake == 1:
                socket.setsockopt(zmq.SUBSCRIBE, '')
                logging.debug("ZMQ::FOLI::{0}::{1} send:{2}".format(me, conn, "zmq.SUBSCRIBE"))
            else:
                try:
                    msg_out = pipe_out.recv()
                    logging.debug("ZMQ::FOLI::{0}::{1}::PIPE OUT recv:{2}".format(me, conn, msg_out))
                    message_out = str(msg_out).encode(CONFIG.Coding)
                    socket.send(message_out)
                    logging.debug("ZMQ::FOLI::{0}::{1} send:{2}".format(me, conn, message_out))
                except EOFError:
                    is_active = False
            # RECV --------------------------------
            if has_response:
                message_in = socket.recv()
                logging.debug("ZMQ::FOLI::{0}::{1} recv:{2}".format(me, conn, message_in))
                msg_in = MSG(message_in.decode(CONFIG.Coding))
                pipe_in.send(msg_in)
                logging.debug("ZMQ::FOLI::{0}::{1}::PIPE IN send:{2}".format(me, conn, msg_in))
        else:
            is_active = queue_ctrl.get()
    else:
        queue_ctrl.close()


def first_in_last_out(conn: str, me: str, queue_ctrl: Queue, pipe_in: Pipe, pipe_out: Pipe):
    """
    # 先进后出 / 只进
    # 控制信号（启动）--> 输入队列（推入）--> 控制信号（需反馈）--> 输出队列（推出）
    :param conn: str            # 套接字
    :param me: str              # 传输方式 ( "REPLY", "SUBSCRIBE", "PULL")
    :param queue_ctrl: Queue    # 控制队列 ("is_active", "is_response"):(bool, bool)
    :param pipe_in: Pipe        # 输入队列 (MSG,)
    :param pipe_out: pipe       # 输出队列 (MSG,)
    :return: None
    """
    context = zmq.Context()
    # ------- REPLY ----------------------------
    if me in ["REPLY"]:
        socket = context.socket(zmq.REP)
        socket.bind(conn)
        has_response = True
    # ------- REPLY ----------------------------
    elif me in ["PUBLISH"]:
        socket = context.socket(zmq.PUB)
        socket.bind(conn)
        has_response = False
    # ------- PULL -----------------------------
    elif me in ["PULL"]:
        socket = context.socket(zmq.PULL)
        socket.bind(conn)
        has_response = False
    # ------- DEFAULT: PULL -----------------------------
    else:
        socket = context.socket(zmq.PULL)
        socket.bind(conn)
        has_response = False
    logging.debug("ZMQ::FILO::{0} bind:{1}".format(me, conn))
    # ------------------------------------- QUEUE
    is_active = queue_ctrl.get()
    while is_active:
        if queue_ctrl.empty():
            # RECV --------------------------------
            message_in = socket.recv()
            logging.debug("ZMQ::FILO::{0}::{1} recv:{2}".format(me, conn, message_in))
            msg_in = MSG(message_in.decode(CONFIG.Coding))
            pipe_in.send(msg_in)
            logging.debug("ZMQ::FILO::{0}::{1}::PIPE IN send:{2}".format(me, conn, msg_in))
            # SEND --------------------------------
            if has_response:
                try:
                    msg_out = pipe_out.recv()
                    logging.debug("ZMQ::FILO::{0}::{1}::PIPE OUT recv:{2}".format(me, conn, msg_out))
                    message_out = str(msg_out).encode(CONFIG.Coding)
                    socket.send(message_out)
                    logging.debug("ZMQ::FILO::{0}::{1} send:{2}".format(me, conn, message_out))
                except EOFError:
                    is_active = False
        else:
            is_active = queue_ctrl.get()
    else:
        queue_ctrl.close()


class ZMQ:
    def __init__(self, conn: (URL, str)):
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        # Check
        if self.conn.scheme not in ["tcp", "pgm", "inproc"]:
            raise ValueError("Invalid scheme{0}.".format(self.conn.scheme))
        self.pipe_in = Pipe()                           # PIPE IN
        self.pipe_out = Pipe()                          # PIPE OUT
        self.queue_ctrl = Queue(CONFIG.MQ_MAX_DEPTH)    # QUEUE CTRL
        self.active = False                             # 激活状态
        self.initialed = None                           # 初始化模式
        self.process = None                             # 队列进程

    def release(self):
        if isinstance(self.process, Process):
            self.queue_ctrl.put(False)
            time.sleep(CONFIG.MQ_BLOCK_TIME)
            self.process.terminate()
        while not self.queue_ctrl.empty():
            self.queue_ctrl.get()
        self.process = None
        self.initialed = None
        self.active = False

    close = release

    def start(self, me: str):
        self.release()
        is_active = True
        while self.queue_ctrl.full():
            time.sleep(CONFIG.MQ_BLOCK_TIME)
        else:
            self.queue_ctrl.put(is_active)
        if me in ["REPLY", "SUBSCRIBE", "PULL"]:
            self.process = Process(target=first_in_last_out,
                                   name="zmq.{0}".format(me),
                                   args=(str(self.conn), me, self.queue_ctrl, self.pipe_in[0], self.pipe_out[1]))
        else:
            self.process = Process(target=first_out_last_in,
                                   name="zmq.{0}".format(me),
                                   args=(str(self.conn), me, self.queue_ctrl, self.pipe_in[0], self.pipe_out[1]))
        self.process.start()
        self.initialed = me
        self.active = is_active

    def request(self, msg: MSG):        # 先出后进::请求  ZMQ::FOLI::REQUEST
        me = "REQUEST"
        if self.initialed is None:
            self.start(me)
        elif self.initialed not in [me]:
            self.release()
            self.start(me)
        # -----------------------------
        if self.active:
            self.pipe_out[0].send(msg)
            logging.debug("ZMQ::{0}::{1}::PIPE OUT send:{2}".format(me, self.conn, msg))
            msg_in = self.pipe_in[1].recv()
            logging.debug("ZMQ::{0}::{1}::PIPE IN recv:{2}".format(me, self.conn, msg_in))
            return msg_in
        else:
            self.release()

    def reply(self, func: callable):        # 先进后出::反馈  ZMQ::FILO::REPLY
        me = "REPLY"
        if self.initialed is None:
            self.start(me)
        elif self.initialed not in [me]:
            self.release()
            self.start(me)
        # -----------------------------
        if self.active:
            msg_in = self.pipe_in[1].recv()
            logging.debug("ZMQ::{0}::{1}::PIPE IN recv:{2}".format(me, self.conn, msg_in))
            msg_out = func(msg_in)
            self.pipe_out[0].send(msg_out)
            logging.debug("ZMQ::{0}::{1}::PIPE OUT send:{2}".format(me, self.conn, msg_out))
        else:
            self.release()

    def push(self, msg: MSG):       # 只出::推出    ZMQ::FOLI::PUSH
        me = "PUSH"
        if self.initialed is None:
            self.start(me)
        elif self.initialed not in [me]:
            self.release()
            self.start(me)
        # -----------------------------
        if self.active:
            self.pipe_out[0].send(msg)
            logging.debug("ZMQ::{0}::{1}::PIPE OUT send:{2}".format(me, self.conn, msg))
        else:
            self.release()

    def pull(self):     # 只进::拉入    ZMQ::FILO::PULL
        me = "PULL"
        if self.initialed is None:
            self.start(me)
        elif self.initialed not in [me]:
            self.release()
            self.start(me)
        # -----------------------------
        if self.active:
            msg_in = self.pipe_in[1].recv()
            logging.debug("ZMQ::{0}::{1}::PIPE IN recv:{2}".format(me, self.conn, msg_in))
            return msg_in
        else:
            self.release()
            return None

    def publish(self, msg: MSG):    # 先进后出::发布    ZMQ::FILO::PUBLISH
        me = "PUBLISH"
        if self.initialed is None:
            self.start(me)
        elif self.initialed not in [me]:
            self.release()
            self.start(me)
        # -----------------------------
        if self.active:
            self.pipe_out[0].send(msg)
            logging.debug("ZMQ::{0}::{1}::PIPE OUT send:{2}".format(me, self.conn, msg))
        else:
            self.release()

    def subscribe(self):    # 先出后进::订阅  ZMQ::FOLI::SUBSCRIBE
        me = "SUBSCRIBE"
        if self.initialed is None:
            self.start(me)
        elif self.initialed not in [me]:
            self.release()
            self.start(me)
        # -----------------------------
        if self.active:
            msg_in = self.pipe_in[1].recv()
            logging.debug("ZMQ::{0}::{1}::PIPE IN recv:{2}".format(me, self.conn, msg_in))
            return msg_in
        else:
            self.release()
