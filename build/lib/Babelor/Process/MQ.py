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
# Inner Required
from Babelor.Message import MSG, URL
from Babelor.Config import GLOBAL_CFG
# Global Parameters
MSG_Q_MAX_DEPTH = GLOBAL_CFG["MSG_Q_MAX_DEPTH"]
CODING = GLOBAL_CFG["CODING"]
BlockingTime = GLOBAL_CFG["MSG_Q_BlockingTime"]


def first_out_last_in(conn: str, me: str, queue_ctrl: Queue, queue_in: Queue, queue_out: Queue):
    """
    # 先出后进 / 只出
    # 控制信号（启动）--> 输出队列（推出）--> 控制信号（需反馈）--> 输入队列（推入）
    :param conn: str            # 套接字
    :param me: str              # 传输方式 ( "REQUEST", "PUBLISH", "PUSH")
    :param queue_ctrl: Queue    # 控制队列 ("is_active",):(bool,)
    :param queue_in: Queue      # 输出队列 ("msg_in",):(MSG,)
    :param queue_out: Queue     # 输入队列 ("msg_out",):(MSG,)
    :return: None
    """
    # Outer Required
    import zmq
    # 初始化 initial
    context = zmq.Context.instance()
    if me in ["REQUEST"]:
        socket = context.socket(zmq.REQ)
        socket.connect(conn)
        has_response = True
    elif me in ["PUBLISH"]:
        socket = context.socket(zmq.PUB)
        socket.bind(conn)
        has_response = False
    else:   # PUSH
        socket = context.socket(zmq.PUSH)
        socket.connect(conn)
        has_response = False
    is_active = queue_ctrl.get()                    # 控制信号（初始化）
    # 消息队列 to message queue
    while is_active:                                # 控制信号（启动）
        if queue_ctrl.empty():                      # 控制信号（无变更），敏捷响应
            while queue_out.empty():                # 出 / get / send
                time.sleep(BlockingTime)
            else:
                msg_out = queue_out.get()           # 输出队列（推出）
                if isinstance(msg_out, MSG):
                    message_out = str(msg_out).encode(CODING)
                    socket.send(message_out)        # 消息 出
                    del message_out                 # 释放 msg_out
                del msg_out
                if has_response:                    # 控制信号（需反馈）
                    while queue_in.full():          # 进 / put / recv
                        time.sleep(BlockingTime)
                    else:
                        message_in = socket.recv()  # 消息 进
                        msg_in = MSG(message_in.decode(CODING))
                        queue_in.put(msg_in)        # 输入队列（推入）
                        del message_in, msg_in      # 释放 msg_in
        else:                                       # 控制信号（变更）
            is_active = queue_ctrl.get()
    else:                                           # 队列关闭
        queue_ctrl.close()
        queue_out.close()
        queue_in.close()
        del socket, context


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
    # Outer Required
    import zmq
    # 初始化 initial
    context = zmq.Context.instance()
    if me in ["REPLY"]:
        socket = context.socket(zmq.REP)
        socket.bind(conn)
        has_response = True
    elif me in ["SUBSCRIBE"]:
        socket = context.socket(zmq.SUB)
        socket.connect(conn)
        socket.setsockopt(zmq.SUBSCRIBE, '')        # 初次握手订阅
        has_response = False
    else:   # PULL
        socket = context.socket(zmq.PULL)
        socket.bind(conn)
        has_response = False
    is_active = queue_ctrl.get()                    # 控制信号（初始化）
    # 消息队列 from message queue
    while is_active:                                # 控制信号（启动）
        if queue_ctrl.empty():                      # 控制信号（无变更），敏捷响应
            while queue_in.full():                  # 进 / put / recv
                time.sleep(BlockingTime)
            else:
                message_in = socket.recv()          # 消息 进
                msg_in = MSG(message_in.decode(CODING))
                queue_in.put(msg_in)                # 输入队列（推入）
                del message_in, msg_in              # 释放 msg_in
                if has_response:                    # 控制信号（需反馈）
                    while queue_out.empty():        # 出 / get / send
                        time.sleep(BlockingTime)
                    else:
                        msg_out = queue_out.get()   # 输出队列（推出）
                        if isinstance(msg_out, MSG):
                            message_out = str(msg_out).encode(CODING)
                            socket.send(message_out)  # 消息 出
                            del message_out
                        del msg_out                 # 释放 msg_out
        else:                                       # 控制信号（变更）
            is_active = queue_ctrl.get()
    else:                                           # 队列关闭
        queue_ctrl.close()
        queue_out.close()
        queue_in.close()
        del socket, context


class MessageQueue:
    def __init__(self, conn: URL):
        if isinstance(conn, str):
            self.conn = URL(conn)                 # 套接字
        else:
            self.conn = conn
        if self.conn.scheme not in ["tcp", "pgm", "inproc"]:       # 协议（校验）
            raise ValueError("Invalid scheme{0}.".format(self.conn.scheme))
        self.conn = self.conn.check   # 默认端口（校验）
        self.__queue_in = Queue(MSG_Q_MAX_DEPTH)    # 进站队列（初始化）
        self.__queue_out = Queue(MSG_Q_MAX_DEPTH)   # 出站队列（初始化）
        self.__queue_ctrl = Queue(MSG_Q_MAX_DEPTH)  # 控制队列（初始化）
        self.active = False                         # 激活状态（初始 不激活）
        self.__initialed = None                     # 已初始化模式
        self.process = None                         # 队列控制进程

    def release(self):
        if self.active:                             # 激活状态
            if self.__initialed is None:            # 未初始化模式
                self.active = False
            else:                                   # 已初始化模式
                self.__queue_ctrl.put(False)                # 控制信号（停止）
                time.sleep(BlockingTime)
                try:
                    self.process.close()                    # 进程释放（软释放）
                except ValueError:
                    self.process.kill()                     # 进程释放（硬释放）
                self.process = None
                self.__queue_in = Queue(MSG_Q_MAX_DEPTH)    # 进站队列（初始化）
                self.__queue_out = Queue(MSG_Q_MAX_DEPTH)   # 出站队列（初始化）
                self.__queue_ctrl = Queue(MSG_Q_MAX_DEPTH)  # 控制队列（初始化）
                self.__initialed = None                     # 已初始化模式（无模式，初始化）
                self.active = False                         # 激活状态（未激活，初始化）
        else:
            pass

    close = release

    def __start(self, me: str, func: callable):
        is_active = True
        if self.__initialed is not None:    # 已初始化
            self.release()
        self.__queue_ctrl.put(is_active)    # 控制信号
        self.process = Process(target=func, name="zmq.{0}".format(me),
                               args=(str(self.conn), me, self.__queue_ctrl, self.__queue_in, self.__queue_out))
        self.process.start()
        self.__initialed = me
        self.active = True

    def request(self, msg_out: MSG):        # 先出后进
        me = "REQUEST"
        if self.__initialed not in [me]:
            self.__start(me=me, func=first_out_last_in)
        if self.active:
            while self.__queue_out.full():                      # 出 / put
                time.sleep(BlockingTime)
            else:
                if isinstance(msg_out, MSG):                    # MSG 校验
                    self.__queue_out.put(msg_out)               # 输出数据
                    while self.__queue_in.empty():              # 进 / get
                        time.sleep(BlockingTime)
                    else:
                        msg_in = self.__queue_in.get()          # 输入数据
                        if isinstance(msg_in, MSG):             # MSG 校验
                            return msg_in
                        del msg_in
        else:
            self.release()
            return None

    def reply(self, func: callable):    # 先进后出
        me = "REPLY"
        if self.__initialed not in[me]:
            self.__start(me=me, func=first_in_last_out)
        if self.active:
            while self.__queue_in.empty():
                time.sleep(BlockingTime)
            else:
                msg_in = self.__queue_in.get()                  # 输入数据
                if isinstance(msg_in, MSG):                     # MSG 校验
                    msg_out = func(msg_in)
                    if isinstance(msg_out, MSG):                # MSG 校验
                        while self.__queue_out.full():
                            time.sleep(BlockingTime)
                        else:
                            self.__queue_out.put(msg_out)       # 输出数据
                    del msg_out
        else:
            self.release()

    def push(self, msg_out: MSG):       # 只出
        me = "PUSH"
        if self.__initialed not in[me]:
            self.__start(me=me, func=first_out_last_in)
        if self.active:
            # print("MessageQueue PUSH out:", msg_out)
            if isinstance(msg_out, MSG):                        # MSG 校验
                while self.__queue_out.full():
                    time.sleep(BlockingTime)
                else:
                    self.__queue_out.put(msg_out)               # 输出数据
        else:
            self.release()

    def pull(self):     # 只进
        me = "PULL"
        if self.__initialed not in[me]:
            self.__start(me=me, func=first_in_last_out)
        if self.active:
            while self.__queue_in.empty():
                time.sleep(BlockingTime)
            else:
                msg_in = self.__queue_in.get()                 # 输入数据
                # print("MessageQueue PULL IN:", msg_in)
                if isinstance(msg_in, MSG):                    # MSG 校验
                    return msg_in
                else:
                    del msg_in
        else:
            self.release()
            return None

    def publish(self, msg_out: MSG):    # 只出
        me = "PUBLISH"
        if self.__initialed not in [me]:
            self.__start(me=me, func=first_out_last_in)
        if self.active:
            if isinstance(msg_out, MSG):                    # MSG 校验
                while self.__queue_out.full():
                    time.sleep(BlockingTime)
                else:
                    self.__queue_out.put(msg_out)           # 输出数据
        else:
            self.release()

    def subscribe(self):    # 只进
        me = "SUBSCRIBE"
        if self.__initialed not in [me]:
            self.__start(me=me, func=first_in_last_out)
        if self.active:
            while self.__queue_in.empty():
                time.sleep(BlockingTime)
            else:
                msg_in = self.__queue_in.get()        # 输入数据
                if isinstance(msg_in, MSG):
                    return msg_in
                else:
                    del msg_in
        else:
            self.release()
            return None
