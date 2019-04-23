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
import zmq
# Inner Required
from Message.MESSAGE import MSG, URL
from Config.CONFIG import GLOBAL_CFG
# Global Parameters
MSG_Q_MAX_DEPTH = GLOBAL_CFG["MSG_Q_MAX_DEPTH"]
CODING = GLOBAL_CFG["CODING"]
BlockingTime = GLOBAL_CFG["MSG_Q_BlockingTime"]


def consumer(socket: zmq.Socket, queue_ctrl: Queue, queue_in: Queue, queue_out: Queue):
    """
    # 消息消费者（先出后进）
    # 控制信号（启动）--> 输出队列（推出）--> 控制信号（需反馈）--> 输入队列（推入）
    :param socket: zmq.Socket   # 通讯接口
    :param queue_ctrl: Queue    # 控制队列 ("is_active", "is_response"):(bool, bool)
    :param queue_in: Queue      # 输出队列 (MSG,)
    :param queue_out: Queue     # 输入队列 (MSG,)
    :return: None
    """
    is_active, is_response = queue_ctrl.get()   # 控制信号（初始化）
    while is_active:                            # 控制信号（启动）
        if queue_ctrl.empty():                  # 控制信号（无变更），敏捷响应
            while queue_out.empty():            # 输出队列（空）
                time.sleep(BlockingTime)
            else:
                msg_out = queue_out.get()       # 输出队列（推出）
                message_out = str(msg_out).encode(CODING)
                socket.send(message_out)
            if is_response:                     # 控制信号（需反馈）
                while queue_in.full():          # 输入队列（满）
                    time.sleep(BlockingTime)
                else:
                    message_in = socket.recv()
                    msg_in = MSG(message_in.decode(CODING))
                    queue_out.put(msg_in)       # 输入队列（推入）
        else:                                   # 控制信号（变更）
            is_active, is_response = queue_ctrl.get()
    else:                                       # 队列关闭
        queue_ctrl.close()
        queue_out.close()
        queue_in.close()
        socket.close()


def producer(socket: zmq.Socket, queue_ctrl: Queue, queue_in: Queue, queue_out: Queue):
    """
    # 消息生产者（先进后出）
    # 控制信号（启动）--> 输入队列（推入）--> 控制信号（需反馈）--> 输出队列（推出）
    :param socket: zmq.Socket   # 通讯接口
    :param queue_ctrl: Queue    # 控制队列 ("is_active", "is_response"):(bool, bool)
    :param queue_in: Queue      # 输出队列 (MSG,)
    :param queue_out: Queue     # 输入队列 (MSG,)
    :return: None
    """
    is_active, is_response = queue_ctrl.get()   # 控制信号（初始化）
    while is_active:                            # 控制信号（启动）
        if queue_ctrl.empty():                  # 控制信号（无变更），敏捷响应
            while queue_in.full():              # 输入队列（空）
                time.sleep(BlockingTime)
            else:
                message_in = socket.recv()
                msg_in = MSG(message_in.decode(CODING))
                queue_in.put(msg_in)            # 输入队列（推入）
            if is_response:                     # 控制信号（需反馈）
                while queue_out.empty():        # 输出队列（空）
                    time.sleep(BlockingTime)
                else:
                    msg_out = queue_out.get()   # 输出队列（推出）
                    message_out = str(msg_out).encode(CODING)
                    socket.send(message_out)
        else:                                   # 控制信号（变更）
            is_active, is_response = queue_ctrl.get()
    else:                                       # 队列关闭
        queue_ctrl.close()
        queue_out.close()
        queue_in.close()
        socket.close()


class MessageQueue:
    def __init__(self, conn: URL):
        if isinstance(conn, str):
            self.__conn = URL(conn)                 # 套接字
        else:
            self.__conn = conn
        if self.__conn.scheme not in ["tcp"]:       # 协议（校验）
            raise ValueError("Invalid scheme{0}.".format(self.__conn.scheme))
        self.__conn = self.__dict__["conn"].check   # 默认端口（校验）
        self.__queue_in = Queue(MSG_Q_MAX_DEPTH)    # 进站队列（初始化）
        self.__queue_out = Queue(MSG_Q_MAX_DEPTH)   # 出站队列（初始化）
        self.__queue_ctrl = Queue(MSG_Q_MAX_DEPTH)  # 控制队列（初始化）
        self.__active = None                        # 已激活模式
        self.__initialed = None                     # 已初始化模式
        self.__process = None                       # 队列控制进程
        self.__context = zmq.Context()              # GPL 消息库
        self.__socket = None                        # 通讯接口

    def release(self):
        if self.__active is not None and self.__initialed is not None:
            if isinstance(self.__dict__["__process"], Process):         # 已分配进程
                self.__dict__["__queue_ctrl"].put((False, False))       # 控制信号（停止）
                time.sleep(BlockingTime)
                try:
                    self.__dict__["__process"].close()                  # 进程释放（软释放）
                except ValueError:
                    self.__dict__["__process"].kill()                   # 进程释放（硬释放）
                self.__dict__["__process"] = None
                self.__dict__["__queue_in"] = Queue(MSG_Q_MAX_DEPTH)    # 进站队列（初始化）
                self.__dict__["__queue_out"] = Queue(MSG_Q_MAX_DEPTH)   # 出站队列（初始化）
                self.__dict__["__queue_ctrl"] = Queue(MSG_Q_MAX_DEPTH)  # 控制队列（初始化）
                self.__dict__["socket"] = None                          # 通讯接口（释放）
                self.__dict__["__initialed"] = None                     # 已初始化模式（无模式）
                self.__dict__["__active"] = None                        # 已激活模式（无模式）
            else:                                                       # 未分配进程
                self.__dict__["__process"] = None                       # 队列控制进程（初始化）
                self.__dict__["socket"] = None                          # 通讯接口（初始化）
                self.__dict__["__initialed"] = None                     # 已初始化模式（初始化）
                self.__dict__["__active"] = None                        # 已激活模式（初始化）
        return self

    def __initial(self, is_active: bool, is_response: bool, me: str, func):
        self.__dict__["__queue_ctrl"].put((is_active, is_response))  # 控制信号
        self.__dict__["__process"] = Process(target=func,
                                             args=(self.__dict__["__socket"], self.__dict__["__queue_ctrl"],
                                                   self.__dict__["__queue_in"], self.__dict__["__queue_out"]))
        self.__dict__["__process"].start()
        self.__dict__["__initialed"] = me
        self.__dict__["__active"] = me

    def request(self, msg: MSG):
        me = "REQUEST"      # 先出后进
        if self.__dict__["__initialed"] not in [me]:
            self.release()
            self.__dict__["__socket"] = self.__dict__["__context"].socket(zmq.REQ)
            self.__dict__["__socket"].connect(self.__dict__["__conn"].to_string(False, False, False, False))
            self.__initial(True, True, me, consumer)
        if self.__dict__["__active"] in [me]:
            while self.__dict__["__queue_in"].empty():
                time.sleep(BlockingTime)
            else:
                self.__dict__["__queue_out"].put(msg)           # 输出数据
                return self.__dict__["__queue_in"].get()        # 输入数据
        else:
            self.release()
            return None

    def reply(self, func: callable):
        me = "REPLY"        # 先进后出
        if self.__dict__["__initialed"] not in[me]:
            self.release()
            self.__dict__["__socket"] = self.__dict__["__context"].socket(zmq.REP)
            self.__dict__["__socket"].bind(self.__dict__["__conn"].to_string(False, False, False, False))
            self.__initial(True, True, me, producer)
        if self.__dict__["__active"] in [me]:
            while self.__dict__["__queue_in"].empty():
                time.sleep(BlockingTime)
            else:
                msg = self.__dict__["__queue_in"].get()         # 输入数据
                self.__dict__["__queue_out"].put(func(msg))     # 输出数据
        else:
            self.release()

    def push(self, msg: MSG):
        me = "PUSH"         # 只出
        if self.__dict__["__initialed"] not in [me]:
            self.release()
            self.__dict__["__socket"] = self.__dict__["__context"].socket(zmq.PUSH)
            self.__dict__["__socket"].connect(self.__dict__["__conn"].to_string(False, False, False, False))
            self.__initial(True, False, me, producer)
        if self.__dict__["__active"] in [me]:
            while self.__dict__["__queue_out"].full():
                time.sleep(BlockingTime)
            else:
                self.__dict__["__queue_out"].put(msg)           # 输出数据
        else:
            self.release()

    def pull(self):
        me = "PULL"         # 只进
        if self.__dict__["__initialed"] not in [me]:
            self.release()
            self.__dict__["__socket"] = self.__dict__["__context"].socket(zmq.PULL)
            self.__dict__["__socket"].bind(self.__dict__["__conn"].to_string(False, False, False, False))
            self.__initial(True, False, me, consumer)
        if self.__dict__["__active"] in [me]:
            while self.__dict__["__queue_in"].empty():
                time.sleep(BlockingTime)
            else:
                return self.__dict__["__queue_in"].get()        # 输入数据
        else:
            self.release()
            return None

    def publish(self, msg: MSG):
        me = "PUBLISH"      # 只出
        if self.__dict__["__initialed"] not in [me]:
            self.release()
            self.__dict__["__socket"] = self.__dict__["__context"].socket(zmq.PUB)
            self.__dict__["__socket"].bind(self.__dict__["__conn"].to_string(False, False, False, False))
            self.__initial(True, False, me, producer)
        if self.__dict__["__active"] in [me]:
            while self.__dict__["__queue_out"].full():
                time.sleep(BlockingTime)
            else:
                self.__dict__["__queue_out"].put(msg)           # 输出数据
        else:
            self.release()

    def subscribe(self):
        me = "SUBSCRIBE"    # 只进
        if self.__dict__["__initialed"] not in [me]:
            self.release()
            self.__dict__["__socket"] = self.__dict__["__context"].socket(zmq.SUB)
            self.__dict__["__socket"].connect(self.__dict__["__conn"].to_string(False, False, False, False))
            self.__dict__["__socket"].setsockopt(zmq.SUBSCRIBE, '')     # 订阅
            self.__initial(True, False, me, consumer)
        if self.__dict__["__active"] in [me]:
            while self.__dict__["__queue_in"].empty():
                time.sleep(BlockingTime)
            else:
                return self.__dict__["__queue_in"].get()        # 输入数据
        else:
            self.release()
            return None
