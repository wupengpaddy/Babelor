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
from Message.Message import MSG, URL
from CONFIG.config import GLOBAL_CFG
# Global Parameters
MSG_Q_MAX_DEPTH = GLOBAL_CFG["MSG_Q_MAX_DEPTH"]
CODING = GLOBAL_CFG["CODING"]
BlockingTime = GLOBAL_CFG["MSG_Q_BlockingTime"]


def consumer_request(socket: zmq.Socket, msg: MSG):
    socket.send(str(msg).encode(CODING))
    reply_msg = MSG(socket.recv())
    return reply_msg


def producer_reply(socket: zmq.Socket, ctrl_queue: Queue, func):
    while True:
        is_break = False                                        # Control Flow
        if not ctrl_queue.empty():
            is_break = ctrl_queue.get()
        if is_break:
            return
        msg = MSG(socket.recv().decode(CODING))                 # Data Flow
        reply_msg = func(msg)
        socket.send(reply_msg).encode(CODING)


def producer_push(socket: zmq.Socket, ctrl_queue: Queue, msg_queue: Queue):
    while True:
        is_break = False                                        # Control Flow
        if not ctrl_queue.empty():
            is_break = ctrl_queue.get()
        if is_break:
            return
        if msg_queue.empty():                                   # Data Flow
            time.sleep(BlockingTime)
        else:
            message = str(msg_queue.get())
            socket.send(message.encode(CODING))


def consumer(socket: zmq.Socket, queue_ctrl: Queue = None, queue_in: Queue = None, queue_out: Queue = None):
    is_break, is_response = queue_ctrl.get()
    while is_break:  # Control Flow
        if queue_ctrl.empty():  # 敏捷响应
            while queue_in.full():
                time.sleep(BlockingTime)
            else:
                message = socket.recv().decode(CODING)  # 消息接收
                message = MSG(message)
                queue_in.put(message)
            if is_response:
                while queue_out.empty():
                    time.sleep(BlockingTime)
                else:
                    message = socket.recv().encode(CODING)  # 消息反馈

        else:
            is_break, is_response = queue_ctrl.get()
    else:
        queue_ctrl.close()
        queue_in.close()
        queue_out.close()
        socket.close()


def producer(socket: zmq.Socket, queue_ctrl: Queue = None, queue_out: Queue = None):
    is_break = True
    while is_break:     # Control Flow
        if queue_ctrl.empty():      # 敏捷响应
            while queue_out.empty():
                time.sleep(BlockingTime)
            else:                   # Data Flow
                message = queue_out.get()
                if isinstance(message, MSG):
                    message = str(message)
                socket.send(message.encode(CODING))     # 消息发送
        else:
            is_break = queue_ctrl.get()
    else:
        queue_ctrl.close()
        queue_out.close()
        socket.close()


class MessageQueue:
    def __init__(self, conn: URL):
        if isinstance(conn, str):
            self.__conn = URL(conn)     # 连接套接字符串
        else:
            self.__conn = conn
        if self.__conn.scheme not in ["tcp"]:       # 协议校验
            raise ValueError("Invalid scheme{0}.".format(self.__conn.scheme))
        self.__conn = self.__dict__["conn"].check       # 默认端口校验
        self.__queue_input = Queue(MSG_Q_MAX_DEPTH)     # 进站队列
        self.__queue_output = Queue(MSG_Q_MAX_DEPTH)    # 出站队列
        self.__queue_ctrl = Queue(MSG_Q_MAX_DEPTH)      # 控制队列
        self.__active = None                            # 已激活模式
        self.__initialed = None                         # 已初始化模式
        self.__process = None                           # 进程
        self.__context = zmq.Context()                  # GPL 消息库
        self.__socket = None                            # 通讯接口

    def release_queue(self):
        if self.__active is not None and self.__initialed is not None:
            self.__dict__["__queue_ctrl"].put(False)        # 进程内部控制关闭
            time.sleep(BlockingTime)
            try:
                self.__dict__["__process"].close()          # 进程资源释放
            except ValueError:
                self.__dict__["__process"].kill()           # 进程关闭
            self.__dict__["__process"] = None               # 进程释放
            self.__dict__["__queue_input"].close()          # 进站队列关闭
            self.__dict__["__queue_output"].close()         # 出站队列关闭
            self.__dict__["__queue_ctrl"].close()           # 控制队列关闭
            self.__dict__["__queue_input"] = Queue(MSG_Q_MAX_DEPTH)     # 进站队列重建
            self.__dict__["__queue_output"] = Queue(MSG_Q_MAX_DEPTH)    # 出站队列重建
            self.__dict__["__queue_ctrl"] = Queue(MSG_Q_MAX_DEPTH)      # 控制队列重建
            self.__dict__["socket"] = None                  # 通讯接口释放
            self.__dict__["__initialed"] = None             # 已初始化模式清空
            self.__dict__["__active"] = None                # 已激活模式清空

    def request(self, msg: MSG):
        if self.__initialed not in ["REQUEST"]:
            self.__dict__["__socket"] = self.__dict__["__context"].socket(zmq.REQ)
            self.__dict__["__socket"].connect(self.__dict__["__conn"].to_string(False, False, False, False))
            self.__dict__["__initialed"] = "REQUEST"
            self.__dict__["__active"] = "REQUEST"
        if self.__dict__["__active"] in ["REQUEST"]:
            return consumer_request(self.__dict__["__socket"], msg)
        else:
            self.__dict__["__socket"] = None
            self.__dict__["__initialed"] = None
            self.__dict__["__active"] = None

    def reply(self, func: classmethod):
        if self.__initialed not in["REPLY"]:
            self.__dict__["__socket"] = self.__dict__["__context"].socket(zmq.REP)
            self.__dict__["__socket"].bind(self.__dict__["__conn"].to_string(False, False, False, False))
            self.__dict__["__process"] = Process(target=producer_reply, args=(self.__dict__["__socket"],
                                                                              self.__dict__["__queue_ctrl"], func))
            self.__dict__["__process"].start()
            self.__dict__["__initialed"] = "REPLY"
            self.__dict__["__active"] = "REPLY"
        if self.__dict__["__active"] in ["REPLY"]:
            pass
        else:
            self.__dict__["__ctrl"].put(False)
            self.__dict__["__process"].close()
            self.__dict__["__process"] = None
            self.__dict__["socket"] = None
            self.__dict__["__initialed"] = None
            self.__dict__["active"] = None

    def push(self, msg: MSG = None):
        if self.__initialed not in ["PUSH"]:
            self.__dict__["__socket"] = self.__dict__["__context"].socket(zmq.PUSH)
            self.__dict__["__socket"].connect(self.__dict__["__conn"].to_string(False, False, False, False))
            self.__dict__["__process"] = Process(target=producer_push, args=(self.__dict__["__socket"],
                                                                             self.__dict__["__ctrl"],
                                                                             self.__dict__["__queue"]))
            self.__dict__["__process"].start()
            self.__dict__["__initialed"] = "PUSH"
            self.__dict__["active"] = "PUSH"
        self.__dict__["__queue"].put(msg)
        if self.__dict__["__active"] in ["PUSH"]:
            pass
        else:
            self.__dict__["__ctrl"].put(False)
            self.__dict__["__process"].close()
            self.__dict__["__process"] = None
            self.__dict__["socket"] = None
            self.__dict__["__initialed"] = None
            self.__dict__["active"] = None

    def pull(self, is_break=False):
        if not self.is_not_init["PULL"]:
            self.socket = self.context.socket(zmq.PULL)
            self.socket.bind(self.__conn.to_string(False, False, False, False))
            pull_process = Process(target=consumer_pull, args=(self.socket, self.ctrl_queue, self.msg_queue))
            pull_process.start()
            self.is_not_init["PULL"] = False
        if is_break:
            self.ctrl_queue.put(is_break)
            self.is_not_init["PULL"] = True
            return None
        while True:
            if self.msg_queue.empty():
                time.sleep(BlockingTime)
            else:
                return self.msg_queue.get()

    def publish(self, msg: MSG, is_break=False):
        if not self.is_not_init["PUBLISH"]:
            self.socket = self.context.socket(zmq.PUB)
            self.socket.bind(self.__conn.to_string(False, False, False, False))
            publish_process = Process(target=producer_publish, args=(self.socket, self.ctrl_queue, self.msg_queue))
            publish_process.start()
            self.is_not_init["PUBLISH"] = False
        self.msg_queue.put(msg)
        if is_break:
            self.ctrl_queue.put(is_break)
            self.is_not_init["PUBLISH"] = True
            return None

    def subscribe(self, is_break=False):
        if not self.is_not_init["SUBSCRIBE"]:
            self.socket = self.context.socket(zmq.SUB)
            self.socket.connect(self.__conn.to_string(False, False, False, False))
            self.socket.setsockopt(zmq.SUBSCRIBE, '')
            subscribe_process = Process(target=consumer_subscribe, args=(self.socket, self.ctrl_queue, self.msg_queue))
            subscribe_process.start()
            self.is_not_init["SUBSCRIBE"] = False
        if is_break:
            self.ctrl_queue.put(is_break)
            self.is_not_init["SUBSCRIBE"] = True
            return None
        while True:
            if self.msg_queue.empty():
                time.sleep(BlockingTime)
            else:
                return self.msg_queue.get()
