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

# 外部依赖
import zmq
import time
from multiprocessing import Queue, Process
# 内部依赖
from Message.Message import MSG, URL
from CONFIG.config import GLOBAL_CFG
# 全局参数
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


def consumer_pull(socket: zmq.Socket, ctrl_queue: Queue, msg_queue: Queue):
    while True:
        is_break = False                                        # Control Flow
        if not ctrl_queue.empty():
            is_break = ctrl_queue.get()
        if is_break:
            return
        if msg_queue.full():                                    # Data Flow
            time.sleep(BlockingTime)
        else:
            message = socket.recv()
            msg_queue.put(MSG(message.decode(CODING)))


def producer_publish(socket: zmq.Socket, ctrl_queue: Queue, msg_queue: Queue):
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


def consumer_subscribe(socket: zmq.Socket, ctrl_queue: Queue, msg_queue: Queue):
    while True:
        is_break = False                                        # Control Flow
        if not ctrl_queue.empty():
            is_break = ctrl_queue.get()
        if is_break:
            return
        if msg_queue.full():                                    # Data Flow
            time.sleep(BlockingTime)
        else:
            message = socket.recv()
            msg_queue.put(MSG(message.decode(CODING)))


class MessageQueue:
    """
    MessageQueue Model
    """
    def __init__(self, msg: MSG):
        self.msg_queue = Queue(MSG_Q_MAX_DEPTH)
        self.ctrl_queue = Queue(MSG_Q_MAX_DEPTH)
        self.origination = msg.origination
        self.destination = msg.destination
        self.is_not_init = {
            "REQUEST": True,
            "REPLY": True,
            "PUSH": True,
            "PULL": True,
            "PUBLISH": True,
            "SUBSCRIBE": True,
        }
        self.context = zmq.Context()
        self.socket = None

    def request(self, msg: MSG, is_break=False):
        if not isinstance(self.destination, URL):
            raise ValueError("Invalid destination path{0} or type:{1}.".format(self.destination,
                                                                               type(self.destination)))
        if self.destination.scheme not in ["tcp"]:
            raise ValueError("Invalid destination scheme{0}.".format(self.destination.scheme))
        if self.is_not_init["REQUEST"]:
            self.socket = self.context.socket(zmq.REQ)
            self.socket.connect(self.destination.to_string(False, False, False, False))
            self.is_not_init["REQUEST"] = False
        if is_break:
            self.is_not_init["REQUEST"] = True
        return consumer_request(self.socket, msg)

    def reply(self, func, is_break=False):
        if not isinstance(self.origination, URL):
            raise ValueError("Invalid origination path{0} or type:{1}.".format(self.origination,
                                                                               type(self.origination)))
        if self.is_not_init["REPLY"]:
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.origination.to_string(False, False, False, False))
            reply_process = Process(target=producer_reply, args=(self.socket, self.ctrl_queue, func))
            reply_process.start()
            self.is_not_init["REPLY"] = False
        if is_break:
            self.ctrl_queue.put(is_break)
            self.is_not_init["REPLY"] = True

    def push(self, msg: MSG, is_break=False):
        if not isinstance(self.destination, URL):
            raise ValueError("Invalid destination path {0} or type:{1}.".format(self.destination,
                                                                                type(self.destination)))
        if self.is_not_init["PUSH"]:
            self.socket = self.context.socket(zmq.PUSH)
            self.socket.connect(self.destination.to_string(False, False, False, False))
            push_process = Process(target=producer_push, args=(self.socket, self.ctrl_queue, self.msg_queue))
            push_process.start()
            self.is_not_init["PUSH"] = False
        self.msg_queue.put(msg)
        if is_break:
            self.ctrl_queue.put(is_break)
            self.is_not_init["PUSH"] = True

    def pull(self, is_break=False):
        if not isinstance(self.origination, URL):
            raise ValueError("Invalid origination path{0} or type:{1}.".format(self.origination,
                                                                               type(self.origination)))
        if not self.is_not_init["PULL"]:
            self.socket = self.context.socket(zmq.PULL)
            self.socket.bind(self.origination.to_string(False, False, False, False))
            pull_process = Process(target=consumer_pull, args=(self.socket, self.ctrl_queue, self.msg_queue))
            pull_process.start()
            self.is_not_init["PULL"] = False
        if is_break:
            self.ctrl_queue.put(is_break)
            self.is_not_init["PULL"] = True
            return MSG
        while True:
            if self.msg_queue.empty():
                time.sleep(BlockingTime)
            else:
                return self.msg_queue.get()

    def publish(self, msg: MSG, is_break=False):
        if not isinstance(self.origination, URL):
            raise ValueError("Invalid origination path{0} or type:{1}.".format(self.origination,
                                                                               type(self.origination)))
        if not self.is_not_init["PUBLISH"]:
            self.socket = self.context.socket(zmq.PUB)
            self.socket.bind(self.origination.to_string(False, False, False, False))
            publish_process = Process(target=producer_publish, args=(self.socket, self.ctrl_queue, self.msg_queue))
            publish_process.start()
            self.is_not_init["PUBLISH"] = False
        self.msg_queue.put(msg)
        if is_break:
            self.ctrl_queue.put(is_break)
            self.is_not_init["PUBLISH"] = True

    def subscribe(self, is_break=False):
        if not isinstance(self.destination, URL):
            raise ValueError("Invalid destination path{0} or type:{1}.".format(self.destination,
                                                                               type(self.destination)))
        if not self.is_not_init["SUBSCRIBE"]:
            self.socket = self.context.socket(zmq.SUB)
            self.socket.connect(self.destination.to_string(False, False, False, False))
            self.socket.setsockopt(zmq.SUBSCRIBE, '')
            subscribe_process = Process(target=consumer_subscribe, args=(self.socket, self.ctrl_queue, self.msg_queue))
            subscribe_process.start()
            self.is_not_init["SUBSCRIBE"] = False
        if is_break:
            self.ctrl_queue.put(is_break)
            self.is_not_init["SUBSCRIBE"] = True
            return MSG
        while True:
            if self.msg_queue.empty():
                time.sleep(BlockingTime)
            else:
                return self.msg_queue.get()
