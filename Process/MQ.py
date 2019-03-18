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
import zmq
import time
from multiprocessing import Queue, Process
from Tools.Conversion import json2dict
from Message.Message import MSG, URL


MSG_Q_MAX_DEPTH = 100
CODING = "utf-8"
BlockingTime = 1/1024


def sender_request(receiver: str, msg: MSG):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(str(receiver))
    socket.send(str(msg).encode(CODING))
    reply_msg = MSG(socket.recv())
    return reply_msg


def receiver_reply(receiver: str, func, has_next: int):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(str(receiver))
    while True:
        if has_next == 0:
            return
        msg = MSG(json2dict(socket.recv().decode(CODING)))
        reply_msg = func(msg)
        socket.send(reply_msg).encode(CODING)
        has_next -= 1


def sender_push(receiver: URL, msg_queue: Queue):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(receiver)
    while True:
        if msg_queue.empty():
            time.sleep(BlockingTime)
        else:
            msg, has_next = msg_queue.get()
            if has_next == 0:
                return
            message = str(msg)
            socket.send(message.encode(CODING))
            has_next -= 1
        pass


def receiver_pull(receiver: URL, msg_queue: Queue, ctrl_queue: Queue):
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.bind(receiver)
    has_next = 1
    while True:
        if not ctrl_queue.empty():
            has_next = ctrl_queue.get()
            if has_next == 0:
                return
        if msg_queue.full():
            time.sleep(BlockingTime)
        else:
            message = socket.recv()
            msg = MSG(json2dict(message.decode(CODING)))
            msg_queue.put(msg)
            has_next -= 1


class MessageQueue:
    """
    MessageQueue Model
    """
    def __init__(self, msg: MSG):
        self.queue = Queue(MSG_Q_MAX_DEPTH)
        self.ctrl_queue = Queue(MSG_Q_MAX_DEPTH)
        self.origination = str(msg.origination)
        self.destination = str(msg.destination)
        self.is_init = {
            "PUSH": False,
            "PULL": False,
            "PUB": False,
            "SUB": False,
        }

    def request(self, msg: MSG):
        return sender_request(self.destination, msg)

    def reply(self, func, has_next=-1):
        return receiver_reply(self.origination, func, has_next)

    def push(self, msg: MSG, has_next=1):
        if not self.is_init["PUSH"]:
            push_process = Process(target=sender_push, args=(self.destination, self.queue))
            push_process.start()
        self.queue.put((msg, has_next))

    def pull(self, has_next=-1):
        if not self.is_init["PULL"]:
            pull_process = Process(target=receiver_pull, args=(self.origination, self.queue, self.ctrl_queue))
            pull_process.start()
        self.ctrl_queue.put((has_next, ))
        while True:
            if self.queue.empty():
                time.sleep(BlockingTime)
            else:
                return self.queue.get()
