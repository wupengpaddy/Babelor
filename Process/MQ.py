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
from multiprocessing import Queue
from Tools.Conversion import json2dict
from Message.Message import MSG


MSG_Q_MAX_DEPTH = 100
CODING = "utf-8"
BlockingTime = 1/1024


class MessageQueue:
    """
    MessageQueue Model
    """
    def __init__(self, sender=None, receiver=None):
        self.context = zmq.Context()
        # self.sender = "tcp://127.0.0.1:10011"
        self.sender = sender
        self.receiver = receiver

    def sender_request(self, msg: MSG):
        if self.receiver is None:
            raise ValueError
        socket = self.context.socket(zmq.REQ)
        socket.connect(self.receiver)
        message = str(msg)
        socket.send(message.encode(CODING))
        reply_msg = MSG(socket.recv())
        return reply_msg

    def receiver_reply(self, func):
        if self.sender is None:
            raise ValueError
        socket = self.context.socket(zmq.REP)
        socket.bind(self.sender)
        while True:
            message = socket.recv()
            msg = MSG(json2dict(message.decode(CODING)))
            reply_msg = func(msg)
            socket.send(reply_msg).encode(CODING)

    def sender_push(self, msg_q_out: Queue):
        if self.receiver is None:
            raise ValueError
        socket = self.context.socket(zmq.PUSH)
        socket.connect(self.receiver)
        while True:
            if msg_q_out.empty():
                time.sleep(BlockingTime)
            else:
                msg = msg_q_out.get()
                message = str(msg)
                socket.send(message.encode(CODING))
            pass

    def receiver_pull(self, msg_q_in: Queue):
        if self.sender is None:
            raise ValueError
        socket = self.context.socket(zmq.PULL)
        socket.bind(self.sender)
        while True:
            message = socket.recv()
            msg = MSG(json2dict(message.decode(CODING)))
            msg_q_in.put(msg)
