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
from multiprocessing import Process
# Outer Required
# Inner Required
from Babelor.Session import MQ
from Babelor.Presentation import MSG, URL
# Global Parameters


def try_push():
    msg = MSG()
    msg.origination = URL("tcp://*:10001")
    msg.destination = URL("tcp://127.0.0.1:10001")
    mq = MQ("tcp://127.0.0.1:10001")
    for i in range(0, 100, 1):
        # print("push msg:", msg)
        msg.activity = str(i)
        # print("push msg:", i, msg)
        mq.push(msg)
        time.sleep(0.001)


def try_pull():
    mq = MQ("tcp://*:10001")
    for i in range(0, 100, 1):
        msg = mq.pull()
        # print("pull msg:", i, msg)


def demo_push_pull():
    process = Process(target=try_pull)
    process.start()
    time.sleep(1)
    try_push()


def try_request():
    msg = MSG()
    msg.origination = URL("tcp://127.0.0.1:10001")
    mq = MQ(URL("tcp://127.0.0.1:10001"))
    print("request msg:", msg)
    msg = mq.request(msg)
    print("reply msg:", msg)


def try_reply_func(msg: MSG):
    msg.destination = URL().init("oracle")
    return msg


def try_reply():
    mq = MQ(URL("tcp://*:10001"))
    mq .reply(try_reply_func)


def demo_request_reply():
    thread = Thread(target=try_reply)
    thread.start()
    try_request()


def try_publish():
    msg = MSG()
    msg.origination = URL("tcp://127.0.0.1:10001")
    mq = MQ(URL("tcp://*:10001"))
    print("publish msg:", msg)
    mq.publish(msg)


def try_subscribe():
    mq = MQ(URL("tcp://127.0.0.1:10001"))
    msg = mq.subscribe()
    print("subscribe msg:", msg)


def demo_publish_subscribe():
    thread = Thread(target=try_publish)
    thread.start()
    time.sleep(2)
    try_subscribe()


if __name__ == '__main__':
    demo_push_pull()
    # demo_request_reply()
    # demo_publish_subscribe()
