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
from threading import Thread
# Outer Required
# Inner Required
from Babelor.Session import MessageQueue
from Babelor.Presentation import MSG, URL
# Global Parameters


def try_push():
    msg = MSG()
    msg.origination = URL("tcp://127.0.0.1:10001")
    mq = MessageQueue(URL("tcp://127.0.0.1:10001"))
    print("push msg:", msg)
    mq.push(msg)


def try_pull():
    mq = MessageQueue(URL("tcp://*:10001"))
    msg = mq.pull()
    print("pull msg:", msg)


def test_push_pull():
    thread = Thread(target=try_pull)
    thread.start()
    try_push()


def try_request():
    msg = MSG()
    msg.origination = URL("tcp://127.0.0.1:10001")
    mq = MessageQueue(URL("tcp://127.0.0.1:10001"))
    print("request msg:", msg)
    msg = mq.request(msg)
    print("reply msg:", msg)


def try_reply_func(msg: MSG):
    msg.destination = URL().init("oracle")
    return msg


def try_reply():
    mq = MessageQueue(URL("tcp://*:10001"))
    mq .reply(try_reply_func)


def test_request_reply():
    thread = Thread(target=try_reply)
    thread.start()
    try_request()


def try_publish():
    msg = MSG()
    msg.origination = URL("tcp://127.0.0.1:10001")
    mq = MessageQueue(URL("tcp://*:10001"))
    print("publish msg:", msg)
    mq.publish(msg)


def try_subscribe():
    mq = MessageQueue(URL("tcp://127.0.0.1:10001"))
    msg = mq.subscribe()
    print("subscribe msg:", msg)


def test_publish_subscribe():
    thread = Thread(target=try_publish)
    thread.start()
    time.sleep(2)
    try_subscribe()


if __name__ == '__main__':
    # test_push_pull()
    # test_request_reply()
    test_publish_subscribe()
