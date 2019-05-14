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
import logging
import sched
import time
from multiprocessing import Process
# Outer Required
# Inner Required
from Babelor.Application import TEMPLE
from Babelor.Presentation import MSG, URL, CASE
from Babelor.Session import MQ
# Global Parameters
# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')


def func_sender(msg: MSG):
    # -—————————————------------------------ INIT ---------
    arguments = {}
    for i in range(0, msg.dt_count, 1):
        argument = msg.read_datum(i)
        if argument["path"] not in arguments.keys():
            arguments[argument["path"]] = argument["stream"]
    # -—————————————------------------------ PROCESS ------
    msg_out = msg
    # -—————————————------------------------ END ----------
    return msg_out


def func_treater(msg: MSG):
    # -—————————————------------------------ INIT ---------
    data = {}
    for i in range(0, msg.dt_count, 1):
        datum = msg.read_datum(i)
        if datum["path"] not in data.keys():
            data[datum["path"]] = datum["stream"]
    # -—————————————------------------------ PROCESS ------
    msg_out = msg
    # -—————————————------------------------ END ----------
    return msg_out


def func_encrypter(msg: MSG):
    # -————————————------------------------ INIT ---------
    data = {}
    for i in range(0, msg.dt_count, 1):
        datum = msg.read_datum(i)
        if datum["path"] not in data.keys():
            data[datum["path"]] = datum["stream"]
    # -————————————------------------------ PROCESS ------
    msg_out = msg
    # -————————————------------------------ END ----------
    return msg_out


def sender(url):
    myself = TEMPLE(url)
    myself.open(role="sender")


def treater(url):
    myself = TEMPLE(url)
    myself.open(role="treater", func=func_treater)


def encrypter(url):
    myself = TEMPLE(url)
    myself.open(role="encrypter", func=func_encrypter)


def receiver(url):
    myself = TEMPLE(url)
    myself.open(role="receiver")


def receiver_init():
    # -————————————------------------------ MESSAGE -----
    case = CASE("{0}#{1}".format(origination_url, destination_url))
    receiver_msg = MSG()
    receiver_msg.case = case
    # -————————————------------------------ RECEIVER ----
    receiver_msg.origination = edge_node_url["inner"]
    receiver_msg.destination = destination_url
    # logging.warning("RECEIVER::INIT::{0} send:{1}".format(receiver_url["inner"], receiver_msg))
    recv_init = MQ(receiver_url["outer"])
    recv_init.push(receiver_msg)


def sender_init():
    # -————————————------------------------ MESSAGE -----
    case = CASE("{0}#{1}".format(origination_url, destination_url))
    sender_msg = MSG()
    sender_msg.case = case
    # -————————————------------------------ SENDER ------
    sender_msg.origination = origination_url
    sender_msg.destination = edge_node_url["outer"]
    sender_msg.add_datum(None, "20190505.xlsx")
    sender_msg.add_datum(None, "20190506.xlsx")
    sender_msg.add_datum(None, "20190507.xlsx")
    send_init = MQ(sender_url["outer"])
    send_init.push(sender_msg)


def main():
    # -————————————------------------------ PROCESS ------
    temple = {
        # "treater": Process(target=treater, args=(treater_url["inner"],)),
        # "encrypter": Process(target=encrypter, args=(encrypter_url["inner"],)),
        "receiver": Process(target=receiver, args=(receiver_url["inner"],)),
        "receiver_init": Process(target=receiver_init),
        "sender": Process(target=sender, args=(sender_url["inner"],)),
        "sender_init": Process(target=sender_init),
    }
    # for obj in temple.items():
    #     key, value = obj
    #     value.start()
    temple["receiver"].start()
    temple["sender"].start()
    temple["receiver_init"].start()
    temple["sender_init"].start()


sender_url = {
    "inner": URL("tcp://*:20001"),
    "outer": URL("tcp://127.0.0.1:20001"),
}
treater_url = {
    "inner": URL("tcp://*:20002"),
    "outer": URL("tcp://127.0.0.1:20002"),
}
encrypter_url = {
    "inner": URL("tcp://*:20003"),
    "outer": URL("tcp://127.0.0.1:20003"),
}
receiver_url = {
    "inner": URL("tcp://*:20004"),
    "outer": URL("tcp://127.0.0.1:20004"),
}
edge_node_url = {
    "inner": URL("tcp://*:20005"),
    "outer": URL("tcp://127.0.0.1:20005"),
}
origination_url = URL("file:///C:/Users/geyua/PycharmProjects/Babelor/data/dir1/")
destination_url = URL("file:///C:/Users/geyua/PycharmProjects/Babelor/data/dir2/")


if __name__ == '__main__':
    main()

