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
from multiprocessing import Process
# Outer Required
# Inner Required
from Babelor.Application import TEMPLE
from Babelor.Presentation import MSG, URL, CASE
from Babelor.Session import MQ
# Global Parameters
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def func_treater(msg_in: MSG):
    # -—————————————------------------------ INIT ---------
    dt = {}
    for i in range(0, msg_in.nums, 1):
        attachment = msg_in.read_datum(i)
        if attachment["path"] in dt.keys():
            dt[attachment["path"]] = attachment["stream"]
    # -—————————————------------------------ PROCESS ------
    msg_out = msg_in
    # -—————————————------------------------ END ----------
    return msg_out


def func_encrypter(msg_in: MSG):
    # -————————————------------------------ INIT ---------
    dt = {}
    for i in range(0, msg_in.nums, 1):
        attachment = msg_in.read_datum(i)
        if attachment["path"] in dt.keys():
            dt[attachment["path"]] = attachment["stream"]
    # -————————————------------------------ PROCESS ------
    msg_out = msg_in
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


def main():
    # -————————————------------------------ TEMPLE -------
    sender_url = {
        "inner": URL("tcp://*:10001"),
        "outer": URL("tcp://127.0.0.1:10001"),
    }
    treater_url = {
        "inner": URL("tcp://*:10002"),
        "outer": URL("tcp://127.0.0.1:10002"),
    }
    encrypter_url = {
        "inner": URL("tcp://*:10003"),
        "outer": URL("tcp://127.0.0.1:10003"),
    }
    receiver_url = {
        "inner": URL("tcp://*:10004"),
        "outer": URL("tcp://127.0.0.1:10004"),
    }
    edge_node_url = {
        "inner": URL("tcp://*:10005"),
        "outer": URL("tcp://127.0.0.1:10005"),
    }
    origination_url = URL("file://docs")
    destination_url = URL("file://new_docs")
    # -————————————------------------------ PROCESS ------
    temple = {
        "sender": Process(target=sender, args=(sender_url["inner"],)),
        # "treater": Process(target=treater, args=(treater_url["inner"],)),
        # "encrypter": Process(target=encrypter, args=(encrypter_url["inner"],)),
        "receiver": Process(target=receiver, args=(receiver_url["inner"],)),
    }
    for obj in temple.items():
        key, value = obj
        value.start()
    # -————————————------------------------ MESSAGE -----
    case = CASE("{0}#{1}".format(origination_url, destination_url))
    msg = MSG()
    msg.case = case
    # -————————————------------------------ RECEIVER ----
    receiver_msg = msg
    receiver_msg.origination = edge_node_url["inner"]
    receiver_msg.destination = destination_url
    logging.warning("RECEIVER::INIT::{0} send:{1}".format(receiver_url["inner"], receiver_msg))
    time.sleep(10)
    receiver_init = MQ(receiver_url["outer"])
    receiver_init.push(receiver_msg)
    receiver_init.close()
    # -————————————------------------------ SENDER ------
    # sender_msg = msg
    # sender_msg.origination = origination_url
    # sender_msg.destination = edge_node_url["outer"]
    # sender_msg.add_datum(None, "Babelor-2019-Data-Structure.vsdx")
    # sender_msg.add_datum(None, "Babelor-2019-Protocol.vsdx")
    # print("init sender:", sender_msg)
    # sender_start = MQ(sender_url["outer"])
    # sender_start.push(sender_msg)


if __name__ == '__main__':
    main()
