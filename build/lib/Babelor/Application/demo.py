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
# Outer Required
# Inner Required
from Babelor.Application import TEMPLE
from Babelor.Presentation import MSG, URL, CASE
# Global Parameters


def func_treater(msg_in: MSG):
    # -———————————————————————————————————------------------------ INIT ---------
    data_tuple = {}
    for i in range(0, msg_in.nums, 1):
        attachment = msg_in.read_datum(i)
        if attachment["path"] in data_tuple.keys():
            data_tuple[attachment["path"]] = attachment["stream"]
    # -———————————————————————————————————------------------------ PROCESS -------
    msg_out = msg_in
    # -———————————————————————————————————------------------------ END -----------
    return msg_out


def sender():
    myself = TEMPLE(URL("tcp://*:2511"))
    myself.open(role="sender")


def treater():
    myself = TEMPLE(URL("tcp://*:2511"))
    myself.open(role="treater", func=func_treater)


def receiver():
    myself = TEMPLE(URL("tcp://*:2511"))
    myself.open(role="receiver")


if __name__ == '__main__':
    sender()
    # treater()
    # receiver()
