# coding=utf-8
# Copyright 2018 StrTrek Team Authors.
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
import datetime
from Process.MQ import MessageQueue
from Message.Message import MSG, EMPTY_MSG, URL, reply_check
from DataBase.SQL import SQL


SQL = {
    "INIT": """

""",
    "UPDATE": """

""",
    "DAILY": """

""",
    "CLEAN": """

""",
}


def sender():
    mq_msg = EMPTY_MSG
    mq_msg.origination = URL("tcp://172.21.98.66:3307")
    mq_msg.destination = URL("tcp://172.21.98.66:10001")
    mq = MessageQueue(mq_msg)

    from_conn = URL("oracle://username:password@hostname/service")
    to_conn = URL("mysql://username:password@hostname/service#table")
    from_db = SQL(from_conn)
    to_db = SQL(to_conn)

    df = from_db.read(SQL["INIT"])
    msg = mq_msg.renew()
    msg.case = "SHA-AFDS"
    msg.activity = "INIT"
    msg.data = df.to_json()
    reply = mq.request(msg)
    while reply_check(reply):
        from_db.read(SQL["UPDATE"])
        


if __name__ == '__main__':
    sender()
