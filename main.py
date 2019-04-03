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
import re
from Process.MQ import MessageQueue
from Message.Message import URL, MSG
from DataBase.SQL import SQL

BlockingTime = 0.025                # 堵塞时间

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
    mq_msg.destination = URL("tcp://172.21.98.66:10001")
    mq = MessageQueue(mq_msg)

    from_conn = URL("oracle://username:password@hostname/service")
    to_conn = URL("mysql://username:password@hostname/service#table")
    from_db = SQL(from_conn)
    to_db = SQL(to_conn)

    df_from_db = from_db.read(SQL["INIT"])
    msg = mq_msg.renew()
    msg.case = "SHA-AFDS"
    msg.activity = "INIT"
    msg.data = df_from_db.to_json()
    reply = mq.request(msg)
    while check_success_reply(reply):
        df_from_db = from_db.read(SQL["UPDATE"])


def receiver():
    mq_msg = EMPTY_MSG
    mq_msg.origination = URL("tcp://*:3307")
    mq = MessageQueue(mq_msg)


if __name__ == '__main__':
    url = "oracle://username:password@hostname:port/service#table"
    num = re.sub(r':port/', "/", url)
    print(num)

