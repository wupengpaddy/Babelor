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
from Message import URL, MSG, CASE
from DataBase import SQL
from Process import MessageQueue


class SENDER:
    def __init__(self, conn: URL):
        # conn = "tcp://*:port"
        if isinstance(conn, str):
            self.conn = URL(conn)
        self.conn = self.__dict__["conn"].check
        self.mq = MessageQueue(conn)
        self.mq.pull()

    