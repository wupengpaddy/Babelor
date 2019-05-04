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

# System Required
import os
import base64
# Outer Required
# Inner Required
from Babelor.Presentation import URL, MSG
from Babelor.Config import GLOBAL_CFG
from Babelor.Data import EXCEL
# Global Parameters


class FILE:
    def __init__(self, conn: URL):
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        self.conn = self.__dict__["conn"].check

    def read(self, msg: MSG):
        with open(self.conn.path, "rb") as f:
            stream = base64.b64encode(f.read())
            msg.add_datum(datum=stream, path=self.conn.path)
        return msg

    def write(self, msg: MSG):
        for i in range(0, msg.nums, 1):
            stream = msg.read_datum(i)
            f = base64.b64decode(stream["stream"])
