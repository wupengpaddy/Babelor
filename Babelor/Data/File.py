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
from Babelor.Presentation.UniformResourceIdentifier import URL
from Babelor.Presentation.Message import MSG
from Babelor.Config.Config import GLOBAL_CFG
# Global Parameters


class FILE:
    def __init__(self, conn: URL):
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        self.conn = self.__dict__["conn"].check

    def read(self, msg: MSG):
        new_msg = msg
        new_msg.nums = 0
        for i in range(0, msg.nums, 1):
            dt = msg.read_datum(i)
            path = os.path.join(self.conn.path, dt["path"])
            if os.path.exists(path):
                with open(path, "rb") as file:
                    stream = base64.b64encode(file.read())
                    new_msg.add_datum(datum=stream, path=dt["path"])
            else:
                new_msg.add_datum(datum=None, path=dt["path"])
        return new_msg

    def write(self, msg: MSG):
        if not os.path.exists(self.conn.path):
            os.mkdir(self.conn.path)
        for i in range(0, msg.nums, 1):
            dt = msg.read_datum(i)
            stream = base64.b64decode(dt["stream"])
            path = os.path.join(self.conn.path, dt["path"])
            if stream is not None:
                with open(path, "wb") as file:
                    file.write(stream)
