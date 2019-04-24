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
import os
# Outer Required
import pandas as pd
from sqlalchemy import create_engine
# Inner Required
from Babelor.Message import URL, MSG
from Babelor.Config import GLOBAL_CFG
# Global Parameters
os.environ['NLS_LANG'] = GLOBAL_CFG["NLS_LANG"]     # 切换中文字符


class SQL:
    def __init__(self, conn: URL):
        # conn = "oracle://username:password@hostname:1521/service"
        # conn = "mysql://username:password@hostname:3306/service"
        if isinstance(conn, str):
            self.__conn = URL(conn)
        else:
            self.__conn = conn
        self.__conn = self.__dict__["conn"].check
        self.__engine = create_engine(self.__conn.to_string())

    def read(self, msg: MSG):
        new_msg = msg
        new_msg.nums = 0
        for i in range(0, msg.nums, 1):
            rt = msg.read_datum(i)
            df = pd.read_sql(sql=rt["stream"], con=self.__engine)
            df = df.rename(str.upper, axis='columns')
            new_msg.add_datum(df.to_msgpack(), path=rt["path"])
        return new_msg

    def write(self, msg: MSG):
        for i in range(0, msg.nums, 1):
            rt = msg.read_datum(i)
            df = pd.read_msgpack(rt["stream"])
            df.to_sql(rt["path"], con=self.__engine, if_exists='replace', index=False, index_label=False)
