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
import os
import time
import pandas as pd
from sqlalchemy import create_engine
from Message import URL
# 切换中文字符
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class SQL:
    """
    Load Data from Oracle
    """
    def __init__(self, uri: str, typ: str):
        """
        :param sql: SQL脚本
        """

        uri = {
            "scheme": "oracle+cx_oracle",
            "username": "spia_acdm",
            "password": "Wonders",
            "netloc": "10.28.130.13:1521",
            "hostname": "mail.shairport.com",
            "port": 1521,
            "query": "orcl",
            "fragment": "",
        }
        conn = "oracle+cx_oracle://" + cfg["user"] + ":" + cfg["password"] \
               + "@" + cfg["host"] + ":" + str(cfg["port"]) + "/" + cfg["service"]
        self.engine = create_engine(conn)
        self.sql = sql
        self.typ = typ
        self.isLoad = False
        self.df = None

    def load(self):
        df = pd.read_sql(sql=self.sql, con=self.engine)
        self.df = df.rename(str.upper, axis='columns')
        self.isLoad = True
        return self.df

    def save(self):
        temp_path = "data/{0}-{1}.xlsx".format(self.typ, time.strftime('%Y-%m-%d', time.localtime(time.time())))
        if self.isLoad:
            df = self.df
        else:
            df = self.load()
        df.to_excel(temp_path)
        return temp_path
