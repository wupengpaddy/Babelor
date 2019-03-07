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
import pandas as pd
from sqlalchemy import create_engine
from Message.Message import URL
# 切换中文字符
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class SQL:
    """
    Load Data from Oracle
    """
    def __init__(self, conn: str):
        """
        :param conn: SQL连接脚本
        """
        # conn = "oracle://username:password@hostname:1521/path"
        # conn = "mysql://username:password@hostname:3306/path"
        self.engine = create_engine(sql_connection_check(conn))
        self.request_sql = None
        self.reply_df = None

    def request(self, sql: str):
        self.request_sql = sql
        reply_df = pd.read_sql(sql=self.request_sql, con=self.engine)
        self.reply_df = reply_df.rename(str.upper, axis='columns')
        return self.reply_df


def sql_connection_check(conn: str):
    # conn = "oracle+cx_oracle://username:password@hostname:1521/path"
    # conn = "mysql+pymysql://username:password@hostname:3306/path"
    conn_url = URL(conn)
    port = 0
    if conn_url.scheme == "oracle":
        conn_url.scheme = "oracle+cx_oracle"
        port = 1521
    if conn_url.scheme == "mysql":
        conn_url.scheme = "mysql+pymysql"
        port = 3306
    if conn_url.port is None:
        conn_url.port = port
        conn_url.netloc = "{0}:{1}".format(conn_url.netloc, port)
    return conn_url.get_url(True, False, False, False)
