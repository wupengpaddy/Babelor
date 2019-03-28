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
from Message.Message import URL, check_sql_url
# 切换中文字符
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class SQL:
    """
    Load Data from Oracle
    """
    def __init__(self, conn: object):
        """
        :param conn: SQL连接脚本
        """
        # conn = "oracle://username:password@hostname:1521/service#table_name"
        # conn = "mysql://username:password@hostname:3306/service#table_name"
        if isinstance(conn, URL):
            self.conn = conn
        elif isinstance(conn, str):
            self.conn = URL(conn)
        else:
            raise AttributeError("输入的参数错误")
        self.engine = create_engine(check_sql_url(self.conn, has_table=False))

    def read(self, sql: str):
        df = pd.read_sql(sql=sql, con=self.engine)
        return df.rename(str.upper, axis='columns')

    def write(self, df: pd.DataFrame):
        if self.conn.fragment is None:
            raise ValueError("未定义写入表名")
        table_name = self.conn.fragment
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=False, index_label=False)
        return True
