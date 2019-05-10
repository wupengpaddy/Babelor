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
import logging
# Outer Required
import pandas as pd
from sqlalchemy import create_engine
# Inner Required
from Babelor.Presentation import URL, MSG
from Babelor.Config import GLOBAL_CFG
# Global Parameters
os.environ['NLS_LANG'] = GLOBAL_CFG["NLS_LANG"]     # 切换中文字符


class SQL:
    def __init__(self, conn: URL):
        # conn = "oracle://username:password@hostname:1521/service"
        # conn = "mysql://username:password@hostname:3306/service"
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        self.conn = self.__dict__["conn"].check
        self.engine = create_engine(self.conn.to_string())

    def read(self, msg: MSG):
        logging.debug("SQL::{0}::READ msg:{1}".format(self.conn, msg))
        msg_out = MSG()
        msg_out.origination = msg.origination
        msg_out.encryption = msg.encryption
        msg_out.treatment = msg.treatment
        msg_out.destination = msg.destination
        msg_out.case = msg.case
        msg_out.activity = msg.activity
        logging.debug("SQL::{0}::READ msg_out:{1}".format(self.conn, msg_out))
        # ----------------------------------
        for i in range(0, msg.nums, 1):
            rt = msg.read_datum(i)
            df = pd.read_sql(sql=rt["stream"], con=self.engine)
            df = df.rename(str.upper, axis='columns')
            msg_out.add_datum(datum=df, path=rt["path"])
        logging.info("SQL::{0}::READ return:{1}".format(self.conn, msg_out))
        return msg_out

    def write(self, msg: MSG):
        logging.info("SQL::{0} write:{1}".format(self.conn, msg))
        for i in range(0, msg.nums, 1):
            rt = msg.read_datum(i)
            df = rt["stream"]
            if isinstance(df, pd.DataFrame):
                df.to_sql(rt["path"], con=self.engine, if_exists='replace', index=False, index_label=False)
            else:
                logging.warning("SQL::{0}::WRITE failed.".format(self.conn))
