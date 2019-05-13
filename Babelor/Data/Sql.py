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

# Global Parameters
from Babelor.Config import CONFIG
os.environ['NLS_LANG'] = CONFIG.Default_LANG     # 切换中文字符


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
        # ----------------------------------
        rm_idx = []
        for i in range(0, msg.args_count, 1):
            argument = msg.read_args(i)
            df = pd.read_sql(sql=argument["stream"], con=self.engine)
            df = df.rename(str.upper, axis='columns')
            msg.add_datum(datum=df, path=argument["path"])
            rm_idx = [i] + rm_idx
        # ----------------------------------
        if CONFIG.IS_DATA_READ_START:
            for i in rm_idx:
                msg.remove_args(i)
        logging.info("SQL::{0}::READ return:{1}".format(self.conn, msg))
        return msg

    def write(self, msg: MSG):
        logging.debug("SQL::{0} write:{1}".format(self.conn, msg))
        # ----------------------------------
        rm_idx = []
        for i in range(0, msg.dt_count, 1):
            rt = msg.read_datum(i)
            df = rt["stream"]
            path = os.path.splitext(rt["path"])[0]
            if isinstance(df, pd.DataFrame):
                df.to_sql(path, con=self.engine, if_exists='replace', index=False, index_label=False)
                logging.info("SQL::{0}::WRITE successfully.".format(self.conn))
            else:
                logging.warning("SQL::{0}::WRITE failed.".format(self.conn))
            rm_idx = [i] + rm_idx
        # ----------------------------------
        if CONFIG.IS_DATA_WRITE_END:
            for i in rm_idx:
                msg.remove_datum(i)
