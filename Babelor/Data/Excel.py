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
import base64
import logging
# Outer Required
import pandas as pd
import xlrd
# Inner Required
from Babelor.Presentation import URL, MSG
# Global Parameters


class EXCEL:
    def __init__(self, conn: URL):
        # "excel:///<path>"
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        self.conn = self.__dict__["conn"].check

    def read(self, msg: MSG):
        logging.debug("EXCEL::{0}::READ msg:{1}".format(self.conn, msg))
        msg_out = MSG()
        msg_out.origination = msg.origination
        msg_out.encryption = msg.encryption
        msg_out.treatment = msg.treatment
        msg_out.destination = msg.destination
        msg_out.case = msg.case
        msg_out.activity = msg.activity
        logging.debug("EXCEL::{0}::READ msg_out:{1}".format(self.conn, msg_out))
        # -------------------------------------------------
        for i in range(0, msg.nums, 1):
            dt = msg.read_datum(i)
            path = os.path.join(self.conn.path, dt["path"])
            if os.path.exists(path):
                df = pd.read_excel(path)
                msg_out.add_datum(datum=df.to_msgpack(), path=dt["path"])
                logging.info("EXCEL {0} is read:{1}".format(path, os.path.exists(path)))
            else:
                msg_out.add_datum(datum=None, path=dt["path"])
        logging.info("EXCEL::{0}::READ return:{1}".format(self.conn, msg_out))
        return msg_out

    def write(self, msg: MSG):
        logging.info("EXCEL::{0}::WRITE msg:{1}".format(self.conn, msg))
        if not os.path.exists(self.conn.path):
            os.mkdir(self.conn.path)
        for i in range(0, msg.nums, 1):
            dt = msg.read_datum(i)
            if dt["stream"] is not None:
                stream = base64.b64decode(dt["stream"])
                path = os.path.join(self.conn.path, dt["path"])
                with open(path, "wb") as file:
                    file.write(stream)
                logging.info("EXCEL {0} is write:{1}.".format(path, os.path.exists(path)))


def sheets_merge(read_path, write_path):
    """
    :param read_path: 读取路径
    :param write_path: 写入路径
    :return: None
    """
    book = xlrd.open_workbook(read_path)
    writer = None
    for sheet in book.sheets():
        reader = pd.read_excel(read_path, sheet_name=sheet.name)
        if writer is None:
            writer = reader
        else:
            writer = writer.append(reader.fillna(""))       # NaN clean up
    writer = writer.reset_index(drop=True)                  # idx clean up
    writer.to_excel(write_path)


if __name__ == '__main__':
    sheets_merge("excel.xlsx", "excel_out.xlsx")
