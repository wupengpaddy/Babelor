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
import logging
# Outer Required
import xlrd
import pandas as pd
# Inner Required
from Babelor.Presentation import URL, MSG, DATUM
# Global Parameters


class FILE:
    def __init__(self, conn: (URL, str)):
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        if os.path.splitext(self.conn.path)[-1] in [""]:
            self.url_is_dir = True
        else:
            self.url_is_dir = False

    def read(self, msg: MSG):
        logging.debug("FILE::{0}::READ msg:{1}".format(self.conn, msg))
        msg_out = MSG()
        msg_out.origination = msg.origination
        msg_out.encryption = msg.encryption
        msg_out.treatment = msg.treatment
        msg_out.destination = msg.destination
        msg_out.case = msg.case
        msg_out.activity = msg.activity
        logging.debug("FILE::{0}::READ msg_out:{1}".format(self.conn, msg_out))
        # -------------------------------------------------
        for i in range(0, msg.nums, 1):
            dt = msg.read_datum(i)
            if self.url_is_dir:
                path = os.path.join(self.conn.path, dt["path"])
            else:
                path = self.conn.path
            suffix = os.path.splitext(path)
            # -------------------------------
            if os.path.isfile(path):
                if suffix in ["xls", "xlsx"]:
                    if self.url_is_dir:
                        stream = pd.read_excel(path)
                    else:
                        stream = pd.read_excel(path, sheet_name=dt["path"])
                else:
                    with open(path, "rb") as file:
                        stream = file.read()
                msg_out.add_datum(datum=stream, path=dt["path"])
                logging.info("FILE::{0}::READ successfully.".format(path))
            else:
                msg_out.add_datum(datum=None, path=dt["path"])
                logging.warning("FILE::{0}::READ failed.".format(path))
        logging.info("FILE::{0}::READ return:{1}".format(self.conn, msg_out))
        return msg_out

    def write(self, msg: MSG):
        logging.info("FILE::{0}::WRITE msg:{1}".format(self.conn, msg))
        if self.url_is_dir:
            if not os.path.exists(self.conn.path):
                os.mkdir(self.conn.path)
        for i in range(0, msg.nums, 1):
            dt = msg.read_datum(i)
            df = dt["stream"]
            if self.url_is_dir:
                path = os.path.join(self.conn.path, dt["path"])
            else:
                path = self.conn.path
            if os.path.exists(path):
                logging.warning("FILE::{0} write failed.".format(path))
            else:
                if isinstance(df, pd.DataFrame):
                    df.to_excel(path, index=False)
                    logging.info("FILE::EXCEL::{0} write successfully.".format(path))
                elif df is None:
                    logging.warning("FILE::NONE::{0} write successfully.".format(path))
                else:
                    with open(path, "wb") as file:
                        file.write(df)
                    logging.info("FILE::{0} write successfully.".format(path))


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
