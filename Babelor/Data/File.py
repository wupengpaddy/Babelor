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
import numpy as np
# Inner Required
from Babelor.Presentation import URL, MSG
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
        # -------------------------------------------------
        for i in range(0, msg.args_count, 1):
            arguments = msg.read_args(i)
            if self.url_is_dir:
                path = os.path.join(self.conn.path, arguments["path"])
            else:
                path = self.conn.path
            suffix = os.path.splitext(path)[-1]
            # -------------------------------
            if os.path.isfile(path):
                if suffix in [".xls", ".xlsx"]:
                    if self.url_is_dir:
                        datum = pd.read_excel(path)
                    else:
                        datum = pd.read_excel(path, sheet_name=arguments["path"])
                elif suffix in [".npy"]:
                    datum = np.load(path)
                else:
                    with open(path, "rb") as file:
                        datum = file.read()
                msg.add_datum(datum, arguments["path"])
                logging.info("FILE::{0}::READ successfully.".format(path))
            else:
                logging.warning("FILE::{0}::READ failed.".format(path))
            # -------------------------------
            msg.remove_args(i)
        logging.info("FILE::{0}::READ return:{1}".format(self.conn, msg))
        return msg

    def write(self, msg: MSG):
        logging.debug("FILE::{0}::WRITE msg:{1}".format(self.conn, msg))
        if self.url_is_dir:
            if not os.path.exists(self.conn.path):
                os.mkdir(self.conn.path)
        for i in range(0, msg.dt_count, 1):
            dt = msg.read_datum(i)
            if self.url_is_dir:
                path = os.path.join(self.conn.path, dt["path"])
            else:
                path = self.conn.path
            suffix = os.path.splitext(path)[-1]
            # -------------------------------
            if os.path.exists(path):
                logging.warning("FILE::{0}::WRITE failed.".format(path))
            elif os.path.isfile(os.path.split(path)[0]):
                logging.warning("FILE::{0}::WRITE failed.".format(path))
            else:
                if not os.path.isdir(os.path.split(path)[0]):
                    mkdir(os.path.split(path)[0])
                # -------------------------------
                if suffix in [".xls", ".xlsx"]:
                    if isinstance(dt["stream"], pd.DataFrame):
                        dt["stream"].to_excel(path, index=False)
                        logging.info("FILE::EXCEL::{0}::WRITE successfully.".format(path))
                    else:
                        logging.warning("FILE::EXCEL::{0}::WRITE failed.".format(path))
                elif suffix in [".npy"]:
                    if isinstance(dt["stream"], np.ndarray):
                        np.save(path, dt["stream"])
                        logging.info("FILE::NUMPY::{0}::WRITE successfully.".format(path))
                    else:
                        logging.warning("FILE::NUMPY::{0}::WRITE failed.".format(path))
                elif suffix in [""]:
                    logging.warning("FILE::{0}::WRITE None.".format(path))
                else:
                    with open(path, "wb") as file:
                        file.write(dt["stream"])
                    logging.info("FILE::{0}::WRITE successfully.".format(path))


def mkdir(path: str):
    if not os.path.exists(os.path.split(path)[0]):
        mkdir(os.path.split(path)[0])
        mkdir(path)
    else:
        if os.path.isfile(path):
            os.remove(path)
        if not os.path.exists(path):
            os.mkdir(path)


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
