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
import ftplib
import logging
# Outer Required
import pandas as pd
# Inner Required
from Babelor.Presentation import URL, MSG
from Babelor.Config import GLOBAL_CFG
# Global Parameters
BANNER = GLOBAL_CFG["FTP_BANNER"]
PASV_PORT = GLOBAL_CFG["FTP_PASV_PORTS"]
MAX_CONS = GLOBAL_CFG["FTP_MAX_CONS"]
MAX_CONS_PER_IP = GLOBAL_CFG["FTP_MAX_CONS_PER_IP"]
BUFFER_SIZE = GLOBAL_CFG['FTP_BUFFER_SIZE']


class FTP:
    def __init__(self, conn: URL):
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        if os.path.splitext(self.conn.path)[-1] in [""]:
            self.url_is_dir = True
        else:
            self.url_is_dir = False

    def read(self, msg: MSG):
        logging.debug("FTP::{0}::READ msg:{1}".format(self.conn, msg))
        msg_out = MSG()
        msg_out.origination = msg.origination
        msg_out.encryption = msg.encryption
        msg_out.treatment = msg.treatment
        msg_out.destination = msg.destination
        msg_out.case = msg.case
        msg_out.activity = msg.activity
        logging.debug("FTP::{0}::READ msg_out:{1}".format(self.conn, msg_out))
        # -------------------------------------------------
        ftp = self.open()
        # -------------------------------------------------
        for i in range(0, msg.nums, 1):
            dt = msg.read_datum(i)
            if self.url_is_dir:
                path = os.path.join(self.conn.path, dt["path"])
            else:
                path = self.conn.path
            suffix = os.path.splitext(path)[-1]
            # ----------------------------
            stream = bytes()
            ftp.retrbinary('RETR ' + path, stream, BUFFER_SIZE)
            logging.info("FTP::{0}::READ successfully.".format(path))
            # -------------------------------
            if suffix in ["xls", "xlsx"]:
                temp_path = "temp/temp" + suffix
                with open(temp_path, "wb") as temp_file:
                    temp_file.write(stream)
                if self.url_is_dir:
                    stream = pd.read_excel(temp_path)
                else:
                    stream = pd.read_excel(temp_path, sheet_name=dt["path"])
                os.remove(temp_path)
                del temp_path
                logging.info("FTP::EXCEL::READ successfully.".format(path))
            msg_out.add_datum(datum=stream, path=dt["path"])
        # -------------------------------------------------
        logging.info("FTP::{0}::READ return:{1}".format(self.conn, msg_out))
        return msg_out

    def write(self, msg: MSG):
        logging.info("FTP::{0}::WRITE msg:{1}".format(self.conn, msg))
        # -------------------------------------------------
        ftp = self.open()
        # -------------------------------------------------
        for i in range(0, msg.nums, 1):
            dt = msg.read_datum(i)
            if self.url_is_dir:
                path = os.path.join(self.conn.path, dt["path"])
            else:
                path = self.conn.path
            suffix = os.path.splitext(path)[-1]
            # ----------------------------
            if suffix in ["xls", "xlsx"]:
                temp_path = "temp/temp" + suffix
                if isinstance(dt["stream"], pd.DataFrame):
                    dt["stream"].to_excel(temp_path, index=False)
                    with open(temp_path, "rb") as temp_file:
                        stream = temp_file.read()
                    logging.info("FILE::EXCEL::{0} write successfully.".format(path))
                else:
                    stream = None
                    logging.warning("FILE::EXCEL::{0} write failed.".format(path))
            else:
                stream = dt["stream"]
            if stream is not None:
                ftp.storbinary('STOR ' + path, stream, BUFFER_SIZE)
                logging.info("FTP::{0}::WRITE successfully.".format(self.conn))
        ftp.close()

    def open(self):
        ftp = ftplib.FTP()
        ftp.connect(self.conn.hostname, self.conn.port)
        ftp.login(self.conn.username, self.conn.password)
        # ----------------------------------------------------- 被动模式 - PASV Model
        if isinstance(self.conn.fragment, URL):
            if isinstance(self.conn.fragment.query, dict):
                if self.conn.fragment.query["model"] in ["PASV"]:
                    ftp.set_pasv(True)
            return ftp
