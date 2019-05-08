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
import base64
import logging
# Outer Required
# Inner Required
from Babelor.Presentation import URL, MSG
# Global Parameters


class FILE:
    def __init__(self, conn: URL):
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        self.conn = self.__dict__["conn"].check

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
            path = os.path.join(self.conn.path, dt["path"])
            if os.path.exists(path):
                with open(path, "rb") as file:
                    stream = base64.b64encode(file.read())
                    msg_out.add_datum(datum=stream, path=dt["path"])
                logging.info("FILE {0} is read:{1}".format(path, os.path.exists(path)))
            else:
                msg_out.add_datum(datum=None, path=dt["path"])
        logging.info("FILE::{0}::READ return:{1}".format(self.conn, msg_out))
        return msg_out

    def write(self, msg: MSG):
        logging.info("FILE::{0}::WRITE msg:{1}".format(self.conn, msg))
        if not os.path.exists(self.conn.path):
            os.mkdir(self.conn.path)
        for i in range(0, msg.nums, 1):
            dt = msg.read_datum(i)
            if dt["stream"] is not None:
                stream = base64.b64decode(dt["stream"])
                path = os.path.join(self.conn.path, dt["path"])
                with open(path, "wb") as file:
                    file.write(stream)
                logging.info("FILE {0} is write:{1}.".format(path, os.path.exists(path)))
