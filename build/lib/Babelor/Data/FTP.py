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
import ftplib
# Outer Required
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
        self.conn = self.__dict__["conn"].check

    def write(self, msg: MSG):
        ftp = self.open()
        for i in range(0, msg.nums, 1):
            attachment = msg.read_datum(i)
            ftp.storbinary('STOR ' + attachment["path"].split("/")[-1], attachment["stream"], BUFFER_SIZE)  # 上传文件
        ftp.close()

    def read(self, msg: MSG):
        new_msg = msg
        new_msg.nums = 0
        ftp = self.open()
        for i in range(0, msg.nums, 1):
            attachment = msg.read_datum(i)
            ftp.retrbinary('RETR ' + attachment.split("/")[-1], attachment["stream"], BUFFER_SIZE)  # 下载文件
            new_msg.add_datum(attachment["stream"], attachment['path'])
        ftp.close()
        return new_msg

    def open(self):
        ftp = ftplib.FTP()
        ftp.connect(self.conn.hostname, self.conn.port)     # 连接
        ftp.login(self.conn.username, self.conn.password)   # 登录
        if isinstance(self.conn.fragment, URL):             # 被动模式
            if self.conn.fragment.query not in [""]:
                if self.conn.fragment.query["model"] in ["PASV"]:
                    ftp.set_pasv(True)
            return ftp
