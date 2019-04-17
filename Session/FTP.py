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

# 外部依赖
import ftplib
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
# 内部依赖
from Message.Message import URL, MSG
from CONFIG import GLOBAL_CFG
# 全局参数
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
        ftp = self.ftp_client()
        for i in range(0, msg.nums, 1):
            attachment = msg.read_datum(i)
            ftp.storbinary('STOR ' + attachment["path"].split("/")[-1], attachment["stream"], BUFFER_SIZE)  # 上传文件
        ftp.close()

    def read(self, msg: MSG):
        new_msg = msg
        new_msg.nums = 0
        ftp = self.ftp_client()
        for i in range(0, msg.nums, 1):
            attachment = msg.read_datum(i)
            ftp.retrbinary('RETR ' + attachment.split("/")[-1], attachment["stream"], BUFFER_SIZE)  # 下载文件
            new_msg.add_datum(attachment["stream"], attachment['path'])
        ftp.close()
        return new_msg

    def ftp_client(self):
        ftp = ftplib.FTP()
        ftp.connect(self.conn.hostname, self.conn.port)      # 连接
        ftp.login(self.conn.username, self.conn.password)    # 登录
        if "PASV" in self.conn.fragment:                     # 被动模式
            ftp.set_pasv(True)
        return ftp


class FTPD:
    def __init__(self, conn: URL):
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        self.conn = self.__dict__["conn"].check

    def ftp_server(self):
        authorizer = DummyAuthorizer()
        authorizer.add_user(self.conn.username, self.conn.password, self.conn.path, perm='elradfmwM')
        handler = FTPHandler
        handler.authorizer = authorizer
        handler.banner = BANNER
        if "*" in self.conn.hostname:
            address = ('', int(self.conn.port))
        else:
            handler.masquerade_address = self.conn.hostname
            address = (self.conn.hostname, int(self.conn.port))
        if "PASV" in self.conn.fragment:  # 被动模式
            handler.passive_ports = range(PASV_PORT["START"], PASV_PORT["END"])
        server = FTPServer(address, handler)
        server.max_cons = MAX_CONS
        server.max_cons_per_ip = MAX_CONS_PER_IP
        server.serve_forever()
