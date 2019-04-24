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
# Outer Required
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
# Inner Required
from Babelor.Message import URL
from Babelor.Config import GLOBAL_CFG
# Global Parameters
BANNER = GLOBAL_CFG["FTP_BANNER"]
PASV_PORT = GLOBAL_CFG["FTP_PASV_PORTS"]
MAX_CONS = GLOBAL_CFG["FTP_MAX_CONS"]
MAX_CONS_PER_IP = GLOBAL_CFG["FTP_MAX_CONS_PER_IP"]
BUFFER_SIZE = GLOBAL_CFG['FTP_BUFFER_SIZE']


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
