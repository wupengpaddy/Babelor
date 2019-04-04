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
import os
import ftplib
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from Message.Message import URL


class FTP:
    def __init__(self, conn: str):
        self.conn = URL(conn)

    def send(self, attachments):
        ftp = ftplib.FTP()
        ftp.connect(self.conn.hostname, self.conn.port)     # 连接
        ftp.login(self.conn.username, self.conn.password)   # 登录
        if "PASV" in self.conn.fragment:
            ftp.set_pasv(True)                              # 被动模式
        for attach_path in attachments:
            with open(attach_path, 'rb') as attachment:
                ftp.storbinary('STOR ' + attach_path.split("/")[-1], attachment)  # 上传文件
        ftp.close()

    def receive(self):
        pass


def ftp_server():
    # Instantiate a dummy authorizer for managing 'virtual' users
    authorizer = DummyAuthorizer()

    # Define a new user having full r/w permissions
    authorizer.add_user('user', 'password', '/home/user', perm='elradfmwM')
    # Define a read-only anonymous user
    # authorizer.add_anonymous(os.getcwd())

    # Instantiate FTP handler class
    handler = FTPHandler
    handler.authorizer = authorizer

    # Define a customized banner (string returned when client connects)
    handler.banner = "Welcome to Shanghai Pudong International Airport Information Exchange Platform."

    # Specify a masquerade address and the range of ports to use for
    # passive connections.  De-comment in case you're behind a NAT.
    # handler.masquerade_address = '151.25.42.11'
    handler.passive_ports = range(10000, 10010)

    # Instantiate FTP server class and listen on 0.0.0.0:2121
    address = ('', 2121)
    server = FTPServer(address, handler)

    # set a limit for connections
    server.max_cons = 20
    server.max_cons_per_ip = 5

    # start ftp server
    server.serve_forever()
