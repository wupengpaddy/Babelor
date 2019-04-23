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
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.headerregistry import Address
from email import encoders
# 内部依赖
from Message import URL, MSG
from Config import GLOBAL_CFG
# 全局参数
CODING = GLOBAL_CFG["CODING"]


class TOMAIL:
    def __init__(self, conn: URL):
        # "tomail://receiver_mail_username@receive_mail_hostname/receiver"
        # "smtp://sender_username:sender_password@sender_hostname:port"
        # "tomail://sender_mail_username@sender_mail_hostname/sender"
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        self.conn = self.__dict__["conn"].check
        self.mime = MIMEMultipart()
        self.me = None
        self.to = None
        self.subject = None
        self.content = None

    def _create_mime(self, msg: MSG):
        sender_user = self.conn.fragment.username               # 寄件人用户名
        receiver_user = self.conn.username                      # 收件人用户名
        sender_name = self.conn.fragment.fragment.path          # 寄件人名
        receiver_name = self.conn.path                          # 收件人名
        sender_postfix = self.conn.fragment.fragment.hostname   # 寄件人 postfix
        receiver_postfix = self.conn.hostname                   # 收件人 postfix
        # Structure MIME
        self.me = str(Address(Header(sender_name, CODING).encode(), sender_user, sender_postfix))
        self.to = str(Address(Header(receiver_name, CODING).encode(), receiver_user, receiver_postfix))
        self.subject = msg.read_datum(0)
        self.content = msg.read_datum(1)
        self.mime['from'] = self.me                                     # 寄件人
        self.mime['to'] = self.to                                       # 收件人
        self.mime['subject'] = Header(self.subject, 'UTF-8').encode()   # 标题
        self.mime["Accept-Language"] = "zh-CN"                          # 语言
        self.mime["Accept-Charset"] = "ISO-8859-1,utf-8"                # 字符集
        self.mime.attach(MIMEText(self.content, 'plain', "utf-8"))      # 正文
        if msg.nums > 2:
            for i in range(2, msg.nums, 1):                             # 附件
                attachment = msg.read_datum(i)
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment["stream"])
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment',
                                filename=Header(attachment["path"].split("/")[-1], 'UTF-8').encode())
                self.mime.attach(part)

    def _send(self):
        hostname = self.conn.fragment.hostname      # 邮件服务主机
        port = self.conn.fragment.port              # 邮件服务端口
        username = self.conn.fragment.username      # 邮件服务用户
        password = self.conn.fragment.password      # 邮件服务密码
        with smtplib.SMTP_SSL(host=hostname, port=port) as sess:
            sess.login(user=username, password=password)
            sess.sendmail(self.me, self.to, self.mime.as_string())

    def write(self, msg: MSG):
        self._create_mime(msg)
        self._send()
