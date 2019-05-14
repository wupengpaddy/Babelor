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
import logging
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.headerregistry import Address
from email import encoders
# Outer Required
# Inner Required
from Babelor.Presentation import URL, MSG
# Global Parameters
from Babelor.Config import CONFIG


class TOMAIL:
    def __init__(self, conn: URL):
        # "tomail://<receiver.mail.username>@<receive.mail.hostname>/<receiver>"
        # "smtp://<sender.username>:<sender.password>@<sender.hostname>:<port>"
        # "tomail://<sender.mail.username>@<sender.mail.hostname>/<sender>"
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
        # Connection
        sender_user = self.conn.fragment.username               # 寄件人用户名
        receiver_user = self.conn.username                      # 收件人用户名
        sender_name = self.conn.fragment.fragment.path          # 寄件人名
        receiver_name = self.conn.path                          # 收件人名
        sender_postfix = self.conn.fragment.fragment.hostname   # 寄件人 postfix
        receiver_postfix = self.conn.hostname                   # 收件人 postfix
        # from msg
        data = {}
        for i in range(0, msg.dt_count, 1):
            datum = msg.read_datum(i)
            if datum["path"] not in data.keys():
                data[datum["path"]] = datum["stream"]
        if "subject" in data.keys():
            subject = data["subject"]
        else:
            subject = CONFIG.MAIL_SUBJECT
        if "content" in data.keys():
            content = data["content"]
        else:
            content = CONFIG.MAIL_CONTENT
        attachments = []
        for k in data.keys():
            if k not in ["subject", "content"]:
                attachments.append({"stream": data[k], "path": k, })
        # Structure MIME
        self.me = str(Address(Header(sender_name, CONFIG.Coding).encode(), sender_user, sender_postfix))
        self.to = str(Address(Header(receiver_name, CONFIG.Coding).encode(), receiver_user, receiver_postfix))
        self.subject = subject
        self.content = content
        self.mime['from'] = self.me                                     # 寄件人
        self.mime['to'] = self.to                                       # 收件人
        self.mime['subject'] = Header(self.subject, 'UTF-8').encode()   # 标题
        self.mime["Accept-Language"] = "zh-CN"                          # 语言
        self.mime["Accept-Charset"] = "ISO-8859-1,utf-8"                # 字符集
        self.mime.attach(MIMEText(self.content, 'plain', "utf-8"))      # 正文
        if len(attachments) > 0:                                        # 附件
            for attachment in attachments:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment["stream"])
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment',
                                filename=Header(attachment["path"].split("/")[-1], 'UTF-8').encode())
                self.mime.attach(part)

    def _send(self):
        hostname = self.conn.fragment.hostname                  # 邮件服务主机
        port = self.conn.fragment.port                          # 邮件服务端口
        username = self.conn.fragment.username                  # 邮件服务用户
        password = self.conn.fragment.password                  # 邮件服务密码
        # smtp
        with smtplib.SMTP_SSL(host=hostname, port=port) as sess:
            sess.login(user=username, password=password)
            sess.sendmail(self.me, self.to, self.mime.as_string())

    def write(self, msg: MSG):
        self._create_mime(msg)
        self._send()
        logging.info("TOMAIL::{0}::WRITE msg:{1}".format(self.conn, msg))
