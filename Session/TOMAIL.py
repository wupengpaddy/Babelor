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
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.headerregistry import Address
from email import encoders
from Message.Message import URL


class MAIL:
    """
    Mail Model
    """
    def __init__(self, conn: str):
        # tomail://user1@strtrek.com#smtp://user2:password@192.168.0.1:10001#tomail://用户2@strtrek.com#用户1
        self.conn = URL(conn)

    def send(self, mail_msg: dict):
        sender_user = self.conn.fragment.username               # 寄件人用户名
        receiver_user = self.conn.username                      # 收件人用户名
        sender_name = self.conn.fragment.fragment.username      # 寄件人名
        receiver_name = self.conn.fragment.fragment.fragment    # 收件人名
        sender_postfix = self.conn.fragment.fragment.hostname   # 寄件人 postfix
        receiver_postfix = self.conn.hostname                   # 收件人 postfix
        # Structure MIME
        me = str(Address(Header(sender_name, 'UTF-8').encode(), sender_user, sender_postfix))
        to = str(Address(Header(receiver_name, 'UTF-8').encode(), receiver_user, receiver_postfix))
        msg = MIMEMultipart()
        msg['from'] = me                                                # 寄件人
        msg['to'] = to                                                  # 收件人
        msg['subject'] = Header(mail_msg["subject"], 'UTF-8').encode()      # 标题
        msg["Accept-Language"] = "zh-CN"
        msg["Accept-Charset"] = "ISO-8859-1,utf-8"
        msg.attach(MIMEText(mail_msg["content"], 'plain', "utf-8"))         # 正文
        attachments = mail_msg["attachments"]                               # 附件
        if attachments is not None:
            for attach_path in attachments:
                with open(attach_path, 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', 'attachment',
                                    filename=Header(attach_path.split("/")[-1], 'UTF-8').encode())
                    msg.attach(part)
        # Send mail
        hostname = self.conn.fragment.hostname      # 邮件服务主机
        port = self.conn.fragment.port              # 邮件服务端口
        username = self.conn.fragment.username      # 邮件服务用户
        password = self.conn.fragment.password      # 邮件服务密码
        with smtplib.SMTP_SSL(host=hostname, port=port) as sess:
            sess.login(user=username, password=password)
            sess.sendmail(me, to, msg.as_string())
