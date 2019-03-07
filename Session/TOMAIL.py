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
import pandas as pd
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.headerregistry import Address
from email import encoders


def email_send(select_date: str, df: pd.DataFrame):
    file_path = "data/{0}.xlsx".format(select_date)
    df.to_excel(file_path, index=False)
    mail_to = {
        'name':     '葛元霁',
        'user':     'geyuanji',
        'postfix':  'shairport.com'
    }
    mail_sub = '航班日报{0}'.format(select_date)
    mail_content = "航班日报{0}".format(select_date)
    mail = Mail(mail_to, mail_sub, mail_content, file_path)
    mail.send()
    print("APP45-PVG2SAA send {0} data by email successful.".format(select_date))
    os.remove(file_path)


def json_send(select_date: str, df: pd.DataFrame):
    msg = MessageQueue(select_date, df)
    msg.send()
    print("APP45-PVG2SAA send {0} data via json successful.".format(select_date))


class Mail:
    """
    Mail Model
    """
    def __init__(self, to: dict, sub: str, content: str, attach_path=None):
        """
        :param to: 收件人{'name': str, 'user': str, 'postfix': str}
        :param sub: 邮件主题
        :param content: 邮件正文
        :param attach_path: 邮件附件
        """
        # Mail config
        self.cfg = {
            "host":     "172.21.98.66",           # SMTP 服务器
            "port":     10002,                  # 服务端口
            "user":     "tanghailing",          # 用户名
            "name":     "唐海铃",               # 发件人
            "pass":     "65684446Mail",         # 口令
            "postfix":  "shairport.com"         # 发件箱后缀
        }
        # Structure ME/TO
        self.me = str(Address(Header(self.cfg['name'], 'UTF-8').encode(), self.cfg['user'], self.cfg['postfix']))
        self.to = str(Address(Header(to['name'], 'UTF-8').encode(), to['user'], to['postfix']))
        self.msg = self.create_email(sub, content, attach_path)

    # Structure MIME
    def create_email(self, sub: str, content: str, attach_path=None) -> str:
        msg = MIMEMultipart()
        msg['from'] = self.me                               # 寄件人
        msg['to'] = self.to                                 # 收件人
        msg['subject'] = Header(sub, 'UTF-8').encode()      # 主题
        msg["Accept-Language"] = "zh-CN"
        msg["Accept-Charset"] = "ISO-8859-1,utf-8"
        msg.attach(MIMEText(content, 'plain', "utf-8"))     # 正文
        if attach_path is not None:
            with open(attach_path, 'rb') as attachment:        # 附件
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment',
                                filename=Header(attach_path[attach_path.rindex("/") + 1:], 'UTF-8').encode())
                msg.attach(part)
        return msg.as_string()

    # noinspection PyBroadException
    def send(self):
        # print(self.msg)
        with smtplib.SMTP_SSL(host=self.cfg["host"], port=self.cfg["port"]) as sess:
            sess.login(user=self.cfg["user"], password=self.cfg["pass"])
            sess.sendmail(self.me, self.to, self.msg)


def demo():
    mail_to = {
        'name':     '葛元霁',
        'user':     'geyuanji',
        'postfix':  'strtrek.com'
    }
    mail_sub = '航班日报'
    mail_content = "测试正文"
    mail = Mail(mail_to, mail_sub, mail_content)
    mail.send()


if __name__ == '__main__':
    demo()
