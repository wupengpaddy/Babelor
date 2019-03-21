# coding=utf-8
# Copyright 2019 StrTrek Team Authors.
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
from urllib.parse import urlparse, unquote, urlunparse
from datetime import datetime
from Tools.Conversion import dict2json, json2dict

DatetimeFmt = '%Y-%m-%d %H:%M:%S.%f'
CODING = 'utf-8'


def get_current_datetime():
    return datetime.now().strftime(DatetimeFmt)


MAIL_MSG_BODY = {
    'subject': '测试邮件主题',
    'content': '测试邮件正文',
    'attachments': None
}

RESPONSE_MSG = {
    "SUCCESS": {
        "STAT": True
    },
    "FAULT": {
        "STAT": False
    }
}


class URL:
    def __init__(self, url: str):
        url = urlparse(url)
        self.scheme = url.scheme
        self.username = url.username
        self.password = url.password
        self.hostname = url.hostname
        self.port = url.port
        self.netloc = url.netloc
        if "/" in url.path:
            self.path = url.path[url.path.rindex("/") + 1:]
        else:
            self.path = url.path
        self.params = url.params
        self.query = url.query
        try:
            self.fragment = URL(unquote(url.fragment))
        except RecursionError:
            self.fragment = url.fragment
        self.url = self.get_url()

    def __str__(self):
        return self.get_url()

    def get_url(self, allow_path=True, allow_params=True, allow_query=True, allow_fragment=True):
        path, params, query, fragment = "", "", "", ""
        if allow_path:
            path = self.path
        if allow_params:
            params = self.params
        if allow_query:
            query = self.query
        if allow_fragment:
            if isinstance(self.fragment, URL):
                fragment = self.fragment.get_url()
            else:
                fragment = self.fragment
        return urlunparse((self.scheme, self.netloc, path, params, query, fragment))


class MSG:
    def __init__(self, json: str):
        msg = json2dict(json)
        self.timestamp = msg["head"]["timestamp"]
        self.origination = URL(msg["head"]["origination"])
        self.destination = URL(msg["head"]["destination"])
        self.case = msg["head"]["case"]
        self.activity = msg["head"]["activity"]
        self.treatment = msg["body"]["treatment"]
        self.encryption = msg["body"]["encryption"]
        self.data = msg["body"]["data"]

    def __str__(self):
        return self.get_msg()

    def get_msg(self):
        return dict2json({
            "head": {
                "timestamp": self.timestamp,
                "origination": self.origination,
                "destination": self.destination,
                "case": self.case,
                "activity": self.activity,
            },
            "body": {
                "treatment": self.treatment,
                "encryption": self.encryption,
                "data": self.data,
            }
        })


EMPTY_URL = URL()


EMPTY_MSG = MSG(dict2json({
            "head": {
                "timestamp": "",
                "origination": "",
                "destination": "",
                "case": "",
                "activity": "",
            },
            "body": {
                "treatment": "",
                "encryption": "",
                "data": "",
            }
        }))


def check_url(*args):
    if len(args) > 1:
        raise AttributeError
    if isinstance(args[0], str):
        return URL(args[0])
    elif isinstance(args[0], URL):
        return args[0]
    else:
        raise ValueError


def check_sql_url(*args):
    # conn = "oracle://username:password@hostname/service"
    # conn = "mysql://username:password@hostname/service"
    # conn = "oracle://username:password@hostname:1521/service"
    # conn = "mysql://username:password@hostname:3306/service"
    url = URL(args[0])
    port = 0
    if url.scheme == "oracle":
        url.scheme = "oracle+cx_oracle"
        port = 1521
    if url.scheme == "mysql":
        url.scheme = "mysql+pymysql"
        port = 3306
    if url.port is None:
        url.port = port
        url.netloc = "{0}:{1}".format(url.netloc, port)
    # conn = "oracle+cx_oracle://username:password@hostname:1521/service"
    # conn = "mysql+pymysql://username:password@hostname:3306/service"
    return url.get_url(True, False, False, False)


def check_ftp_url(*args):
    # conn = "ftp://username:password@hostname:21/path#PASV"
    url = URL(args[0])
    if url.scheme != "ftp":
        raise ValueError
    if url.port is None:
        url.port = 21
        url.netloc = "{0}:{1}".format(url.netloc, url.port)
    return url.get_url(True, False, False, True)


def demo_tomail():
    conn = "{0}#{1}#{2}#{3}".format("tomail://receiver_mail_username@receive_mail_hostname",
                                    "smtp://sender_username:sender_password@sender_hostname:10001",
                                    "tomail://sender_mail_username_2@senderhostname_2",
                                    "receiver_user")
    url = URL(conn)
    print("URL:", url)
    print("收件人:", url.netloc)
    print("收件人协议:", url.scheme)
    print("收件人用户名:", url.username)
    print("收件人名:", url.fragment.fragment.fragment)
    print("发件人：", "{0}@{1}".format(url.fragment.username, url.fragment.fragment.hostname))
    print("发件人协议:", url.fragment.fragment.scheme)
    print("发件人用户名:", url.fragment.username)
    print("发件人名:", url.fragment.fragment.username)
    print("服务协议:", url.fragment.scheme)
    print("服务用户:", url.fragment.username)
    print("服务密码:", url.fragment.password)
    print("服务地址", url.fragment.hostname)
    print("服务端口", url.fragment.port)


def demo_sql():
    conn = "oracle://username:password@hostname:1521/service"
    url = URL(conn)
    print("URL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("数据服务", url.path)


def demo_ftp():
    conn = "ftp://username:password@hostname/path#PASV"
    url = URL(conn)
    print("URL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("服务路径", url.path)
    print("服务模式", url.fragment)

