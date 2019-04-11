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
from urllib.parse import urlparse, unquote, urlunparse, quote
from Tools.Conversion import dict2json, json2dict, dict2xml, xml2dict
from datetime import datetime
from CONFIG.config import GLOBAL_CFG
import base64
import re

DatetimeFmt = GLOBAL_CFG["DatetimeFormat"]
PortFmt = GLOBAL_CFG["PortFormat"]
CODING = GLOBAL_CFG["CODING"]
MSG_TYPE = GLOBAL_CFG["MSG_TYPE"]


def current_datetime() -> str:
    return datetime.now().strftime(DatetimeFmt)


class URL:
    # scheme://username:password@hostname:port/path;params?query#fragment
    scheme = "scheme"
    username = "username"
    password = "password"
    hostname = "hostname"
    port = "port"
    netloc = "{0}:{1}@{2}:{3}".format(username, password, hostname, port)
    path = "path"
    params = "params"
    query = "query"
    fragment = "fragment"

    def __init__(self, url=None):
        if isinstance(url, str):
            self.from_string(url)

    def __str__(self):
        return self.to_string()

    __repr__ = __str__

    def __setattr__(self, key, value):
        self.__dict__[key] = value      # source update
        keys = ["port", "hostname", "username", "password"]     # relative update
        if (key in keys) and (len([False for k in keys if k not in self.__dict__.keys()]) == 0):
            self.compose_netloc()
        if key in ["netloc"]:
            self.decompose_netloc()

    def __getattr__(self, item):
        return self.__dict__[item]

    def decompose_netloc(self):
        netloc = self.__dict__["netloc"]
        if not isinstance(netloc, str):
            raise ValueError("netloc 错误赋值")
        if "@" in netloc:
            auth, location = netloc.split("@")
        else:
            auth = None
            location = netloc
        if auth is None:
            self.__dict__["username"] = None
            self.__dict__["password"] = None
        else:
            if ":" in auth:
                self.__dict__["username"], self.__dict__["password"] = auth.split(":")
            else:
                self.__dict__["username"] = auth
                self.__dict__["password"] = None
        if ":" in location:
            self.__dict__["hostname"], self.__dict__["port"] = location.split(":")
        else:
            self.__dict__["hostname"] = location

    def compose_netloc(self):
        if self.__dict__["password"] is None:
            if self.__dict__["username"] is None:
                auth = None
            else:
                auth = self.username
        else:
            auth = "{0}:{1}".format(self.__dict__["username"], self.__dict__["password"])
        if self.__dict__["port"] is None:
            location = self.__dict__["hostname"]
        else:
            location = "{0}:{1}".format(self.__dict__["hostname"], self.__dict__["port"])
        if auth is None:
            self.__dict__["netloc"] = location
        else:
            self.__dict__["netloc"] = "{0}@{1}".format(auth, location)

    def to_string(self, allow_path=True, allow_params=True, allow_query=True, allow_fragment=True) -> str:
        path, params, query, fragment = "", "", "", ""
        if allow_path:
            path = self.path
        if allow_params:
            params = self.params
        if allow_query:
            query = self.query
        if allow_fragment:
            if isinstance(self.fragment, URL):
                fragment = quote(self.fragment.to_string())
            else:
                fragment = quote(self.fragment)
        return urlunparse((self.scheme, self.netloc, path, params, query, fragment))

    def from_string(self, url_string: str):
        default_port = None
        if re.search(r":port#", url_string) is not None:
            url_string = re.sub(r":port#", "#", url_string)
            default_port = "port"
        if re.search(r":port/", url_string) is not None:
            url_string = re.sub(r":port/", "/", url_string)
            default_port = "port"
        if re.search(r":port$", url_string) is not None:
            url_string = re.sub(r":port$", "", url_string)
            default_port = "port"
        url = urlparse(url_string)
        self.scheme = url.scheme
        self.username = url.username
        self.password = url.password
        self.hostname = url.hostname
        if default_port is None:
            self.port = url.port
        else:
            self.port = default_port
        self.path = re.sub(r'^/', "", re.sub(r'/$', "", url.path))
        self.params = url.params
        self.query = url.query
        if url.fragment == "":
            self.fragment = ""
        elif re.search("://", unquote(url.fragment)) is not None:
            try:
                self.fragment = URL(unquote(url.fragment))
            except RecursionError:
                self.fragment = url.fragment
        else:
            self.fragment = url.fragment

    @property
    def check(self):
        # "mysql://username:password@hostname:port/service#table"
        default_port = None
        if self.scheme in ["mysql"]:
            # "mysql+pymysql://username:password@hostname:3306/service#table"
            self.scheme = "mysql+pymysql"
            default_port = 3306
        # "oracle://username:password@hostname:port/service#table"
        if self.scheme in ["oracle"]:
            # "oracle+cx_oracle://username:password@hostname:1521/service#table"
            self.scheme = "oracle+cx_oracle"
            default_port = 1521
        # "ftp://username:password@hostname:port/path#PASV"
        if self.scheme in ["ftp", "ftpd"]:
            # "ftp://username:password@hostname:21/path#PASV"
            default_port = 21
        # "smtp://username:password@hostname:port"
        if self.scheme in ["smtp"]:
            # "smtp://username:password@hostname:port"
            default_port = 25
        # "pop3://username:password@hostname:port"
        if self.scheme in ["pop3"]:
            # "pop3://username:password@hostname:port"
            default_port = 110
        # "http://username:password@hostname:port"
        if self.scheme in ["http"]:
            # "http://username:password@hostname:port"
            default_port = 80
        if re.match(PortFmt, str(self.port)) is None:
            if default_port is not None:
                self.port = default_port
            else:
                self.port = None
        return self

    def init(self, scheme=None):
        if scheme in ["ftp"]:
            self.__init__("ftp://username:password@hostname:port/path#PASV")
        if scheme in ["mysql"]:
            self.__init__("mysql://username:password@hostname:port/service")
        if scheme in ["oracle"]:
            self.__init__("oracle://username:password@hostname:port/service")
        if scheme in ["tomail+smtp"]:
            smtp_conn = "{0}#{1}".format("smtp://sender_username:sender_password@sender_hostname:port",
                                         quote("tomail://sender_mail_username@sender_mail_hostname/sender"))
            self.__init__("{0}#{1}".format("tomail://receiver_mail_username@receive_mail_hostname/receiver",
                                           quote(smtp_conn)))
        if scheme in ["tcp"]:
            self.__init__("tcp://hostname:port")
        return self


class CASE:
    def __init__(self, case=None):
        self.origination = URL()
        self.destination = URL()
        if isinstance(case, str):
            self.from_string(case)

    def __str__(self):
        return self.to_string()

    __repr__ = __str__

    def to_string(self):
        return "{0}#{1}".format(quote(str(self.origination)), quote(str(self.destination)))

    def from_string(self, case: str):
        case = case.split("#")
        if len(case) == 2:
            self.origination = URL(unquote(case[0]))
            self.destination = URL(unquote(case[1]))
        else:
            raise ValueError("地址格式错误")


class DATA:
    def __init__(self, case=None):
        self.coding = CODING
        self.format = None
        self.path = None
        self.stream = None
        if isinstance(case, str):
            self.from_string(case)

    def from_string(self, case: str):
        case = case.split("#")
        if len(case) == 2:
            self.origination = URL(unquote(case[0]))
            self.destination = URL(unquote(case[1]))
        else:
            raise ValueError("地址格式错误")


def null_keep(item: object, item_type: classmethod = str) -> object:
    if item is None:
        return None
    elif isinstance(item, item_type):
        return item
    else:
        return item_type(item)


class MSG:
    timestamp = current_datetime()
    origination = URL()
    destination = URL()
    case = CASE()
    activity = None
    treatment = None
    encryption = None
    data = {
        "": None,
        "path": None,
        "stream": None,
    }

    def __init__(self, msg=None):
        if isinstance(msg, str):
            if MSG_TYPE is "json":
                self.from_json(msg)
            elif MSG_TYPE is "xml":
                self.from_xml(msg)
            else:
                raise NotImplementedError("仅支持 xml 和 json 类消息")
        if isinstance(msg, dict):
            self.from_dict(msg)

    def __str__(self):
        return self.to_string()

    __repr__ = __str__

    def __setattr__(self, key, value):
        self.__dict__[key] = value  # source update
        if key != "timestamp":
            self.__dict__["timestamp"] = current_datetime()

    def to_dict(self):
        return {
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
        }

    def to_serialize(self):
        return {
            "head": {
                "timestamp": null_keep(self.timestamp),
                "origination": null_keep(self.origination),
                "destination": null_keep(self.destination),
                "case": null_keep(self.case),
                "activity": null_keep(self.activity),
            },
            "body": {
                "treatment": null_keep(self.treatment),
                "encryption": null_keep(self.encryption),
                "data": null_keep(self.data),
            }
        }

    def to_string(self):
        if MSG_TYPE is "json":
            return self.to_json()
        elif MSG_TYPE is "xml":
            return self.to_xml()
        else:
            raise NotImplementedError("仅支持 xml 和 json 类消息")

    def to_json(self):
        return dict2json(self.to_serialize())

    def to_xml(self):
        return dict2xml(self.to_serialize())

    def from_dict(self, msg: dict):
        if not isinstance(msg, dict):
            raise AttributeError("参数类型错误")
        keys = ["head", "body"].extend(self.__dict__.keys())
        if len([False for k in msg.keys() if k not in keys]) > 0:
            raise ValueError("传入的变量参数错误")
        else:
            if isinstance(msg["head"], dict) and isinstance(msg["body"], dict):
                ks = len([True for k in msg["head"].keys() if k in keys]) +\
                     len([True for k in msg["body"].keys() if k in keys])
                if ks == (len(self.__dict__.keys()) - 2):
                    pass
                else:
                    raise ValueError("传入的变量参数错误")
            else:
                raise ValueError("传入的变量参数错误")
        if msg["head"]["timestamp"] is None:
            self.timestamp = current_datetime()
        else:
            self.timestamp = msg["head"]["timestamp"]
        self.origination = null_keep(msg["head"]["origination"], URL)
        self.destination = null_keep(msg["head"]["destination"], URL)
        self.case = null_keep(msg["head"]["case"], CASE)
        self.activity = null_keep(msg["head"]["activity"])
        self.treatment = null_keep(msg["body"]["treatment"], URL)
        self.encryption = null_keep(msg["body"]["encryption"])
        self.data = null_keep(msg["body"]["data"])
        self.data_check()

    def from_json(self, msg: str):
        self.from_dict(json2dict(msg))

    def from_xml(self, msg: str):
        self.from_dict(xml2dict(msg))

    def swap(self):
        origination = self.origination
        self.origination = self.destination
        self.destination = origination
        return self

    def forward(self, destination: URL):
        self.origination = self.destination
        self.destination = destination
        return self

    def heartbeat(self, state=None):
        # state is None: set to msg.heartbeat.request
        # state is bool: set to msg.heartbeat.reply
        if state is None:
            self.treatment = None
            self.encryption = None
            self.data = {"SUCCESS": None}
        else:
            self.swap()
            self.treatment = None
            self.encryption = None
            self.activity = "heartbeat"
            self.data = {"SUCCESS": state}
        return self

    def data_check(self):
        if self.encryption in ["base64"]:
            self.data = base64.b64decode(self.__dict__["data"])
        return self


def check_success_reply_msg(*args) -> bool:
    # Function Input Check
    if len(args) > 1:
        raise AttributeError("输入参数过多")
    if isinstance(args[0], str):
        msg = MSG(args[0])
    elif isinstance(args[0], MSG):
        msg = args[0]
    else:
        raise ValueError("未知的数据类型")
    if isinstance(msg.data, dict):
        if "SUCCESS" in msg.data.keys():
            return msg.data["SUCCESS"]
        else:
            return False
    else:
        return False


def demo_tomail():
    url = URL()
    url.init("tomail+smtp")
    print("URL:", url)
    print("收件人邮箱:", url.netloc)
    print("收件人用户名:", url.username)
    print("收件人名:", url.path)
    print("发件人邮箱：", url.fragment.fragment.netloc)
    print("发件人用户名:", url.fragment.username)
    print("发件人名:", url.fragment.fragment.path)
    print("服务协议:", url.fragment.scheme)
    print("服务用户:", url.fragment.username)
    print("服务密码:", url.fragment.password)
    print("服务地址", url.fragment.hostname)
    print("服务端口", url.fragment.port)


def demo_mysql():
    url = URL()
    url.init("mysql")
    # url.check()
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("数据服务", url.path)


def demo_oracle():
    url = URL()
    url.init("oracle")
    # url.check()
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("数据服务", url.path)


def demo_ftp():
    url = URL()
    url.init("ftp")
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("服务路径", url.path)
    print("服务模式", url.fragment)


def demo_msg_mysql2ftp():
    msg = MSG()
    ftp_url = URL()
    mysql_url = URL()
    msg.origination = mysql_url.init("mysql")
    msg.destination = ftp_url.init("ftp")
    msg.activity = "init"
    print(msg)


if __name__ == '__main__':
    # demo_tomail()
    # demo_ftp()
    # demo_mysql()
    demo_msg_mysql2ftp()
