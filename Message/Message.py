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
import re

DatetimeFmt = '%Y-%m-%d %H:%M:%S.%f'
CODING = 'utf-8'
MSG_TYPE = 'json'           # "xml"


def current_datetime() -> str:
    return datetime.now().strftime(DatetimeFmt)


class URL:
    def __init__(self, url=None):
        # scheme://username:password@hostname:port/path;params?query#fragment
        if isinstance(url, str):
            if re.search(r"://", url) is None:
                raise ValueError
            self.from_string(url)
        elif url is None:
            self.scheme = "scheme"
            self.username = "username"
            self.password = "password"
            self.hostname = "hostname"
            self.port = "port"
            self.netloc = "{0}:{1}@{2}:{3}".format(self.username, self.password, self.hostname, self.port)
            self.path = "path"
            self.params = "params"
            self.query = "query"
            self.fragment = "fragment"
        else:
            raise AttributeError

    def __str__(self):
        return self.to_string()

    __repr__ = __str__

    def __setattr__(self, key, value):
        self.__dict__[key] = value  # source update
        keys = ["port", "hostname", "username", "password"]
        if (key in keys) and (len([False for k in keys if k not in self.__dict__.keys()]) == 0):    # relative update
            if self.__dict__["password"] is None:
                if self.__dict__["username"] is None:
                    user_info = None
                else:
                    user_info = self.username
            else:
                user_info = "{0}:{1}".format(self.__dict__["username"], self.__dict__["password"])
            if self.__dict__["port"] is None:
                net_info = self.__dict__["hostname"]
            else:
                net_info = "{0}:{1}".format(self.__dict__["hostname"], self.__dict__["port"])
            if user_info is None:
                self.__dict__["netloc"] = net_info
            else:
                self.__dict__["netloc"] = "{0}@{1}".format(user_info, net_info)

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

    def from_string(self, url: str):
        pattern = re.compile(r":[A-Za-z_-]+[$/]")
        if pattern.search(url) is None:    # urlparse not apply a string
            port_pattern = None
            url = urlparse(url)
        else:
            port_pattern = pattern.findall(url)[0]
            url = urlparse(pattern.sub("/", url))
        self.scheme = url.scheme
        self.username = url.username
        self.password = url.password
        self.hostname = url.hostname
        if port_pattern is None:
            self.port = url.port
        else:
            self.port = re.sub(r'^:', "", re.sub(r'/$', "", port_pattern))
        self.netloc = "{0}:{1}@{2}:{3}".format(self.username, self.password, self.hostname, self.port)
        self.path = re.sub(r'/$', "", url.path)
        self.params = url.params
        self.query = url.query
        try:
            self.fragment = URL(unquote(url.fragment))
        except ValueError:
            self.fragment = url.fragment


EMPTY_URL = URL("scheme://username:password@hostname:port/path;params?query#fragment")


def null_value_check(msg_value: object, msg_value_type: classmethod = str) -> object:
    if msg_value is None:
        return None
    elif isinstance(msg_value, msg_value_type):
        return msg_value
    else:
        return msg_value_type(msg_value)


class MSG:
    def __init__(self, msg=None):
        if isinstance(msg, str):
            if MSG_TYPE is "json":
                self.from_json(msg)
            elif MSG_TYPE is "xml":
                self.from_xml(msg)
            else:
                raise NotImplementedError("仅支持 xml 和 json 类消息")
        elif isinstance(msg, dict):
            self.from_dict(msg)
        elif msg is None:
            self.timestamp = current_datetime()
            self.timestamp = None
            self.origination = None
            self.destination = None
            self.case = None
            self.activity = None
            self.treatment = None
            self.encryption = None
            self.data = None

    def __str__(self):
        return self.to_string()

    __repr__ = __str__

    def __setattr__(self, key, value):
        self.__dict__[key] = value  # source update
        keys = ["timestamp", "origination", "destination", "case", "activity", "treatment", "encryption", "data"]
        if len(self.__dict__.keys()) == len(keys):  # force update datetime
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

    def to_string(self):
        if MSG_TYPE is "json":
            return self.to_json()
        elif MSG_TYPE is "xml":
            return self.to_xml()
        else:
            raise NotImplementedError("仅支持 xml 和 json 类消息")

    def to_json(self):
        return dict2json(self.to_dict())

    def to_xml(self):
        return dict2xml(self.to_dict())

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
        self.origination = null_value_check(msg["head"]["origination"], URL)
        self.destination = null_value_check(msg["head"]["destination"], URL)
        self.case = null_value_check(msg["head"]["case"])
        self.activity = null_value_check(msg["head"]["activity"])
        self.treatment = null_value_check(msg["body"]["treatment"], URL)
        self.encryption = null_value_check(msg["body"]["encryption"], URL)
        self.data = null_value_check(msg["body"]["data"])

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

    def renew(self):
        self.timestamp = current_datetime()
        self.data = None
        return self

    def success_reply(self, state: bool):
        self.renew()
        self.swap()
        self.treatment = None
        self.encryption = None
        self.data = {"SUCCESS": state}
        return self


def check_sql_url(url: URL, has_table=False) -> str:
    # conn = "oracle://username:password@hostname/service#table"
    # conn = "mysql://username:password@hostname/service#table"
    # conn = "oracle://username:password@hostname:port/service#table"
    # conn = "mysql://username:password@hostname:port/service#table"
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
    # conn = "oracle+cx_oracle://username:password@hostname:1521/service#table"
    # conn = "mysql+pymysql://username:password@hostname:3306/service#table"
    if has_table:
        return url.to_string(True, False, False, True)
    else:
        return url.to_string(True, False, False, False)


def check_ftp_url(*args) -> str:
    # conn = "ftp://username:password@hostname:10001/path#PASV"
    url = URL(args[0])
    if url.scheme != "ftp":
        raise ValueError
    if url.port is None:
        url.port = 21
        url.netloc = "{0}:{1}".format(url.netloc, url.port)
    return url.to_string(True, False, False, True)


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
    conn = "{0}#{1}#{2}#{3}".format("tomail://receiver_mail_username@receive_mail_hostname",
                                    "smtp://sender_username:sender_password@sender_hostname:port",
                                    "frommail://sender_mail_username_2@senderhostname_2",
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
    conn = "oracle://username:password@hostname:port/service"
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


if __name__ == '__main__':
    u = EMPTY_URL
    print(u)

