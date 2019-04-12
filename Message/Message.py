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
import time
import re

DatetimeFmt = GLOBAL_CFG["DatetimeFormat"]
PortFmt = GLOBAL_CFG["PortFormat"]
CODING = GLOBAL_CFG["CODING"]
MSG_TYPE = GLOBAL_CFG["MSG_TYPE"]


def current_datetime() -> str:
    return datetime.now().strftime(DatetimeFmt)


class URL:
    def __init__(self, url=None):
        # scheme://username:password@hostname:port/path;params?query#fragment
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
        if isinstance(url, str):
            self.from_string(url)

    def __str__(self):
        return self.to_string()

    __repr__ = __str__

    def __eq__(self, other):
        if isinstance(other, URL):
            pass
        else:
            raise ValueError

    def __setattr__(self, key, value):
        self.__dict__[key] = value      # source update
        keys = ["port", "hostname", "username", "password"]     # relative update
        is_keys_init = len([False for k in keys if k not in list(self.__dict__.keys())]) == 0
        if (key in keys) and is_keys_init:
            self._compose_netloc()
        if key in ["netloc"]:
            self._decompose_netloc()

    def __getattr__(self, item):
        return self.__dict__[item]

    def _decompose_netloc(self):
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

    def _compose_netloc(self):
        if self.__dict__["password"] is None:
            if self.__dict__["username"] is None:
                auth = None
            else:
                auth = self.__dict__["username"]
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

    def from_string(self, string: str):
        default_port = None
        url_string = string
        if re.search(r":port#", string) is not None:
            url_string = re.sub(r":port#", "#", string)
            default_port = "port"
        if re.search(r":port/", string) is not None:
            url_string = re.sub(r":port/", "/", string)
            default_port = "port"
        if re.search(r":port$", string) is not None:
            url_string = re.sub(r":port$", "", string)
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
        # "file://username:password@hostname:port/path"
        if self.scheme in ["file"]:
            # "file://username:password@hostname/path"
            default_port = None
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
        if scheme in ["http"]:
            self.__init__("http://username:password@hostname:port")
        if scheme in ["file"]:
            self.__init__("file://username:password@hostname/path")
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


def null_keep(item: object, item_type: classmethod = str) -> object:
    if item is None:
        return None
    elif isinstance(item, item_type):
        return item
    else:
        return item_type(item)


class MSG:
    def __init__(self, msg=None):
        self.timestamp = current_datetime()     # 时间戳
        self.origination = None                 # 来源节点
        self.destination = None                 # 目标节点
        self.case = None                        # 案例
        self.activity = None                    # 事件
        self.treatment = None                   # 计算节点
        self.encryption = None                  # 加/解密节点
        self.nums = 0                           # 文件数量
        self.coding = None                      # 编码
        self.format = None                      # 格式
        self.stream = None                      # 流
        self.path = None                        # 路径
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
        self.__dict__[key] = value
        keys = ["coding", "format", "path", "stream"]
        is_keys_init = len([False for k in keys if k not in list(self.__dict__.keys())]) == 0
        if key in ["nums"]:
            if value == 0 and is_keys_init:
                for k in keys:
                    self.__dict__[k] = None
        if key in keys and is_keys_init:
            if isinstance(value, list) and self.__dict__["nums"] == 1:
                self.__dict__[key] = value[0]
            # print([False for k in keys if self.__dict__[k] is None])
        if key not in ["timestamp"]:
            self.__dict__["timestamp"] = current_datetime()

    def to_dict(self):
        return {
            "head": {
                "timestamp": self.timestamp,        # 时间戳
                "origination": self.origination,    # 来源    -   节点
                "destination": self.destination,    # 目标    -   节点
                "treatment": self.treatment,        # 计算    -   节点
                "encryption": self.encryption,      # 加/解密  -  节点
                "case": self.case,
                "activity": self.activity,
            },
            "body": {
                "nums": self.nums,
                "coding": self.coding,
                "format": self.format,
                "path": self.path,
                "stream": self.stream,
            },
        }

    def to_serialize(self):
        return {
            "head": {
                "timestamp": null_keep(self.timestamp),
                "origination": null_keep(self.origination),
                "destination": null_keep(self.destination),
                "treatment": null_keep(self.treatment),
                "encryption": null_keep(self.encryption),
                "case": null_keep(self.case),
                "activity": null_keep(self.activity),
            },
            "body": {
                "nums": self.nums,
                "coding": self.coding,
                "format": self.format,
                "path": self.path,
                "stream": self.stream,
            },
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
        def _value_check(key: str, item: dict, datum_type: classmethod = str):
            if key in item.keys():
                self.__dict__[key] = null_keep(item[key], datum_type)
            else:
                self.__dict__[key] = None

        if not isinstance(msg, dict):
            raise AttributeError("参数类型错误")
        if "head" in msg.keys():
            # set timestamp from msg
            _value_check("timestamp", msg["head"])
            if self.__dict__["timestamp"] is None:
                self.__dict__["timestamp"] = current_datetime()
            # set origination from msg
            _value_check("origination", msg["head"], URL)
            # set destination from msg
            _value_check("destination", msg["head"], URL)
            # set treatment from msg
            _value_check("treatment", msg["head"], URL)
            # set encryption from msg
            _value_check("encryption", msg["head"], URL)
            # set case from msg
            _value_check("case", msg["head"], CASE)
            # set activity from msg
            _value_check("activity", msg["head"])
        else:
            self.__dict__["timestamp"] = current_datetime()
            self.__dict__["origination"] = None
            self.__dict__["destination"] = None
            self.__dict__["treatment"] = None
            self.__dict__["encryption"] = None
            self.__dict__["case"] = None
            self.__dict__["activity"] = None
        if "body" in msg.keys():
            # set nums from msg
            _value_check("nums", msg["body"], int)
            # set coding from msg
            self.__dict__["coding"] = msg["body"]["coding"]
            # set path from msg
            self.__dict__["path"] = msg["body"]["path"]
            # set format from msg
            self.__dict__["format"] = msg["body"]["format"]
            # set stream from msg
            self.__dict__["stream"] = msg["body"]["stream"]
        else:
            self.__dict__["nums"] = 0
            self.__dict__["coding"] = None
            self.__dict__["path"] = None
            self.__dict__["format"] = None
            self.__dict__["stream"] = None

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

    def add_datum(self, ):
        if self.coding in ["base64"]:
            self.stream = base64.b64decode(self.__dict__["stream"])


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
    url = URL().init("mysql")
    url.check
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("数据服务", url.path)


def demo_oracle():
    url = URL().init("oracle")
    # url.check()
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("数据服务", url.path)


def demo_ftp():
    url = URL().init("ftp")
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("服务路径", url.path)
    print("服务模式", url.fragment)


def demo_tcp():
    url = URL().init("tcp")
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务地址", url.hostname)
    print("服务端口", url.port)


def demo_msg_mysql2ftp():
    msg = MSG()
    msg.origination = URL().init("mysql").check
    msg.destination = URL().init("ftp").check
    msg.treatment = URL().init("tcp")
    msg.activity = "init"
    print(msg)
    msg_string = str(msg)
    new_msg = MSG(msg_string)
    time.sleep(1/100)
    print(new_msg)
    time.sleep(1 / 100)
    new_msg.nums = 1
    print(new_msg)


if __name__ == '__main__':
    # demo_tomail()
    # demo_ftp()
    # demo_mysql()
    # demo_tcp()
    demo_msg_mysql2ftp()
