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

# 外部依赖
from urllib.parse import urlparse, unquote, urlunparse, quote, parse_qs, urlencode
import re
# 内部依赖
from Babelor.Config import GLOBAL_CFG
# 全局参数
DatetimeFmt = GLOBAL_CFG["DatetimeFormat"]
PortFmt = GLOBAL_CFG["PortFormat"]


class URL:
    def __init__(self, url=None):
        """
        Parse a string into a URL components:
        <scheme>://<username>:<password>@<hostname>:<port>/<path>;<params>?<query>#<fragment>
        """
        self.scheme = ""
        self.username = None
        self.password = None
        self.hostname = None
        self.port = None
        self.netloc = ""
        self.path = ""
        self.params = ""
        self.query = ""
        self.fragment = ""
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

    def _decompose_netloc(self):
        if not isinstance(self.__dict__["netloc"], str):
            raise ValueError("netloc 错误赋值")
        # ----------------------------------------------------------------------
        if "@" in self.__dict__["netloc"]:
            auth, location = self.__dict__["netloc"].split("@")
        else:
            auth = None
            location = self.__dict__["netloc"]
        # ----------------------------------------------------------------------
        if auth is None:
            self.__dict__["username"] = None
            self.__dict__["password"] = None
        else:
            if ":" in auth:
                self.__dict__["username"], self.__dict__["password"] = auth.split(":")
            else:
                self.__dict__["username"] = auth
                self.__dict__["password"] = None
        # ----------------------------------------------------------------------
        if ":" in location:
            self.__dict__["hostname"], self.__dict__["port"] = location.split(":")
        else:
            self.__dict__["hostname"] = location
        # ----------------------------------------------------------------------
        del location, auth

    def _compose_netloc(self):
        if self.__dict__["password"] is None:
            if self.__dict__["username"] is None:
                auth = None
            else:
                auth = self.__dict__["username"]
        else:
            auth = "{0}:{1}".format(self.__dict__["username"], self.__dict__["password"])
        # ----------------------------------------------------------------------
        if self.__dict__["port"] is None:
            location = self.__dict__["hostname"]
        else:
            location = "{0}:{1}".format(self.__dict__["hostname"], self.__dict__["port"])
        # ----------------------------------------------------------------------
        if auth is None:
            self.__dict__["netloc"] = location
        else:
            self.__dict__["netloc"] = "{0}@{1}".format(auth, location)
        # ----------------------------------------------------------------------
        del auth, location

    def to_string(self, allow_path=True, allow_params=True, allow_query=True, allow_fragment=True) -> str:
        path, params, query, fragment = "", "", "", ""
        # ----------------------------------------------------------------------
        if allow_path:
            path = self.path
        # ----------------------------------------------------------------------
        if allow_params:
            params = self.params
        # ----------------------------------------------------------------------
        if allow_query:
            if self.query in [""]:
                query = ""
            else:
                query = urlencode(self.query)
        # ----------------------------------------------------------------------
        if allow_fragment:
            if isinstance(self.fragment, URL):
                fragment = quote(self.fragment.to_string())
            else:
                fragment = quote(self.fragment)
        # ----------------------------------------------------------------------
        return urlunparse((self.scheme, self.netloc, path, params, query, fragment))

    def from_string(self, string: str):
        if re.search(r":port#", string) is not None:
            url_string = re.sub(r":port#", "#", string)
            default_port = "port"
        elif re.search(r":port/", string) is not None:
            url_string = re.sub(r":port/", "/", string)
            default_port = "port"
        elif re.search(r":port$", string) is not None:
            url_string = re.sub(r":port$", "", string)
            default_port = "port"
        else:
            url_string = string
            default_port = None
        # ----------------------------------------------------------------------
        url = urlparse(url_string)
        del url_string
        # ----------------------------------------------------------------------
        self.scheme = url.scheme
        # ----------------------------------------------------------------------
        self.username = url.username
        # ----------------------------------------------------------------------
        self.password = url.password
        # ----------------------------------------------------------------------
        self.hostname = url.hostname
        # ----------------------------------------------------------------------
        if default_port is None:
            self.port = url.port
        else:
            self.port = default_port
        del default_port
        # ----------------------------------------------------------------------
        self.path = re.sub(r'^/', "", re.sub(r'/$', "", url.path))
        # ----------------------------------------------------------------------
        self.params = url.params
        # ----------------------------------------------------------------------
        if url.query in [""]:
            self.query = ""
        else:
            self.query = dict([(k, v[0]) for k, v in parse_qs(url.query).items()])
        # ----------------------------------------------------------------------
        if url.fragment in [""]:
            self.fragment = ""
        else:
            try:
                self.fragment = URL(unquote(url.fragment))
            except RecursionError:
                self.fragment = url.fragment

    @property
    def check(self):
        default_port = None
        check_url = URL(self.to_string())
        if self.scheme in ["mysql"]:
            # "mysql://<username>:<password>@<hostname>:<port>/<path>"
            # "mysql+pymysql://<username>:<password>@<hostname>:3306/<path>"
            check_url.scheme = "mysql+pymysql"
            default_port = 3306
        if self.scheme in ["oracle"]:
            # "oracle://<username>:<password>@<hostname>:<port>/<path>"
            # "oracle+cx_oracle://<username>:<password>@<hostname>:1521/<path>"
            check_url.scheme = "oracle+cx_oracle"
            default_port = 1521
        if self.scheme in ["ftp", "ftpd"]:
            # "ftp://<username>:<password>@<hostname>:<port>#?model=PASV
            # "ftp://<username>:<password>@<hostname>:21#?model=PASV
            # "ftp://<username>:<password>@<hostname>:<port>"
            # "ftp://<username>:<password>@<hostname>:21#?model=PORT
            default_port = 21
        if self.scheme in ["smtp"]:
            # "smtp://<username>:<password>@<hostname>:<port>"
            # "smtp://<username>:<password>@<hostname>:25"
            default_port = 25
        if self.scheme in ["pop3"]:
            # "pop://<username>:<password>@<hostname>:<port>"
            # "pop://<username>:<password>@<hostname>:110"
            default_port = 110
        if self.scheme in ["http"]:
            # "http://<username>:<password>@<hostname>:<port>"
            # "http://<username>:<password>@<hostname>:80"
            default_port = 80
        if self.scheme in ["file"]:
            # "file://<username>:<password>@<hostname>/<path>"
            default_port = None
        if self.scheme in ["tcp"]:
            # "tcp://<hostname>:<port>"
            default_port = 10001
        # ----------------------------------------------------------------------
        if re.match(PortFmt, str(self.port)) is None:
            if default_port is not None:
                check_url.port = default_port
            else:
                check_url.port = None
        # ----------------------------------------------------------------------
        if isinstance(check_url.fragment, URL):
            check_url.fragment = check_url.fragment.check
        return check_url

    def init(self, scheme=None):
        if scheme in ["ftp"]:
            self.__init__("ftp://username:password@hostname:port#?model=PASV")
        if scheme in ["ftpd"]:
            self.__init__("ftpd://username:password@hostname:port/path#?model=PASV")
        if scheme in ["mysql"]:
            self.__init__("mysql://username:password@hostname:port/path")
        if scheme in ["oracle"]:
            self.__init__("oracle://username:password@hostname:port/path")
        if scheme in ["tomail+smtp"]:
            smtp_conn = "{0}#{1}".format("smtp://sender_username:sender_password@sender_hostname:port",
                                         quote("tomail://sender_mail_username@sender_mail_postfix/发件人"))
            self.__init__("{0}#{1}".format("tomail://receiver_mail_username@receive_mail_postfix/收件人",
                                           quote(smtp_conn)))
        if scheme in ["tcp"]:
            self.__init__("tcp://hostname:port")
        if scheme in ["http"]:
            self.__init__("http://username:password@hostname:port/path;params?query#fragment")
        if scheme in ["https"]:
            self.__init__("https://username:password@hostname:port/path;params?query#fragment")
        if scheme in ["file"]:
            self.__init__("file://username:password@hostname/path;params")
        return self
