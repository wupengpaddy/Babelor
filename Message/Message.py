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


class URL:
    def __init__(self, url: str):
        url = urlparse(url)
        self.scheme = url.scheme
        self.username = url.username
        self.password = url.password
        self.hostname = url.hostname
        self.port = url.port
        self.netloc = url.netloc
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
                "encryption": self.encryption,
                "data": self.data,
            }
        })


def check_url(*args):
    if len(args) > 1:
        raise AttributeError
    if isinstance(args[0], str):
        return URL(args[0])
    elif isinstance(args[0], URL):
        return args[0]
    else:
        raise ValueError


def demo():
    # b = urlparse("scheme://username:password@host:10/path;params?query2#fragment")
    # b = urlunparse(("scheme", "username:password@host:10", "path", "params", "query", "fragment"))
    b = URL("tomail://zhangpeng@shairport.com#smtp://tanghailing:65684446Mail@172.21.98.66:10001#唐海铃")
    b.scheme = "orcale"
    print("URL:", b)
    print("目标协议:", b.scheme)
    print("收件人:", b.netloc)
    print("发送协议:", b.fragment.scheme)
    print("服务用户:", b.fragment.username)
    print("服务密码:", b.fragment.password)
    print("主机地址", b.fragment.hostname)
    print("主机端口", b.fragment.port)
    print("邮件表示", b.fragment.fragment)


if __name__ == '__main__':
    demo()
