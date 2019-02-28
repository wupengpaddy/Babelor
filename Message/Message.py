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
from urllib.parse import urlparse, unquote
from datetime import datetime
import json

DatetimeFmt = '%Y-%m-%d %H:%M:%S.%f'
CODING = 'utf-8'


def get_current_datetime():
    return datetime.now().strftime(DatetimeFmt)


class URL:
    def __init__(self, *args):
        if len(args) > 1:
            raise AttributeError
        if not isinstance(args[0], str):
            raise ValueError
        if "port" in args[0]:
            self.url = urlparse(args[0].replace("port", "0"))
        else:
            self.url = urlparse(args[0])
        self.scheme = self.url.scheme
        self.username = self.url.username
        self.password = self.url.password
        self.hostname = self.url.hostname
        self.port = self.url.port
        self.path = self.url.path
        self.query = self.url.query
        try:
            self.fragment = URL(unquote(self.url.fragment))
            self.fragment_is_url = True
        except RecursionError:
            self.fragment = self.url.fragment
            self.fragment_is_url = False

    def __str__(self):
        return self.url.geturl()


class MSG:
    def __init__(self, org, dst, case: str, activity: str, encrypt: str, data):
        self.timestamp = get_current_datetime()
        self.origination = check_url(org)
        self.destination = check_url(dst)
        self.case = case
        self.activity = activity
        self.encryption = encrypt
        if isinstance(data, str):
            self.data = json.load(data)
        elif isinstance(data, dict):
            self.data = data
        else:
            raise ValueError

    def __str__(self):
        return json.dumps({
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
        }, ensure_ascii=False)


def check_url(*args):
    if isinstance(args[0], str):
        return URL(args[0])
    elif isinstance(args[0], URL):
        return args[0]
    else:
        raise ValueError
