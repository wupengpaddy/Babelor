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

# System Required
from datetime import datetime
from urllib.parse import unquote, quote
# Outer Required
# Inner Required
from Babelor.Presentation.UniformResourceIdentifier import URL
# Global Parameters
from Babelor.Config import CONFIG


def current_datetime() -> str:
    return datetime.now().strftime(CONFIG.Datetime_FMT)


class CASE:
    def __init__(self, case=None):
        self.origination = None
        self.destination = None
        self.timestamp = current_datetime()
        if isinstance(case, str):
            self.from_string(case)

    def __str__(self):
        return self.to_string()

    __repr__ = __str__

    def to_string(self):
        return "{0}#{1}#{2}".format(quote(str(self.origination)), quote(str(self.destination)), quote(self.timestamp))

    def from_string(self, case: str):
        case = case.split("#")
        if len(case) == 3:
            self.origination = URL(unquote(case[0]))
            self.destination = URL(unquote(case[1]))
            self.timestamp = unquote((case[2]))
        elif len(case) == 2:
            self.origination = URL(unquote(case[0]))
            self.destination = URL(unquote(case[1]))
            self.timestamp = current_datetime()
        else:
            raise ValueError("地址格式错误")
