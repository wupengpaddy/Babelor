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
import base64
# Outer Required
import pandas as pd
# Inner Required
from Babelor.Tools import dict2json, json2dict, dict2xml, xml2dict
from Babelor.Presentation.UniformResourceIdentifier import URL
from Babelor.Presentation.Case import CASE
# Global Parameters
from Babelor.Config import CONFIG


def current_datetime() -> str:
    return datetime.now().strftime(CONFIG.Datetime_FMT)


def null_keep(item: object, item_type: classmethod = str) -> object:
    if item is None:
        return None
    elif isinstance(item, item_type):
        return item
    else:
        return item_type(item)


class MSG:
    def __init__(self, msg: str = None):
        self.timestamp = current_datetime()     # 时间戳        -   Time Stamp
        self.origination = None                 # 来源节点      -   Source Node
        self.destination = None                 # 目标节点      -   Target Node
        self.case = None                        # 实例          -   Instance
        self.activity = None                    # 步骤          -   Step in Instance
        self.treatment = None                   # 计算节点      -   Computing Node
        self.encryption = None                  # 加/解密节点   -   Encrypted Node
        self.nums = 0                           # 参数个数      -   Number of Arguments
        self.coding = None                      # 文字编码      -   Character Coding
        self.dtype = None                       # 数据编码      -   Data Encoding
        self.stream = None                      # 数据流        -   Data Stream
        self.path = None                        # 路径          -   Path
        if isinstance(msg, str):
            if CONFIG.MSG_TPE in ["json"]:
                self.from_json(msg)
            elif CONFIG.MSG_TPE in ["xml"]:
                self.from_xml(msg)
            else:
                raise NotImplementedError("仅支持 xml 和 json 类消息")

    def __str__(self):
        return self.to_string()

    __repr__ = __str__

    def __setattr__(self, key, value):
        self.__dict__[key] = value
        keys = ["coding", "dtype", "path", "stream"]
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

    def update(self):
        self.timestamp = current_datetime()

    def to_dict(self):
        return {
            "head": {
                "timestamp": self.timestamp,        # 时间戳        -   Time Stamp
                "origination": self.origination,    # 来源节点      -   Source Node
                "destination": self.destination,    # 目标节点      -   Target Node
                "treatment": self.treatment,        # 计算节点      -   Computing Node
                "encryption": self.encryption,      # 加/解密节点   -   Encrypted Node
                "case": self.case,                  # 实例          -   Instance
                "activity": self.activity,          # 步骤          -   Step in Instance
            },
            "body": {
                "nums": self.nums,                  # 参数个数      -   Number of Arguments
                "coding": self.coding,              # 文字编码      -   Character Coding
                "dtype": self.dtype,                # 数据编码      -   Data Encoding
                "path": self.path,                  # 路径          -   Path
                "stream": self.stream,              # 数据流        -   Data Stream
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
                "dtype": self.dtype,
                "path": self.path,
                "stream": self.stream,
            },
        }

    def to_string(self):
        if CONFIG.MSG_TPE in ["json"]:
            return self.to_json()
        elif CONFIG.MSG_TPE in ["xml"]:
            return self.to_xml()
        else:
            raise NotImplementedError("Support xml and json pattern only.")

    def to_json(self):
        return dict2json(self.to_serialize())

    def to_xml(self):
        return dict2xml(self.to_serialize())

    def _from_key_url(self, key: str, dt: dict, cls: classmethod, default: object = None):
        if key in dt.keys():
            if dt[key] is None:
                self.__dict__[key] = default
            else:
                if isinstance(dt[key], cls):
                    self.__dict__[key] = dt[key]
                else:
                    self.__dict__[key] = cls(dt[key])
        else:
            self.__dict__[key] = default

    def from_dict(self, msg: dict):
        # set value from msg head
        if "head" in msg.keys():
            # set timestamp from msg head ------------------------------------
            self._from_key_url("timestamp", msg["head"], str, current_datetime())
            # set origination from msg head ----------------------------------
            self._from_key_url("origination", msg["head"], URL, None)
            # set destination from msg head ----------------------------------
            self._from_key_url("destination", msg["head"], URL, None)
            # set treatment from msg head ------------------------------------
            self._from_key_url("treatment", msg["head"], URL, None)
            # set encryption from msg head -----------------------------------
            self._from_key_url("encryption", msg["head"], URL, None)
            # set case from msg head -----------------------------------------
            self._from_key_url("case", msg["head"], CASE, None)
            # set activity from msg head -------------------------------------
            self._from_key_url("activity", msg["head"], str)
        else:
            self.__dict__["timestamp"] = current_datetime()
            self.__dict__["origination"] = None
            self.__dict__["destination"] = None
            self.__dict__["treatment"] = None
            self.__dict__["encryption"] = None
            self.__dict__["case"] = None
            self.__dict__["activity"] = None
        # set value from msg body
        if "body" in msg.keys():
            # set nums from msg body -----------------------------------------
            self._from_key_url("nums", msg["body"], int, None)
            # set coding from msg body ---------------------------------------
            self._from_key_url("coding", msg["body"], list, None)
            # set path from msg body -----------------------------------------
            self._from_key_url("path", msg["body"], list, None)
            # set dtype from msg body ----------------------------------------
            self._from_key_url("dtype", msg["body"], list, None)
            # set stream from msg body ---------------------------------------
            self._from_key_url("stream", msg["body"], list, None)
        else:
            self.__dict__["nums"] = 0
            self.__dict__["coding"] = None
            self.__dict__["path"] = None
            self.__dict__["dtype"] = None
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

    def add_datum(self, datum, path=None):
        dt = datum_to_stream(datum)
        path = null_keep(path)
        if self.__dict__["nums"] == 0:
            self.__dict__["stream"] = [dt["stream"], ]
            self.__dict__["coding"] = [dt["coding"], ]
            self.__dict__["path"] = [path, ]
            self.__dict__["dtype"] = [dt["dtype"], ]
        else:
            self.__dict__["stream"] = self.__dict__["stream"] + [dt["stream"], ]
            self.__dict__["coding"] = self.__dict__["coding"] + [dt["coding"], ]
            self.__dict__["path"] = self.__dict__["path"] + [path, ]
            self.__dict__["dtype"] = self.__dict__["dtype"] + [dt["dtype"], ]
        self.__dict__["nums"] = self.__dict__["nums"] + 1

    def read_datum(self, num: int):
        if self.__dict__["nums"] == 0:
            return {"stream": None, "path": None}
        else:
            return {
                "stream": stream_to_datum(self.__dict__["stream"][num - 1], self.__dict__["coding"][num - 1],
                                          self.__dict__["dtype"][num - 1]),
                "path": self.__dict__["path"][num - 1],
            }

    def del_datum(self, num: int):
        if self.__dict__["nums"] == 0:
            pass
        elif self.__dict__["nums"] == 1:
            self.nums = 0
        else:
            del self.__dict__["stream"][num]
            del self.__dict__["coding"][num]
            del self.__dict__["path"][num]
            del self.__dict__["dtype"][num]
            self.nums = self.__dict__["nums"] - 1


def datum_to_stream(datum: (str, pd.DataFrame, bytes) = None):
    """
    :param datum: []
    :return: rt:
    {
        "stream": [None, str],
        "coding": [None, "ascii"],
        "dtype": [None, "str", "base64", "pandas.core.frame.DataFrame"]
    }
    """
    if datum is None:
        rt = {
            "stream": None,
            "coding": None,
            "dtype": None,
        }
    elif isinstance(datum, str):
        rt = {
            "stream": datum,
            "coding": None,
            "dtype": "str",
        }
    elif isinstance(datum, pd.DataFrame):
        rt = {
            "stream": base64.b64encode(datum.to_msgpack()).decode("ascii"),
            "coding": "ascii",
            "dtype": "pandas.core.frame.DataFrame",
        }
    else:
        rt = {
            "stream": base64.b64encode(datum).decode("ascii"),
            "coding": "ascii",
            "dtype": "base64",
        }
    return rt


def stream_to_datum(stream: str = None, coding: str = None, dtype: str = None):
    """
    :param stream:[None, str]
    :param coding:[None, str]   ("ascii", None)
    :param dtype:[None, str]    ("base64", "str", "pandas.core.frame.DataFrame", None)
    :return: [None, str, bytes, pd.DataFrame]
    """
    if dtype in ["base64"]:
        return base64.b64decode(stream.encode(coding))
    elif dtype in ["str"]:
        return stream
    elif dtype in ["pandas.core.frame.DataFrame"]:
        return pd.read_msgpack(base64.b64decode(stream.encode(coding)))
    else:
        return None
