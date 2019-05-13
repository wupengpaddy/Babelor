#! /usr/bin/env python3
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
import base64
import logging
# Outer Required
import pandas as pd
import numpy as np
# Inner Required
from Babelor.Presentation.UniformResourceIdentifier import URL, url_null_keep
from Babelor.Tools import dict2json, json2dict, dict2xml, xml2dict, msgpack2dict, dict2msgpack
# Global Parameters
from Babelor.Config import CONFIG


class DATUM:
    def __init__(self, datum=None):
        self.count = 0
        self.stream = []      # 数据流       [str, str]
        self.coding = []      # 编码         [<json>, <json>]
        self.path = []        # 路径         [(str, URL), (str, URL)]
        self.type = []        # 类型         [str, str]
        if isinstance(datum, dict):
            self.from_dict(datum)
        elif isinstance(datum, str):
            if CONFIG.MSG_TPE in ["json"]:
                self.from_json(datum)
            elif CONFIG.MSG_TPE in ["xml"]:
                self.from_xml(datum)
            elif CONFIG.MSG_TPE in ["msgpack"]:
                self.from_msgpack(datum)
            else:
                logging.warning("Defined serialization patterns:{0} are not supported.".format(CONFIG.MSG_TPE))
                raise NotImplementedError("Serialization support xml, json and msgpack only.")

    def add(self, datum, path: (str, URL) = None):
        dt = datum_to_stream(datum)
        path = url_null_keep(path)
        self.stream += [dt["stream"], ]
        self.coding += [dt["coding"], ]
        self.path += [path, ]
        self.type += [dt["type"], ]
        self.count += 1

    def remove(self, idx: int):
        if (idx < self.count) and (self.count > 0):
            del self.stream[idx]
            del self.coding[idx]
            del self.path[idx]
            del self.type[idx]
            self.count -= 1
    
    def read(self, idx: int):
        if (idx < self.count) and (self.count > 0):
            return {
                "stream": stream_to_datum(self.stream[idx], self.coding[idx], self.type[idx]),
                "path": self.path[idx],
            }
        else:
            return {
                "stream": None,
                "path": None,
            }

    def clean(self):
        for k in self.__dict__.keys():
            if k in ["count"]:
                self.__dict__[k] = 0
            else:
                self.__dict__[k] = []

    def __str__(self):
        return self.to_string()

    __repr__ = __str__

    def to_serialize(self):
        return {
            "stream": self.stream,
            "coding": self.coding,
            "path": self.path,
            "type": self.type,
        }

    def to_string(self):
        if CONFIG.MSG_TPE in ["json"]:
            return self.to_json()
        elif CONFIG.MSG_TPE in ["xml"]:
            return self.to_xml()
        elif CONFIG.MSG_TPE in ["msgpack"]:
            return self.to_msgpack()
        else:
            logging.warning("Defined serialization patterns:{0} are not supported.".format(CONFIG.MSG_TPE))
            raise NotImplementedError("Serialization support xml, json and msgpack only.")

    def to_json(self):
        return dict2json(self.to_serialize())

    def to_xml(self):
        return dict2xml(self.to_serialize())

    def to_msgpack(self):
        return dict2msgpack(self.to_serialize())

    def from_json(self, msg: str):
        self.from_dict(json2dict(msg))

    def from_xml(self, msg: str):
        self.from_dict(xml2dict(msg))

    def from_msgpack(self, msg: str):
        self.from_dict(msgpack2dict(msg))

    def from_dict(self, dt: dict):
        self.stream = dt["stream"]
        self.coding = dt["coding"]
        self.path = dt["path"]
        self.type = dt["type"]
        self.count = len(self.type)


def datum_to_stream(datum: (str, pd.DataFrame, bytes, bytearray, None)) -> dict:
    """
    :param datum: (str, pd.DataFrame, bytes, bytearray, None)
    :return: rt:
    {
        "stream": [None, str],
        "coding": [None, "ascii"],
        "type": [None, "str", "base64", "pandas.core.frame.DataFrame"]
    }
    """
    if datum is None:
        rt = {
            "stream": None,
            "coding": None,
            "type": None,
        }
    elif isinstance(datum, str):
        rt = {
            "stream": base64.b64encode(datum.encode(CONFIG.Coding)).decode("ascii"),
            "coding": CONFIG.Coding,
            "type": "str",
        }
    elif isinstance(datum, pd.DataFrame):
        rt = {
            "stream": base64.b64encode(datum.to_msgpack()).decode("ascii"),
            "coding": None,
            "type": "pandas.core.frame.DataFrame",
        }
    elif isinstance(datum, np.ndarray):
        rt = {
            "stream": base64.b64encode(datum).decode("ascii"),
            "coding": dict2json({
                "dtype": str(datum.dtype),
                "shape": datum.shape,
             }),
            "type": "numpy.ndarray",
        }
    elif isinstance(datum, (bytes, bytearray)):
        rt = {
            "stream": base64.b64encode(datum).decode("ascii"),
            "coding": "ascii",
            "type": "base64",
        }
    else:
        rt = {
            "stream": None,
            "coding": None,
            "type": None,
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
        return base64.b64decode(stream.encode("ascii"))
    elif dtype in ["str"]:
        return base64.b64decode(stream.encode("ascii")).decode(coding)
    elif dtype in ["pandas.core.frame.DataFrame"]:
        df = pd.read_msgpack(base64.b64decode(stream.encode("ascii")))
        if CONFIG.IS_SQL_DATA_STRING:
            if isinstance(df, pd.DataFrame):
                df = df.applymap(str)
        return df
    elif dtype in ["numpy.ndarray"]:
        dt_coding = json2dict(coding)
        rt = np.frombuffer(base64.decodebytes(stream.encode("ascii")), dtype=np.dtype(dt_coding["dtype"]))
        return np.reshape(rt, dt_coding["shape"])
    else:
        return None


def datum_null_keep(item: object, item_type: classmethod = str) -> object:
    if item is None:
        return None
    elif isinstance(item, DATUM):
        return item.to_serialize()
    else:
        return item_type(item)
