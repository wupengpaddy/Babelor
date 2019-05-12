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
# Outer Required
import pandas as pd
import numpy as np
# Inner Required
from Babelor.Presentation.UniformResourceIdentifier import URL, url_null_keep
from Babelor.Tools import dict2json, json2dict
# Global Parameters
from Babelor.Config import CONFIG


class DATUM:
    def __init__(self):
        self.count = 0
        self.stream = None
        self.coding = None
        self.path = None
        self.type = None

    def add(self, datum, path: (str, URL) = None):
        dt = datum_to_stream(datum)
        path = url_null_keep(path)
        if self.count == 0:
            self.stream = [dt["stream"], ]
            self.coding = [dt["coding"], ]
            self.path = [path, ]
            self.type = [dt["type"], ]
        else:
            self.stream = self.stream + [dt["stream"], ]
            self.coding = self.coding + [dt["coding"], ]
            self.path = self.path + [path, ]
            self.type = self.type + [dt["type"], ]
        self.count = self.count + 1

    def remove(self, idx: int):
        if self.__dict__["count"] == 0:
            pass
        elif self.__dict__["count"] == 1:
            self.count = 0
            for k in ["coding", "dtype", "path", "stream"]:
                self.__dict__[k] = None
        else:
            del self.__dict__["stream"][idx]
            del self.__dict__["coding"][idx]
            del self.__dict__["path"][idx]
            del self.__dict__["type"][idx]
            self.count = self.__dict__["count"] - 1
    
    def read(self, idx: int):
        if self.count == 0:
            return {"stream": None, "path": None}
        else:
            return {
                "stream": stream_to_datum(self.__dict__["stream"][idx - 1], self.__dict__["coding"][idx - 1],
                                          self.__dict__["dtype"][idx - 1]),
                "path": self.__dict__["path"][idx - 1],
            }


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
        return pd.read_msgpack(base64.b64decode(stream.encode(coding)))
    elif dtype in ["numpy.ndarray"]:
        code = json2dict(coding)
        rt = np.frombuffer(base64.decodebytes(stream.encode("ascii")), dtype=np.dtype(code["dtype"]))
        return np.reshape(rt, code["shape"])
    else:
        return None
