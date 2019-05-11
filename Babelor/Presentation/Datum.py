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
# Global Parameters
from Babelor.Config import CONFIG


class DATUM:
    def __init__(self):
        self.count = 0
        self.stream = None
        self.coding = None
        self.path = None
        self.dtype = None

    def add(self, datum, path: (str, URL) = None):
        dt = datum_to_stream(datum)
        path = url_null_keep(path)
        if self.count == 0:
            self.stream = [dt["stream"], ]
            self.coding = [dt["coding"], ]
            self.path = [path, ]
            self.dtype = [dt["dtype"], ]
        else:
            self.stream = self.stream + [dt["stream"], ]
            self.coding = self.coding + [dt["coding"], ]
            self.path = self.path + [path, ]
            self.dtype = self.dtype + [dt["dtype"], ]
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
            del self.__dict__["dtype"][idx]
            self.count = self.__dict__["count"] - 1
    
    def read(self, idx: int):



def datum_to_stream(datum: (str, pd.DataFrame, bytes, bytearray, None)) -> dict:
    """
    :param datum: (str, pd.DataFrame, bytes, bytearray, None)
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
            "stream": base64.b64encode(datum.encode(CONFIG.Coding)).decode("ascii"),
            "coding": CONFIG.Coding,
            "dtype": "str",
        }
    elif isinstance(datum, pd.DataFrame):
        rt = {
            "stream": base64.b64encode(datum.to_msgpack()).decode("ascii"),
            "coding": None,
            "dtype": "pandas.core.frame.DataFrame",
        }
    elif isinstance(datum, np.ndarray):
        rt = {
            "stream": base64.b64encode(datum).decode("ascii"),
            "coding": None,
            "dtype": "numpy.ndarray",
        }
    elif isinstance(datum, (bytes, bytearray)):
        rt = {
            "stream": base64.b64encode(datum).decode("ascii"),
            "coding": "ascii",
            "dtype": "base64",
        }
    else:
        rt = {
            "stream": None,
            "coding": None,
            "dtype": None,
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
    else:
        return None
