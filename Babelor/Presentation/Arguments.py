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
# Inner Required
from Babelor.Presentation.UniformResourceIdentifier import URL, url_null_keep
from Babelor.Tools import dict2json, json2dict, dict2xml, xml2dict
# Global Parameters
from Babelor.Config import CONFIG


class ARGS:
    def __init__(self, arguments=None):
        self.count = 0      # 参数数量  int
        self.stream = []    # 数据流    [str, str]
        self.path = []      # 路径      [(str, URL), (str, URL)]
        if isinstance(arguments, dict):
            self.from_dict(arguments)
        elif isinstance(arguments, str):
            if CONFIG.MSG_TPE in ["json"]:
                self.from_json(arguments)
            elif CONFIG.MSG_TPE in ["xml"]:
                self.from_xml(arguments)
            # elif CONFIG.MSG_TPE in ["msgpack"]:
            #     self.from_msgpack(datum)
            else:
                logging.warning("Defined serialization patterns:{0} are not supported.".format(CONFIG.MSG_TPE))
                raise NotImplementedError("Serialization support xml, json and msgpack only.")

    def add(self, arguments, path: (str, URL) = None):
        arguments_stream = base64.b64encode(arguments.encode(CONFIG.Coding)).decode("ascii")
        path = url_null_keep(path)
        self.stream += [arguments_stream, ]
        self.path += [path, ]
        self.count += 1

    def remove(self, idx: int):
        if (idx < self.count) and (self.count > 0):
            del self.stream[idx]
            del self.path[idx]
            self.count -= 1

    def read(self, idx: int):
        if (idx < self.count) and (self.count > 0):
            return {
                "stream": base64.b64decode(self.stream[idx].encode("ascii")).decode(CONFIG.Coding),
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
            "path": self.path,
        }

    def to_string(self):
        if CONFIG.MSG_TPE in ["json"]:
            return self.to_json()
        elif CONFIG.MSG_TPE in ["xml"]:
            return self.to_xml()
        # elif CONFIG.MSG_TPE in ["msgpack"]:
        #     return self.to_msgpack()
        else:
            logging.warning("Defined serialization patterns:{0} are not supported.".format(CONFIG.MSG_TPE))
            raise NotImplementedError("Serialization support xml, json and msgpack only.")

    def to_json(self):
        return dict2json(self.to_serialize())

    def to_xml(self):
        return dict2xml(self.to_serialize())

    def from_json(self, msg: str):
        self.from_dict(json2dict(msg))

    def from_xml(self, msg: str):
        self.from_dict(xml2dict(msg))

    def from_dict(self, dt: dict):
        self.stream = dt["stream"]
        self.path = dt["path"]
        self.count = len(self.path)


def args_null_keep(item: object, item_type: classmethod = str) -> object:
    if item is None:
        return None
    elif isinstance(item, ARGS):
        return item.to_serialize()
    else:
        return item_type(item)
