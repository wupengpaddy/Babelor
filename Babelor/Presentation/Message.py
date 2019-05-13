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
import logging
# Outer Required
# Inner Required
from Babelor.Tools import dict2json, json2dict, dict2xml, xml2dict, dict2msgpack, msgpack2dict
from Babelor.Presentation.UniformResourceIdentifier import URL, url_null_keep
from Babelor.Presentation.Case import CASE, current_datetime, case_null_keep
from Babelor.Presentation.Datum import DATUM, datum_null_keep
from Babelor.Presentation.Arguments import ARGS, args_null_keep
# Global Parameters
from Babelor.Config import CONFIG


class MSG:
    def __init__(self, msg: str = None):
        self.timestamp = current_datetime()     # 时间戳        Time Stamp
        self.origination = None                 # 来源节点      Source Node
        self.encryption = None                  # 加/解密节点   Encrypted Node
        self.treatment = None                   # 计算节点      Computing Node
        self.destination = None                 # 目标节点      Target Node
        self.case = None                        # 实例          Instance
        self.activity = None                    # 步骤          Step in Instance
        self.dt_count = 0                       # 数据数量      Data count
        self.data = None                        # 数据          Data
        self.args_count = 0                     # 参数数量      Arguments count
        self.arguments = None                   # 参数          Arguments
        if isinstance(msg, str):
            if CONFIG.MSG_TPE in ["json"]:
                self.from_json(msg)
            elif CONFIG.MSG_TPE in ["xml"]:
                self.from_xml(msg)
            elif CONFIG.MSG_TPE in ["msgpack"]:
                self.from_msgpack(msg)
            else:
                logging.warning("Defined serialization patterns:{0} are not supported.".format(CONFIG.MSG_TPE))
                raise NotImplementedError("Serialization support xml, json and msgpack only.")

    def __str__(self):
        return self.to_string()

    __repr__ = __str__

    def __setattr__(self, key, value):
        self.__dict__[key] = value
        if key not in ["timestamp"]:
            if "timestamp" in self.__dict__.keys():
                self.__dict__["timestamp"] = current_datetime()

    def update(self):
        self.__dict__["timestamp"] = current_datetime()

    def to_dict(self) -> dict:
        return {
            "head": {
                "timestamp": self.timestamp,        # 时间戳        Time Stamp
                "origination": self.origination,    # 来源节点      Source Node
                "encryption": self.encryption,      # 加/解密节点   Encrypted Node
                "treatment": self.treatment,        # 计算节点      Computing Node
                "destination": self.destination,    # 目标节点      Target Node
                "case": self.case,                  # 实例          Instance
                "activity": self.activity,          # 步骤          Step in Instance
            },
            "body": {
                "dt_count": self.dt_count,          # 数据数量      Data count
                "data": self.data,                  # 数据          Data
                "args_count": self.args_count,      # 参数数量      Arguments count
                "arguments": self.arguments,        # 参数          Arguments
            },
        }

    def to_serialize(self) -> dict:
        return {
            "head": {
                "timestamp": self.timestamp,                        # 时间戳        Time Stamp
                "origination": url_null_keep(self.origination),     # 来源节点      Source Node
                "encryption": url_null_keep(self.encryption),       # 加/解密节点   Encrypted Node
                "treatment": url_null_keep(self.treatment),         # 计算节点      Computing Node
                "destination": url_null_keep(self.destination),     # 目标节点      Target Node
                "case": case_null_keep(self.case),                  # 实例          Instance
                "activity": self.activity,                          # 步骤          Step in Instance
            },
            "body": {
                "dt_count": self.dt_count,                          # 数据数量      Data count
                "data": datum_null_keep(self.data),                 # 数据          Data
                "args_count": self.args_count,                      # 参数数量      Arguments count
                "arguments": args_null_keep(self.arguments),        # 参数          Arguments
            },
        }

    def to_string(self):
        if CONFIG.MSG_TPE in ["json"]:
            return self.to_json()
        elif CONFIG.MSG_TPE in ["xml"]:
            return self.to_xml()
        elif CONFIG.MSG_TPE in ["msgpack"]:
            return self.to_msgpack()
        else:
            raise NotImplementedError("Serialization support xml, json and msgpack only.")

    def to_json(self) -> str:
        return dict2json(self.to_serialize())

    def to_xml(self) -> str:
        return dict2xml(self.to_serialize())

    def to_msgpack(self) -> str:
        return dict2msgpack(self.to_serialize())

    def _from_dict_key(self, key: str, dt: dict, cls: classmethod, default: object = None):
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

    def from_dict(self, dt: dict):
        # set value from msg head --------------------------------------------
        if "head" in dt.keys():
            self._from_dict_key("timestamp", dt["head"], str, current_datetime())
            self._from_dict_key("origination", dt["head"], URL, None)
            self._from_dict_key("encryption", dt["head"], URL, None)
            self._from_dict_key("treatment", dt["head"], URL, None)
            self._from_dict_key("destination", dt["head"], URL, None)
            self._from_dict_key("case", dt["head"], CASE, None)
            self._from_dict_key("activity", dt["head"], str)
        else:
            self.__dict__["timestamp"] = current_datetime()
            self.__dict__["origination"] = None
            self.__dict__["encryption"] = None
            self.__dict__["treatment"] = None
            self.__dict__["destination"] = None
            self.__dict__["case"] = None
            self.__dict__["activity"] = None
        # set value from msg body --------------------------------------------
        if "body" in dt.keys():
            self._from_dict_key("dt_count", dt["body"], int, 0)
            self._from_dict_key("data", dt["body"], DATUM, None)
            self._from_dict_key("args_count", dt["body"], int, 0)
            self._from_dict_key("arguments", dt["body"], ARGS, None)
        else:
            self.__dict__["dt_count"] = 0
            self.__dict__["data"] = None
            self.__dict__["args_count"] = 0
            self.__dict__["arguments"] = None

    def from_json(self, msg: str):
        self.from_dict(json2dict(msg))

    def from_xml(self, msg: str):
        self.from_dict(xml2dict(msg))

    def from_msgpack(self, msg: str):
        self.from_dict(msgpack2dict(msg))

    def add_datum(self, datum: object, path=None):
        if self.dt_count == 0:
            self.data = DATUM()
        self.data.add(datum=datum, path=path)
        self.dt_count += 1

    def read_datum(self, idx: int):
        if (self.dt_count > 0) and (idx < self.dt_count):
            return self.data.read(idx=idx)
        else:
            return None

    def remove_datum(self, idx: int):
        if (idx < self.dt_count) and (self.dt_count > 0):
            self.data.remove(idx=idx)
            self.dt_count -= 1
        if (self.data is not None) and (self.dt_count == 0):
            self.data = None

    def clean_datum(self):
        if self.dt_count > 0:
            self.data.clean()
            self.dt_count = 0
            self.data = None

    def add_args(self, argument: object, path=None):
        if self.args_count == 0:
            self.arguments = ARGS()
        self.arguments.add(arguments=argument, path=path)
        self.args_count += 1

    def read_args(self, idx: int):
        if (self.dt_count > 0) and (idx < self.dt_count):
            return self.arguments.read(idx)
        else:
            return None

    def remove_args(self, idx: int):
        if (idx < self.args_count) and (self.args_count > 0):
            self.arguments.remove(idx=idx)
            self.args_count -= 1
        if (self.arguments is not None) and (self.args_count == 0):
            self.arguments = None

    def clean_args(self):
        if self.args_count > 0:
            self.arguments.clean()
            self.args_count = 0
            self.arguments = None
