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

GLOBAL_CFG = {
    # 数据格式 及 编码
    "DateFormat": '%Y-%m-%d',
    "DatetimeFormat": "%Y-%m-%d %H:%M:%S.%f",
    "IPv4_Format": r"(?=(\b|\D))(((\d{1,2})|(1\d{1,2})|(2[0-4]\d)|(25[0-5]))\.){3}((\d{1,2})|(1\d{1,2})|(2[0-4]\d)|(25[0-5]))(?=(\b|\D))",
    "IPv6_Format": r"^([\\da-fA-F]{1,4}:){7}([\\da-fA-F]{1,4})$",
    "PortFormat": r"^([0-9]|[1-9][0-9]|[1-9][0-9][0-9]|[1-9][0-9][0-9][0-9]|[1-5][0-9][0-9][0-9][0-9]|6[0-4][0-9][0-9][0-9]|65[0-4][0-9][0-9]|655[0-2][0-9]|6553[0-5])$",
    "CODING": "utf-8",
    "NLS_LANG": "SIMPLIFIED CHINESE_CHINA.UTF8",
    "IS_STR_VALUE": "True",
    "MSG_TYPE": "json",     # "xml"
    # XML 报配置
    "ROOT_TAG": "root",
    # MQ 配置
    "CTRL_Q_MAX_DEPTH": 8,
    "MSG_Q_MAX_DEPTH": 32,
    "MSG_Q_BlockingTime": 1/3,
    # FTP 配置
    "FTP_BANNER": "Welcome to Information Service Exchange Platform.",
    "FTP_PASV_PORTS": {
        "START": 10000,
        "END": 10009,
    },
    "FTP_BUFFER_SIZE": 1024,
    "FTP_MAX_CONS": 10,
    "FTP_MAX_CONS_PER_IP": 5,
}
