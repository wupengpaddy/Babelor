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


class CONFIG:
    Date_FMT = '%Y-%m-%d'                        # 日期格式  '2019-01-01'
    Datetime_FMT = '%Y-%m-%d %H:%M:%S.%f'        # 时间日期格式    '2019-01-01 00:00:01.000001'
    IPv4_FMT = r"(?=(\b|\D))(((\d{1,2})|(1\d{1,2})|(2[0-4]\d)|(25[0-5]))\.){3}((\d{1,2})|(1\d{1,2})|(2[0-4]\d)|" \
               r"(25[0-5]))(?=(\b|\D)) "
    IPv6_FMT = r"^([\\da-fA-F]{1,4}:){7}([\\da-fA-F]{1,4})$"
    Port_FMT = r"^([0-9]|[1-9][0-9]|[1-9][0-9][0-9]|[1-9][0-9][0-9][0-9]|[1-5][0-9][0-9][0-9][0-9]|6[0-4][0-9][0-9]" \
               r"[0-9]|65[0-4][0-9][0-9]|655[0-2][0-9]|6553[0-5])$ "
    Coding = 'utf-8'                                   # 字符串编码
    Default_LANG = "SIMPLIFIED CHINESE_CHINA.UTF8"     # 默认语言
    MSG_TPE = "json"                                   # 消息包封装 ["json", "xml", "msgpack"]
    XML_ROOT_TAG = "root"                              # XML 根标识
    XML_IS_STR_VALUE = True
    MQ_MAX_DEPTH = 32                                  # MQ 最大深度
    MQ_BLOCK_TIME = 1/1024                             # MQ 消息间阻塞时间
    FTP_BANNER = "Welcome to Babelor Information Service Exchange Platform."
    FTP_PASV_PORTS = [10000, 10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008, 10009]
    FTP_BUFFER = 1024
    FTP_MAX_CONS = 10
    FTP_MAX_CONS_PER_IP = 5
    IS_DATA_WRITE_END = True
    IS_DATA_READ_START = True
    IS_SQL_DATA_STRING = True
