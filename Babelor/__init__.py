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

# Inner Required
from Babelor.Presentation import MSG, URL, CASE
from Babelor.Application import TEMPLE
from Babelor.Session import MQ
from Babelor.Tools import TASKS

# Let users know if they're missing any of hard dependencies
hard_dependencies = (
    "pandas",           # python data analysis and statical     解释器：关系和标签类数据分析和统计
    "numpy",            # python array computing                解释器：数组分析
    "sqlalchemy",       # python database connector             SQL 连接器
    "zmq",              # python binding for 0mq                消息中间件
    "xlrd",             # python excel file read connector      EXCEL 读连接器 (XLS)
    "xlwt",             # python excel file write connector     EXCEL 写连接器 (XLS)
    "pyftpdlib",        # python async ftp server               FTPD 服务器
    "openpyxl",         # python excel file connector           EXCEL 连接器 (XLSX)
    "BeautifulSoup",    # python html4 descriptor               HTML 解释器
    "requests",         # python http connector                 HTTP 连接器
)
missing_dependencies = []
for dependency in hard_dependencies:
    try:
        __import__(dependency)
    except ImportError as e:
        missing_dependencies.append(dependency)

if missing_dependencies:
    raise ImportError(
        "Missing required dependencies {0}".format(missing_dependencies))
del hard_dependencies, missing_dependencies
