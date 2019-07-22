# coding=utf-8
# System Required
import codecs
import os


def load_rst_file(rst_file_path):
    """
    load long description from RST file，publish on PyPi
    从 RST 文件载入长描述，发布在 PyPi 的项目库信息页面上
    """
    return codecs.open(os.path.join(os.path.dirname(__file__), rst_file_path)).read()


# 库名
NAME = "Babelor"

# 版本号
# r"N.N[.N]+[{a|b|c|rc}N[.N]+][.postN][.devN]"
# N.N           主版本号和副版本号
# [.N]          次要版本号
# {a|b|c|rc}    阶段代号(可选), a:alpha, b:beta, c:candidate, rc:release candidate
# N[.N]         阶段主版本号和副版本号(可选)
# .postN        发行后的更新版本号(可选)
# .devN         开发期间的发行版本号(可选)
VERSION = "0.1.4"

# 库包含路径
# Inner Required
PACKAGES = [
    "Babelor",                  # root/                 库/根
    "Babelor/Application",      # root/Application      库/应用层
    "Babelor/Data",             # root/data path        库/数据层
    "Babelor/Presentation",     # root/Presentation     库/表示层
    "Babelor/Session",          # root/Session          库/会话层
    "Babelor/Config",           # root/Config           库/配置层
    "Babelor/Tools",            # root/Tools            库/工具层
]

# 库依赖外部包
# Outer Required
INSTALL_REQUIRED = [
    "requests>=2.18.4",         # python http connector                 HTTP 连接器
    "beautifulsoup4>=4.6.0",    # python html4 descriptor               HTML 解释器
    "SQLAlchemy>=1.2.15",       # python database connector             SQL 连接器
    "numpy>=1.15.4",            # python array computing                解释器：数组分析
    "pandas>=0.23.4",           # python data analysis and statical     解释器：关系和标签类数据分析和统计
    "python-dateutil>=2.7.5",   # python data util                      解释器：通用数据结构
    "pyzmq>=17.1.2",            # python binding for 0mq                消息中间件
    "pyftpdlib>=1.5.4",         # python async ftp server               FTPD 服务器
    "xlrd>=1.2.0",              # python excel file read connector      EXCEL 读连接器 (XLS)
    "xlwt>=1.3.0",              # python excel file write connector     EXCEL 写连接器 (XLS)
    "openpyxl>=2.5.3",          # python excel file connector           EXCEL 连接器 (XLSX)
]

# 库分类
CLASSIFIERS = [
    # 开发状态
    #  1 - Planning 计划              2 - Pre-Alpha 预览内测      3 - Alpha 内测        4 - Beta 公测
    #  5 - Production/Stable 正式     6 - Mature 到期             7 - Inactive 停止
    'Development Status :: 2 - Pre-Alpha',
    # 许可证
    'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
    'License :: OSI Approved :: Apache Software License',
    # 目标用户
    'Intended Audience :: Developers',
    'Intended Audience :: Financial and Insurance Industry',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'Intended Audience :: Manufacturing',
    'Intended Audience :: Healthcare Industry',
    # 主题
    'Topic :: Software Development :: Libraries :: Application Frameworks',
    'Topic :: Scientific/Engineering :: Information Analysis',
    'Topic :: Scientific/Engineering :: Artificial Intelligence',
    'Topic :: System :: Distributed Computing',
    # 目标 Python 版本
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    # 目标操作系统
    'Operating System :: MacOS :: MacOS X',
    'Operating System :: Microsoft :: Windows',
    'Operating System :: POSIX',
]

# 简述
DESCRIPTION = """
Babelor - a tiny integration information service bus library for Python
=======================================================================

**Babelor** is a Python package providing fast, flexible, and expressive
integration information service bus structures designed to make working
with serializable message transportation and service invocation both easy
and intuitive. It aims to be the fundamental high-level building block for
doing practical, **real world** data analysis in Python. Additionally, 
it has the broader goal of becoming **the most powerful and flexible 
open source integration information message transportation / service
invocation tool available in any language**.
"""
# 中文简述
DESCRIPTION_CHN = """
Babelor - 微型集成信息服务总线 Python 库
=======================================================================

**Babelor** 基于 Python 开发的集成信息服务总线，提供高速、灵活、多样化的
信息交互体验，设计用于可序列化信息包或数据包简单与直观的传输和服务调用。
它的目标是成为在 Python 中进行现实世界中信息交互服务的基本高层构建模块。
此外，它还有更广泛的目标，即成为任何语言中最强大和最灵活的开源集成信息传输
或服务调用工具。
"""

# 关键字
KEYWORDS = ["Service Bus", ]

# 作者
AUTHOR = "StrTrek Team Authors"

# 作者联系方式
AUTHOR_EMAIL = "geyuanji@strtrek.com"

# 维护者
MAINTAINER = "StrTrek Team Authors"

# 维护者联系方式
MAINTAINER_EMAIL = "geyuanji@strtrek.com"

# 项目主页
URL = "http://www.strtrek.com/"

# 详细描述
if os.path.exists("README.rst"):
    LONG_DESCRIPTION = load_rst_file("README.rst")
else:
    LONG_DESCRIPTION = open("README.txt", "r").read()

# 许可证
LICENSE = """
Copyright 2019 StrTrek Team Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

if __name__ == '__main__':
    # System Required
    try:
        from setuptools import setup
    except ImportError:
        from distutils.core import setup

    setup(
        name=NAME,                              # 项目名称
        version=VERSION,                        # 项目版本
        description=DESCRIPTION,                # 项目简述
        long_description=LONG_DESCRIPTION,      # 项目说明
        classifiers=CLASSIFIERS,                # 项目分类
        keywords=KEYWORDS,                      # 关键字
        author=AUTHOR,                          # 项目作者
        author_email=AUTHOR_EMAIL,              # 作者邮件
        maintainer=MAINTAINER,                  # 项目维护
        maintainer_email=MAINTAINER_EMAIL,      # 维护邮件
        url=URL,                                # 项目主页
        license=LICENSE,                        # 版权申明
        packages=PACKAGES,                      # 项目库
        include_package_data=True,              # 库包含数据
        zip_safe=True,                          # 压缩安全
        install_requires=INSTALL_REQUIRED,      # 安装外部依赖库
    )   # 封库    Packaged


# 外部执行封库命令

# 校验检查
# python setup.py check

# 封库到 .zip or .tar.gz in dist/
# python setup.py sdist

# 编译为 WHL in dist/
# python setup.py bdist_wheel

# 编译上传至 PyPi
# python setup.py register sdist upload
