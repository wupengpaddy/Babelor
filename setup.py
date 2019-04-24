# coding=utf-8
# System Required
import codecs
import os
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def load_rst_file(rst_file_path):
    """
    load long description from RST file，publish on PyPI
    载入长描述文件，发布在 PyPI 的项目库信息页面上
    """
    return codecs.open(os.path.join(os.path.dirname(__file__), rst_file_path)).read()


NAME = "Babelor"                        # package name          库名
PACKAGES = ["Babelor",                  # root/ path            库/根
            "Babelor/Config",           # root/config path      库/配置层
            "Babelor/DataBase",         # root/database path    库/数据库层
            "Babelor/Message",          # root/message path     库/消息层
            "Babelor/Presentation",     # root/presentation     库/表示层
            "Babelor/Process",          # root/process          库/处理层
            "Babelor/Session",          # root/session          库/会话层
            "Babelor/Tools",            # root/tools            库/工具层
            ]
INSTALL_REQUIRED = [
    "requests>=2.18.4",         # python http connector
    "beautifulsoup4>=4.6.0",    # python html4 descriptor
    "SQLAlchemy>=1.2.15",       # python database connector
    "numpy>=1.15.4",            # python array computing
    "pandas>=0.23.4",           # python data analysis and statical
    "python-dateutil>=2.7.5",   # python data util
    "pyzmq>=17.1.2",            # python binding for 0mq
    "pyftpdlib>=1.5.4",         # python async ftp server
]
DESCRIPTION = """
Babelor - a ting data analysis and manipulation library for Python
=====================================================================

**Babelor** is a Python package providing fast, flexible, and expressive data
structures designed to make working with "relational" or "labeled" data both
easy and intuitive. It aims to be the fundamental high-level building block for
doing practical, **real world** data analysis in Python. Additionally, it has
the broader goal of becoming **the most powerful and flexible open source data
analysis / manipulation tool available in any language**. It is already well on
its way toward this goal.
"""
if os.path.exists("README.rst"):
    LONG_DESCRIPTION = load_rst_file("README.rst")
else:
    LONG_DESCRIPTION = open("README.txt", "r").read()
KEYWORDS = "Service Bus "
AUTHOR = "StrTrek Team Authors"
AUTHOR_EMAIL = "geyuanji@strtrek.com"
MAINTAINER = "StrTrek Team Authors"
MAINTAINER_EMAIL = "geyuanji@strtrek.com"
URL = "http://www.strtrek.com/"
VERSION = "0.1.1"
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
setup(name=NAME, version=VERSION, description=DESCRIPTION, long_description=LONG_DESCRIPTION,
      classifiers=[
          'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Intended Audience :: StrTrek Team Authors',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Microsoft :: Windows',
          'Operating System :: POSIX',
      ], keywords=KEYWORDS, author=AUTHOR, author_email=AUTHOR_EMAIL, url=URL, license=LICENSE,
      packages=PACKAGES, include_package_data=True, zip_safe=True, install_requires=INSTALL_REQUIRED,)

# check distribute code 校验检查
# python setup.py check

# package all               封库
# .zip or .tar.gz in dist/  打包文件为zip或tar.gz
# python setup.py sdist

# build wheel file      编译本地安装包
# whl file in dist/     安装包位于 dist/ 路径下
# python setup.py bdist_wheel

# register and upload to PyPI   编译上传至 PyPI
# python setup.py register sdist upload
