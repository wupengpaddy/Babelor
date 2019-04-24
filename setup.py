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


NAME = "Babelor"                    # package name 库名
PACKAGES = ["somefunctions", ]      # dependency packages 依赖库
DESCRIPTION = """
关于这个包的描述
"""
LONG_DESCRIPTION = load_rst_file("README.rst")
KEYWORDS = "Service Bus "
AUTHOR = "StrTrek Team Authors"
AUTHOR_EMAIL = "strtrek@strtrek.com"
URL = "http://www.strtrek.com/"
VERSION = "0.1.1"
LICENSE = "MIT"


setup(name=NAME,
      version=VERSION,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      classifiers=[
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python',
          'Intended Audience :: Developers',
          'Operating System :: OS Independent',
      ],
      keywords=KEYWORDS,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      url=URL,
      license=LICENSE,
      packages=PACKAGES,
      include_package_data=True,
      zip_safe=True,
      )
