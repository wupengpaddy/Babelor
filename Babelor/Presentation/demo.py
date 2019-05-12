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
import os
import base64
# Outer Required
import pandas as pd
import numpy as np
# Inner Required
from Babelor.Presentation import URL, MSG, CASE
# Global Parameters


def demo_tomail_url():
    url = URL().init("tomail+smtp")
    url = url.check
    print("URL:", url)
    print("收件人邮箱:", url.netloc)
    print("收件人用户名:", url.username)
    print("收件人名:", url.path)
    print("发件人邮箱：", url.fragment.fragment.netloc)
    print("发件人用户名:", url.fragment.username)
    print("发件人名:", url.fragment.fragment.path)
    print("服务协议:", url.fragment.scheme)
    print("服务用户:", url.fragment.username)
    print("服务密码:", url.fragment.password)
    print("服务地址", url.fragment.hostname)
    print("服务端口", url.fragment.port)


def demo_mysql_url():
    url = URL().init("mysql")
    url = url.check
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("数据服务", url.path)


def demo_oracle_url():
    url = URL().init("oracle")
    url = url.check
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("数据服务", url.path)


def demo_ftp_url():
    url = URL().init("ftp")
    url = url.check
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务用户:", url.username)
    print("服务密码:", url.password)
    print("服务地址", url.hostname)
    print("服务端口", url.port)
    print("服务路径", url.path)
    print("服务模式", url.fragment.query["model"])


def demo_tcp_url():
    url = URL().init("tcp")
    url = url.check
    print("\nURL:", url)
    print("服务协议:", url.scheme)
    print("服务地址", url.hostname)
    print("服务端口", url.port)


def demo_msg_mysql2ftp():
    msg = MSG()
    msg.origination = URL().init("mysql").check
    msg.destination = URL().init("ftp").check
    msg.treatment = URL().init("tcp")
    case = CASE()
    case.origination = msg.origination
    case.destination = msg.destination
    msg.activity = "init"
    msg.case = case
    msg.add_datum("This is a test string", path=URL().init("file"))
    msg.add_datum("中文UTF-8编码测试", path=URL().init("file"))
    path = "..\README\Babelor-设计.png"
    with open(path, "rb") as f:
        bytes_f = f.read()
        url = URL().init("file")
        url.path = path
        msg.add_datum(bytes_f, url)

    msg_string = str(msg)

    new_msg = MSG(msg_string)
    new_bytes_f = new_msg.read_datum(3)["stream"]
    new_path = "..\README\Babelor-设计-new.png"
    with open(new_path, "wb") as f:
        f.write(new_bytes_f)


def demo_excel():
    path = "C:/Users/geyua/PycharmProjects/Babelor/data/dir1/20190505.xlsx"
    df = pd.read_excel(path)
    msg = MSG()
    msg.add_datum(df, path)
    message = str(msg)
    new_msg = MSG(message)
    print(msg)
    print(new_msg)


def demo_numpy():
    arr = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    msg = MSG()
    msg.add_datum(arr)
    message = str(msg)
    new_msg = MSG(message)
    print(msg)
    print(new_msg)
    print(new_msg.read_datum(0)["stream"])


if __name__ == '__main__':
    # demo_tomail_url()
    # demo_ftp_url()
    # demo_mysql_url()
    # demo_oracle_url()
    # demo_tcp_url()
    # demo_msg_mysql2ftp()
    # demo_excel()
    demo_numpy()
