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

import re
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse


TAG_RE = re.compile(r'<[^>]+>', re.S)


def sign_html2json(text: str):
    soup = BeautifulSoup(text, "html.parser")
    if text:
        tables = soup.findAll("table")
        dt = {
            "data-user": [],
            "data-time": [],
            "data-sign": [],
            "data-department": [],
            "department-info": [],
        }
        for table in tables:
            dt["data-user"].append(table.attrs["data-user"])
            dt["data-time"].append(table.attrs["data-time"])
            tr = table.findAll("tr")
            dt["data-sign"].append(tr[0].text)
            dt["data-department"].append(tr[2].span.attrs["data-department"])
            dt["department-info"].append(tr[2].attrs["depinfo"])
        js = json.dumps(dt, ensure_ascii=False)
    else:
        js = ""
    return js


def user_html2json(text: str):
    soup = BeautifulSoup(text, "html.parser")
    if soup.span.text:
        dt = {
            "data-user": soup.span.text.split(","),
        }
        js = json.dumps(dt, ensure_ascii=False)
    else:
        js = ""
    return js


def get_html2dataframe(path: str):

    url_rec_doc = "&".join([
        'http://newoa.shairport.com/shairport/dep162/swgl_162.nsf/myview?openform',
        'view=vwdocforfenlei',
        'count={0}',
        'target=blank',
        'start={1}'
    ])  # para 1
    url_login = "http://newoa.shairport.com/Names.nsf?Login"
    header = {
        "Referer": url_rec_doc.format(20, 1),
        "Cache-Control":    "max-age=0",
        "Content-Type":     "application/x-www-form-urlencoded",
        "User-Agent":       " ".join([
            'Mozilla/5.0',
            '(Windows NT 10.0; Win64; x64)',
            'AppleWebKit/537.36',
            '(KHTML, like Gecko)',
            'Chrome/64.0.3282.140',
            'Safari/537.36',
            'Edge/17.17134'
        ]),
        "Accept-Language":  "zh-Hans",
        "Accept":           "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Upgrade-Insecure-Requests": "1",
        "Accept-Encoding":  "gzip, deflate",
        "Host":             urlparse(url_rec_doc.format(20, 1)).netloc,
        "Content-Length":   "446",
        "Connection":       "Keep-Alive"
    }
    data = {
        "%%ModDate": "0000000000000000",
        "reasonType": "0",
        "idx_isuseingV6idx": "1",
        "rftfootercontent": "68342507",
        "%%Surrogate_locale": "1",
        "locale": "zh-cn",
        "Username": "gfceshi",
        "Password": "password",
        "RedirectTo": url_rec_doc.format(20, 1)
    }

    session = requests.Session()
    session.header = header
    reply = session.post(url_login, data=data, allow_redirects=True)
    total_rows = BeautifulSoup(reply.text, "html.parser").body.table.select('input[name="total"]')[0].attrs["value"]
    reply = session.get(url_rec_doc.format(total_rows, 1))
    soup = BeautifulSoup(reply.text, "html.parser")
    rows = soup.textarea.table.findAll("tr")
    title = []
    for cell in rows[0]:
        title.append(TAG_RE.sub("", cell.get_text()))
    data = {
        "起草日期": [],
        "公文类型": [],
        "标题": [],
        "收文编号": [],
        "审核意见": [],
        "主办部门": [],
        "主办意见": [],
        "会办部门": [],
        "会办意见": [],
        "处理状态": [],
        "当前处理人": [],
        "文件链接": [],
    }
    for row in rows[1:]:
        cells = row.findAll("td")
        data["起草日期"].append(TAG_RE.sub("", cells[2].get_text()))
        data["公文类型"].append(TAG_RE.sub("", cells[3].get_text()))
        data["标题"].append(TAG_RE.sub("", cells[4].get_text()))
        data["收文编号"].append(TAG_RE.sub("", cells[5].get_text()))
        data["审核意见"].append(sign_html2json(cells[6].text))
        data["主办部门"].append(TAG_RE.sub("", cells[7].get_text()))
        data["主办意见"].append(sign_html2json(cells[8].text))
        data["会办部门"].append(TAG_RE.sub("", cells[9].get_text()))
        data["会办意见"].append(sign_html2json(cells[10].text))
        data["处理状态"].append(cells[11].font.a.text.split("<")[0])
        data["当前处理人"].append(user_html2json(cells[12].text))
        data["文件链接"].append("http://newoa.shairport.com" + cells[11].font.a.attrs["href"])
    df = pd.DataFrame(data, dtype=str)
    df.to_excel(path)
    return df


if __name__ == '__main__':
    get_html2dataframe("../data/oa_data.xlsx")
