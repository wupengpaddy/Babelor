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
# Outer Required
import pandas as pd
import xlrd
# Inner Required
from Babelor.Presentation import URL, MSG
# Global Parameters


class EXCEL:
    def __init__(self, conn: URL):
        if isinstance(conn, str):
            self.conn = URL(conn)
        else:
            self.conn = conn
        self.conn = self.__dict__["conn"].check

    def read(self, msg: MSG):
        msg_new = msg
        return msg_new

    def write(self, msg: MSG):
        pass


def sheets_merge(read_path, write_path):
    """
    :param read_path: 读取路径
    :param write_path: 写入路径
    :return: None
    """
    book = xlrd.open_workbook(read_path)
    writer = None
    for sheet in book.sheets():
        reader = pd.read_excel(read_path, sheet_name=sheet.name)
        if writer is None:
            writer = reader
        else:
            writer = writer.append(reader.fillna(""))       # NaN clean up
    writer = writer.reset_index(drop=True)                  # idx clean up
    writer.to_excel(write_path)


if __name__ == '__main__':
    sheets_merge("excel.xlsx", "excel_out.xlsx")
