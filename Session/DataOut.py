# coding=utf-8
# Copyright 2018 StrTrek Team Authors.
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
import os
import pandas as pd
from Component.Email import Mail
from Component.MQ import MessageQueue


def email_send(select_date: str, df: pd.DataFrame):
    file_path = "data/{0}.xlsx".format(select_date)
    df.to_excel(file_path, index=False)
    mail_to = {
        'name':     '葛元霁',
        'user':     'geyuanji',
        'postfix':  'shairport.com'
    }
    mail_sub = '航班日报{0}'.format(select_date)
    mail_content = "航班日报{0}".format(select_date)
    mail = Mail(mail_to, mail_sub, mail_content, file_path)
    mail.send()
    print("APP45-PVG2SAA send {0} data by email successful.".format(select_date))
    os.remove(file_path)


def json_send(select_date: str, df: pd.DataFrame):
    msg = MessageQueue(select_date, df)
    msg.send()
    print("APP45-PVG2SAA send {0} data via json successful.".format(select_date))
