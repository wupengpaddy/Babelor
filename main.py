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

# System Required
import time
import datetime
from queue import Queue
from threading import Thread
# Outer Required
import pandas as pd
from Babelor import MSG, URL, CASE, TEMPLE, MessageQueue
# Inner Required
# Global Parameters
DateFormat = '%Y-%m-%d'
CurrentTime = time.localtime(time.time() + (60 * 60 * 8))  # GMT+8
CurrentDateTime = datetime.datetime.now()
CurrentDate = CurrentDateTime.strftime(DateFormat)
# Outer Required Parameters
pd.options.mode.chained_assignment = None       # 关闭写复制警告
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 2000)


# 分割复合数据，并提取指定列
def data_split(dt: str, seq: int):
    r = dt.split(",")
    if len(r) > 1:
        return r[seq]
    else:
        return r[0]


# AFDS 与 CDM　数据混合
def data_mix(afds_dt: str, cdm_dt: str):
    if cdm_dt:
        return cdm_dt
    elif afds_dt:
        return afds_dt
    else:
        return "<EMPTY>"


# AFDS 生成 ICAO_FLNO
def create_icao_flno(dt: pd.Series):
    if dt["ICAO_CARRIER_ICAO_CODE"] == "ZZZ":
        return dt["IATA_FLNO"]
    else:
        return dt["ICAO_CARRIER_ICAO_CODE"] + dt["ICAO_FLIGHT_NUMBER"]


def func_treatment(msg: MSG):
    # -———————————————————————————————————------------------------ INIT ---------
    # 数据参数提取
    data_tuple = {
        "PVG-AFDS": None,
        "PVG-CDM": None,
        "SELECT-DATE": None,
    }
    for i in range(0, msg.nums, 1):
        attachment = msg.read_datum(i)
        if attachment["path"] in data_tuple.keys():
            data_tuple[attachment["path"]] = attachment["stream"]
    # 读取参数
    afds = data_tuple["PVG-AFDS"]
    cdm = data_tuple["PVG-CDM"]
    select_date = data_tuple["SELECT-DATE"]
    # -———————————————————————————————————------------------------ INIT ---------
    msg_out = msg
    msg_out.nums = 0
    # ————————————————————————————————————————--------------- START --------
    # 列名强制输出大写
    afds = afds.rename(str.upper, axis='columns')
    cdm = cdm.rename(str.upper, axis='columns')
    # 清理输出文本空值 为 ""
    afds.replace('nan', '<EMPTY>', inplace=True)
    cdm.replace('nan', '<EMPTY>', inplace=True)
    # 重建索引
    afds = afds.reset_index(drop=True)
    cdm = cdm.reset_index(drop=True)
    # AFDS 数据生成
    afds["ICAO_FLNO"] = afds.apply(lambda x: create_icao_flno(x), axis=1)
    afds["OPERATION"] = afds.apply(lambda x: data_split(x["OPERATION"], 1), axis=1)     # 取对外发布状态
    # ————————————————————————————————————————--------------- START --------
    del data_tuple
    # ————————————————————————————————————————--------------- PROCESS :: 1 -
    # AFDS 进港航班和出港航班分表
    afds_arrival = afds[afds["DIRECTION"] == "Arrival"]
    afds_departure = afds[afds["DIRECTION"] == "Departure"]
    # CDM 进港航班和出港航班分表
    cdm_arrival = cdm[cdm["ICAO_DES"] == "ZSPD"]
    cdm_departure = cdm[cdm["ICAO_ORG"] == "ZSPD"]
    # CDM 建立 航班计划日期
    cdm_arrival["SCHEDULE_DATE"] = cdm_arrival.apply(lambda x: x["STOA"][0:10], axis=1)
    cdm_departure["SCHEDULE_DATE"] = cdm_departure.apply(lambda x: x["STOD"][0:10], axis=1)
    # AFDS 前站/后站 数据生成
    afds_arrival["IATA_ORG"] = afds_arrival.apply(lambda x: data_split(x["PORT_OF_CALL_IATA_CODE"], 1), axis=1)
    afds_arrival["ICAO_ORG"] = afds_arrival.apply(lambda x: data_split(x["PORT_OF_CALL_ICAO_CODE"], 1), axis=1)
    afds_departure["IATA_DES"] = afds_departure.apply(lambda x: data_split(x["PORT_OF_CALL_IATA_CODE"], 0), axis=1)
    afds_departure["ICAO_DES"] = afds_departure.apply(lambda x: data_split(x["PORT_OF_CALL_ICAO_CODE"], 0), axis=1)
    # AFDS 进港数据 字段重命名
    afds_arrival.rename(columns={
        "IATA_FLNO": "ARR_IATA_FLNO",               # 进港航班号      （IATA 二字码）
        "ICAO_FLNO": "ARR_ICAO_FLNO",               # 进港航班号      （ICAO 三字码）
        "REPEAT": "ARR_REPEAT",                     # 进港航班重复号
        "SCHEDULE_DATE": "ARR_SCHEDULE_DATE",       # 进港计划日期
        "REG": "ARR_REG",                           # 进港注册号
        "STAND": "ARR_STAND",                       # 进港停机位
        "SCHEDULE_DATE_TIME": "STOA",               # 计划进港时刻
        "TYS": "ARR_TYS",                           # 进港机型
        "CLA": "ARR_CLA",                           # 进港航班类型
        "SECTOR": "ARR_SECTOR",                     # 进港国际国内属性
        "OPERATION": "ARR_OPERATION",               # 进港运营状态   （对外发布）
        "L_IATA_FLNO": "DEP_IATA_FLNO",             # 出港航班号     （IATA 二字码）  【连班】
        "L_REPEAT": "DEP_REPEAT",                   # 出港航班重复号                  【连班】
        "L_SCHEDULE_DATE": "DEP_SCHEDULE_DATE",     # 出港计划日期                    【连班】
    }, inplace=True)
    # AFDS 出港数据 字段重命名
    afds_departure.rename(columns={
        "IATA_FLNO": "DEP_IATA_FLNO",               # 出港航班号      （IATA 二字码）
        "ICAO_FLNO": "DEP_ICAO_FLNO",               # 出港航班号      （ICAO 三字码）
        "REPEAT": "DEP_REPEAT",                     # 出港航班重复号
        "SCHEDULE_DATE": "DEP_SCHEDULE_DATE",       # 出港计划日期
        "REG": "DEP_REG",                           # 出港注册号
        "STAND": "DEP_STAND",                       # 出港停机位
        "SCHEDULE_DATE_TIME": "STOD",               # 计划出港时刻
        "TYS": "DEP_TYS",                           # 出港机型
        "CLA": "DEP_CLA",                           # 出港航班类型
        "SECTOR": "DEP_SECTOR",                     # 出港国际国内属性
        "OPERATION": "DEP_OPERATION",               # 进港运营状态   （对外发布）
        "L_IATA_FLNO": "ARR_IATA_FLNO",             # 进港航班号     （IATA 二字码）  【连班】
        "L_REPEAT": "ARR_REPEAT",                   # 进港航班重复号                  【连班】
        "L_SCHEDULE_DATE": "ARR_SCHEDULE_DATE",     # 进港计划日期                    【连班】
    }, inplace=True)
    # CDM 进港数据 字段重命名
    cdm_arrival.rename(columns={
        "ICAO_FLNO": "ARR_ICAO_FLNO",           # 进港航班号      （ICAO 三字码）
        "SCHEDULE_DATE": "ARR_SCHEDULE_DATE",   # 进港计划日期
        "REG": "ARR_REG",                       # 进港注册号
        "RUNWAY": "ARR_RUNWAY",                 # 着陆跑道
        "CTRL": "ARR_CTRL",                     # 进港是否流控管制
        "CTRL_CONTENT": "ARR_CTRL_CONTENT",     # 进港流控管制内容
        "CTRL_POINT": "ARR_CTRL_POINT",         # 进港流控管制点
        "CTRL_REASON": "ARR_CTRL_REASON",       # 进港流控原因
    }, inplace=True)
    # CDM 出港数据 字段重命名
    cdm_departure.rename(columns={
        "ICAO_FLNO": "DEP_ICAO_FLNO",           # 出港航班号      （ICAO 三字码）
        "SCHEDULE_DATE": "DEP_SCHEDULE_DATE",   # 出港计划日期
        "REG": "DEP_REG",                       # 出港注册号
        "RUNWAY": "DEP_RUNWAY",                 # 起飞跑道
        "CTRL": "DEP_CTRL",                     # 出港是否流控管制
        "CTRL_CONTENT": "DEP_CTRL_CONTENT",     # 出港流控管制内容
        "CTRL_POINT": "DEP_CTRL_POINT",         # 出港流控管制点
        "CTRL_REASON": "DEP_CTRL_REASON",       # 出港流控原因
    }, inplace=True)
    # AFDS 进港数据字段整理
    afds_arrival = pd.DataFrame(afds_arrival[[
        "ARR_IATA_FLNO",        # 进港航班号      （IATA 二字码）
        "ARR_ICAO_FLNO",        # 进港航班号      （ICAO 三字码）
        "ARR_REPEAT",           # 进港航班重复号
        "ARR_SCHEDULE_DATE",    # 进港计划日期
        "ARR_REG",              # 进港注册号
        "ARR_STAND",            # 进港停机位
        "IATA_ORG",             # 前站            （IATA 三字码）
        "ICAO_ORG",             # 前站            （ICAO 四字码）
        "ARR_TYS",              # 进港机型
        "ARR_CLA",              # 进港航班类型
        "ARR_SECTOR",           # 进港国际国内属性
        "ARR_OPERATION",        # 进港运营状态   （对外发布）
        "AFDS_AIBT",            # 实际上轮档时刻                    【AFDS】
        "PSTD",                 # 前站计划起飞时刻
        "PATD",                 # 前站实际起飞时刻
        "STOA",                 # 计划进港时刻
        "TDT",                  # 实际落地时刻
        # "DEP_IATA_FLNO",       # 出港航班号     （IATA 二字码）  【连班】
        # "DEP_REPEAT",          # 出港航班重复号                  【连班】
        # "DEP_SCHEDULE_DATE",   # 出港计划日期                    【连班】
    ]]).reset_index(drop=True)
    # AFDS 出港数据字段整理
    afds_departure = pd.DataFrame(afds_departure[[
        "DEP_IATA_FLNO",        # 出港航班号      （IATA 二字码）
        "DEP_ICAO_FLNO",        # 出港航班号      （ICAO 三字码）
        "DEP_REPEAT",           # 出港航班重复号
        "DEP_SCHEDULE_DATE",    # 出港计划日期
        "DEP_REG",              # 出港注册号
        "DEP_STAND",            # 出港停机位
        "IATA_DES",             # 后站            （IATA 三字码）
        "ICAO_DES",             # 后站            （ICAO 四字码）
        "DEP_TYS",              # 出港机型
        "DEP_CLA",              # 出港航班类型
        "DEP_SECTOR",           # 出港国际国内属性
        "DEP_OPERATION",        # 出港运营状态   （对外发布）
        "AFDS_AOBT",            # 实际撤轮档时刻                  【AFDS】
        "STOD",                 # 计划出港时刻
        "ABT",                  # 实际起飞时刻
        "ARR_IATA_FLNO",        # 进港航班号     （IATA 二字码）  【连班】
        "ARR_REPEAT",           # 进港航班重复号                  【连班】
        "ARR_SCHEDULE_DATE",    # 进港计划日期                    【连班】
    ]]).reset_index(drop=True)
    # CDM 进港数据字段整理
    cdm_arrival = pd.DataFrame(cdm_arrival[[
        "ARR_ICAO_FLNO",        # 进港航班号      （ICAO 三字码）
        "ARR_SCHEDULE_DATE",    # 进港计划日期
        # "ARR_REG",             # 进港注册号
        # "ICAO_ORG",            # 前站            （ICAO 四字码）
        # "STOA",               # 计划进港时刻
        "ARR_RUNWAY",           # 着陆跑道
        "CDM_AIBT",             # 实际上轮档时刻                   【CDM】
        "ARR_CTRL",             # 进港是否流控管制
        "ARR_CTRL_CONTENT",     # 进港流控管制内容
        "ARR_CTRL_POINT",       # 进港流控管制点
        "ARR_CTRL_REASON",      # 进港流控原因
    ]]).reset_index(drop=True)
    # CDM 出港数据字段整理
    cdm_departure = pd.DataFrame(cdm_departure[[
        "DEP_ICAO_FLNO",  # 出港航班号     （IATA 二字码）
        "DEP_SCHEDULE_DATE",  # 出港计划日期
        # "DEP_REG",                # 出港注册号
        # "ICAO_DES",               # 后站            （ICAO 四字码）
        # "STOD",                   # 计划出港时刻
        "DEP_RUNWAY",  # 起飞跑道
        "CTOT",  # 计算起飞时刻
        "COBT",  # 计算撤轮档时刻
        "CDM_AOBT",  # 实际撤轮档时刻                   【CDM】
        "DEP_CTRL",  # 出港是否流控管制
        "DEP_CTRL_CONTENT",  # 出港流控管制内容
        "DEP_CTRL_POINT",  # 出港流控管制点
        "DEP_CTRL_REASON",  # 出港流控原因
    ]]).reset_index(drop=True)
    # ————————————————————————————————————————--------------- PROCESS :: 1 -
    del afds, cdm
    # ————————————————————————————————————————--------------- PROCESS :: 2 -
    # 选取出港航班
    afds_departure_select = afds_departure[afds_departure["DEP_SCHEDULE_DATE"] == select_date]
    # 出港航班合并
    pvg_departure = pd.merge(afds_departure_select, cdm_departure, how="left",
                             on=["DEP_ICAO_FLNO", "DEP_SCHEDULE_DATE"])
    pvg_departure = pvg_departure.where(pvg_departure.notnull(), "<NaN>")                   # 无匹配空值 为 "<NaN>"
    # 出港航班 AOBT 数据生成
    pvg_departure["AOBT"] = pvg_departure.apply(lambda x: data_mix(x["AFDS_AOBT"], x["CDM_AOBT"]), axis=1)
    # 进港航班合并
    pvg_arrival = pd.merge(afds_arrival, cdm_arrival, how="left", on=["ARR_ICAO_FLNO", "ARR_SCHEDULE_DATE"])
    pvg_arrival = pvg_arrival.where(pvg_arrival.notnull(), "<NaN>")                         # 无匹配空值 为 "<NaN>"
    # 进港航班 AIBT 数据生成
    pvg_arrival["AIBT"] = pvg_arrival.apply(lambda x: data_mix(x["AFDS_AIBT"], x["CDM_AIBT"]), axis=1)
    # 连班合并
    pvg_up = pd.merge(pvg_departure, pvg_arrival, how="left", on=["ARR_IATA_FLNO", "ARR_REPEAT", "ARR_SCHEDULE_DATE"])
    pvg_arrival_select = pvg_arrival[pvg_arrival["ARR_SCHEDULE_DATE"] == select_date]
    pvg = pd.concat([pvg_up, pvg_arrival_select], sort=False)
    # ————————————————————————————————————————--------------- PROCESS :: 2 -
    del pvg_arrival, pvg_departure, cdm_arrival, cdm_departure, pvg_arrival_select, pvg_up
    # ————————————————————————————————————————--------------- PROCESS :: 3 -
    pvg.drop_duplicates(subset=["ARR_IATA_FLNO", "ARR_SCHEDULE_DATE", "ARR_REPEAT"], keep='first', inplace=True)
    pvg.drop_duplicates(subset=["DEP_IATA_FLNO", "DEP_SCHEDULE_DATE", "DEP_REPEAT"], keep='first', inplace=True)
    # 无匹配空值 为 "<NaN>"
    pvg = pvg.where(pvg.notnull(), "<NaN>")
    # 置空值设置为 ""
    pvg.replace('<EMPTY>', '', inplace=True)
    # 数据整理
    pvg = pd.DataFrame(pvg[[
        "ARR_IATA_FLNO",  # 进港航班号      （IATA 二字码）
        "IATA_ORG",  # 前站            （IATA 三字码）
        "PSTD",  # 前站计划起飞时刻
        "PATD",  # 前站实际起飞时刻
        "STOA",  # 计划进港时刻
        "TDT",  # 实际落地时刻
        "ARR_STAND",  # 进港停机位
        "ARR_REG",  # 进港注册号
        "ARR_TYS",  # 进港机型
        "ARR_CLA",  # 进港航班类型
        "ARR_SECTOR",  # 进港国际国内属性
        "ARR_OPERATION",  # 进港运营状态   （对外发布）
        "AIBT",  # 实际上轮档时刻
        "ARR_CTRL",  # 进港是否流控管制
        "ARR_CTRL_CONTENT",  # 进港流控管制内容
        "ARR_CTRL_POINT",  # 进港流控管制点
        "ARR_CTRL_REASON",  # 进港流控原因
        "ARR_RUNWAY",  # 着陆跑道
        "DEP_IATA_FLNO",  # 出港航班号     （IATA 二字码）
        "IATA_DES",  # 后站           （IATA 三字码）
        "STOD",  # 计划出港时刻
        "ABT",  # 实际起飞时刻
        "DEP_STAND",  # 出港停机位
        "DEP_REG",  # 出港注册号
        "DEP_TYS",  # 出港机型
        "DEP_CLA",  # 出港航班类型
        "DEP_SECTOR",  # 出港国际国内属性
        "DEP_OPERATION",  # 出港运营状态   （对外发布）
        "CTOT",  # 计算起飞时刻
        "COBT",  # 计算撤轮档时刻
        "AOBT",  # 实际撤轮档时刻
        "DEP_CTRL",  # 出港是否流控管制
        "DEP_CTRL_CONTENT",  # 出港流控管制内容
        "DEP_CTRL_POINT",  # 出港流控管制点
        "DEP_CTRL_REASON",  # 出港流控原因
        "DEP_RUNWAY"  # 起飞跑道
    ]]).reset_index(drop=True)
    # 数据输出
    pvg.rename(columns={
        "ARR_IATA_FLNO": "进港航班号FLT",
        "IATA_ORG": "前站ORG",
        "PSTD": "前站计划起飞",
        "PATD": "前站实际起飞",
        "STOA": "计划进港STA",
        "TDT": "实际落地时间TDT",
        "ARR_STAND": "进港停机位",
        "ARR_REG": "进港注册号REG",
        "ARR_TYS": "进港机型TYS",
        "ARR_CLA": "进港航班类型CLA",
        "ARR_SECTOR": "进港国际国内属性",
        "ARR_OPERATION": "进港运营状态",
        "AIBT": "进港实际上轮档AIBT",
        "ARR_CTRL": "进港是否流控管制",
        "ARR_CTRL_CONTENT": "进港流控管制内容",
        "ARR_CTRL_POINT": "进港流控管制点",
        "ARR_CTRL_REASON": "进港流控原因",
        "ARR_RUNWAY": "着陆跑道",
        "DEP_IATA_FLNO": "出港航班号FLT",
        "IATA_DES": "后站DES",
        "STOD": "计划出港STD",
        "ABT": "起飞时间ABT",
        "DEP_STAND": "出港停机位",
        "DEP_REG": "出港注册号REG",
        "DEP_TYS": "出港机型TYS",
        "DEP_CLA": "出港航班类型CLA",
        "DEP_SECTOR": "出港国际国内属性",
        "DEP_OPERATION": "出港运营状态",
        "CTOT": "计算起飞CTOT",
        "COBT": "计算撤轮档COBT",
        "AOBT": "实际撤轮档AOBT",
        "DEP_CTRL": "出港是否流控管制",
        "DEP_CTRL_CONTENT": "出港流控管制内容",
        "DEP_CTRL_POINT": "出港流控管制点",
        "DEP_CTRL_REASON": "出港流控原因",
        "DEP_RUNWAY": "起飞跑道"
    }, inplace=True)
    msg_out.add_datum(pvg.to_msgpack(), "{0}.xlsx".format(select_date.replace("-", "")))
    # ————————————————————————————————————————--------------- PROCESS :: 3 -
    del pvg
    return msg_out


def priest(select_date: str):
    # 数据范围
    start_date = datetime.datetime.strptime(select_date, DateFormat) - datetime.timedelta(days=7)
    end_date = datetime.datetime.strptime(select_date, DateFormat) + datetime.timedelta(days=1)
    # PVG-AFDS SQL Shell
    pvg_afds_sql = """
select afds.flight_direction                as DIRECTION,
       afds.flight_identity                 as IATA_FLNO,
       afds.flight_repeat_count             as REPEAT,
       afds.icao_carrier_icao_code          as ICAO_CARRIER_ICAO_CODE,
       afds.icao_flight_number              as ICAO_FLIGHT_NUMBER,
       afds.port_of_call_iata_code          as PORT_OF_CALL_IATA_CODE,
       afds.port_of_call_icao_code          as PORT_OF_CALL_ICAO_CODE,
       afds.previous_station_scheduled_dt   as PSTD,
       afds.previous_station_airborne_dt    as PATD,
       afds.scheduled_date                  as SCHEDULE_DATE,
       afds.scheduled_date_time             as SCHEDULE_DATE_TIME,
       afds.wheels_down_date_time           as TDT,
       afds.wheels_up_date_time             as ABT,
       afds.ACTUAL_ON_BLOCKS_DATE_TIME      as AFDS_AIBT,
       afds.ACTUAL_OFF_BLOCKS_DATE_TIME     as AFDS_AOBT,
       afds.stand_position                  as STAND,
       afds.aircraft_registration           as REG,
       afds.aircraft_subtype_iata_code      as TYS,
       afds.flight_classification_code      as CLA,
       afds.flight_sector_code              as SECTOR,
       afds.operation_type_code             as OPERATION,
       afds.linked_flight_identity          as L_IATA_FLNO,
       afds.linked_flight_repeat_count      as L_REPEAT,
       afds.linked_scheduled_date           as L_SCHEDULE_DATE
from flatter_afds afds
where afds.code_share_status <> 'SF'
  and afds.scheduled_date between '{0}' and '{1}'
order by scheduled_date
""".format(start_date.strftime(DateFormat), end_date.strftime(DateFormat))
    # PVG-CDM SQL Shell
    pvg_cdm_sql = """
select cdm.flno             as ICAO_FLNO,
       cdm.regne            as REG,
       cdm.adep             as ICAO_ORG,
       cdm.ades             as ICAO_DES,
       cdm.runway           as RUNWAY,
       cdm.stod             as STOD,
       cdm.stoa             as STOA,
       cdm.aldt             as ALDT,
       cdm.aibt             as CDM_AIBT,       
       cdm.ctot             as CTOT,
       cdm.cobt             as COBT,
       cdm.dobt             as DOBT,
       cdm.atot             as ATOT,
       cdm.aobt             as CDM_AOBT,
       cdm.IS_STRICT_CTRL   as CTRL,
       cdm.ctrl_content     as CTRL_CONTENT,
       cdm.ctrl_point       as CTRL_POINT,
       cdm.ctrl_reason      as CTRL_REASON
from cdm_flight cdm
where cdm.STOD between to_date('{0}', 'yyyy-mm-dd') and to_date('{1}', 'yyyy-mm-dd')
   or cdm.STOA between to_date('{0}', 'yyyy-mm-dd') and to_date('{1}', 'yyyy-mm-dd')
order by STOD, STOA
""".format(start_date.strftime(DateFormat), end_date.strftime(DateFormat))
    # 消息生成
    msg = MSG()
    msg.add_datum(datum=select_date, path="SELECT-DATE")
    msg.add_datum(datum=pvg_afds_sql, path="PVG-AFDS")
    msg.add_datum(datum=pvg_cdm_sql, path="PVG-CDM")
    msg.origination = URL("oracle://wonders:password@10.169.0.1:1521/oracle")
    msg.destination = URL("tcp://10.169.0.43:9000")
    msg.treatment = URL("tcp://10.169.0.44:9000")
    msg.case = CASE()
    msg.case.origination = URL("oracle://username:password@10.169.0.1:1521/oracle")
    msg.case.destination = URL("ftp://username:password@172.21.98.66:21#PASV")
    mq = MessageQueue(URL("tcp://10.169.0.7:2011"))
    mq.push(msg)
    mq.close()


def range_worker(year: int):
    # 取时间范围
    begin_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year, 12, 31)
    if end_date > datetime.datetime.strptime(CurrentDate, DateFormat):
        end_date = CurrentDate
    # 时间遍历
    for i in range((end_date - begin_date).days + 1):
        select_date = begin_date + datetime.timedelta(days=i)
        priest(select_date.strftime(DateFormat))


def sender():
    myself = TEMPLE(URL("tcp://*:2511"))
    myself.open(role="sender")


def treater():
    myself = TEMPLE(URL("tcp://*:2511"))
    myself.open(role="treater", func=func_treatment)


def receiver():
    myself = TEMPLE(URL("tcp://*:2511"))
    myself.open(role="receiver")


def try_push():
    msg = MSG()
    msg.origination = URL("tcp://127.0.0.1:10001")
    mq = MessageQueue(URL("tcp://127.0.0.1:10001"))
    print("push msg:", msg)
    mq.push(msg)


def try_pull():
    mq = MessageQueue(URL("tcp://*:10001"))
    msg = mq.pull()
    print("pull msg:", msg)


def test_push_pull():
    thread = Thread(target=try_pull)
    thread.start()
    try_push()


def try_request():
    msg = MSG()
    msg.origination = URL("tcp://127.0.0.1:10001")
    mq = MessageQueue(URL("tcp://127.0.0.1:10001"))
    print("request msg:", msg)
    msg = mq.request(msg)
    print("reply msg:", msg)


def try_reply_func(msg: MSG):
    msg.destination = URL().init("oracle")
    return msg


def try_reply():
    mq = MessageQueue(URL("tcp://*:10001"))
    mq .reply(try_reply_func)


def test_request_reply():
    thread = Thread(target=try_reply)
    thread.start()
    try_request()


def try_publish():
    msg = MSG()
    msg.origination = URL("tcp://127.0.0.1:10001")
    mq = MessageQueue(URL("tcp://*:10001"))
    print("publish msg:", msg)
    mq.publish(msg)


def try_subscribe():
    mq = MessageQueue(URL("tcp://127.0.0.1:10001"))
    msg = mq.subscribe()
    print("subscribe msg:", msg)


def test_publish_subscribe():
    thread = Thread(target=try_publish)
    thread.start()
    time.sleep(2)
    try_subscribe()


if __name__ == '__main__':
    # test_push_pull()
    # test_request_reply()
    test_publish_subscribe()
    # treater()
    # receiver()
    # range_worker(year=2018)
