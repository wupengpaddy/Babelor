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
import json
from xml.etree import ElementTree
# Outer Required
# Inner Required
# Global Parameters
from Babelor.Config import CONFIG


def dict2json(dt: dict) -> str:
    return json.dumps(dt, skipkeys=False, ensure_ascii=False)


def json2dict(js: str) -> dict:
    return json.loads(js)


def etree2dict(root: ElementTree.Element) -> dict:
    dt = {}
    lt = []
    ini_tag = None
    for child in root:
        if ini_tag is None:
            ini_tag = child.tag
        if ini_tag == child.tag:
            if len(child.getchildren()) > 0:
                child_dt = etree2dict(child)
                child_dt.update(child.attrib)
            else:
                child_dt = child.text
            lt.append(child_dt)
        else:
            dt[ini_tag] = lt
            ini_tag = child.tag
            if len(child.getchildren()) > 0:
                child_dt = etree2dict(child)
                child_dt.update(child.attrib)
            else:
                child_dt = child.text
            lt = [child_dt]
    if ini_tag is not None:
        dt[ini_tag] = lt
    return dt


def xml2json(xml: str) -> str:
    return dict2json(etree2dict(ElementTree.fromstring(xml.strip())))


def xml2dict(xml: str) -> dict:
    return etree2dict(ElementTree.fromstring(xml.strip()))


def dict2etree(dt: dict, tag=CONFIG.XML_ROOT_TAG, parent=None) -> ElementTree.Element:
    if tag in [CONFIG.XML_ROOT_TAG]:
        root = ElementTree.Element(tag)
    else:
        root = ElementTree.SubElement(parent, tag)
    for key in dt.keys():
        if isinstance(dt[key], list):
            for lt_child in dt[key]:
                if isinstance(lt_child, dict):
                    dict2etree(lt_child, key, root)
                else:
                    child = ElementTree.SubElement(root, key)
                    child.text = lt_child
        elif isinstance(dt[key], dict):
            dict2etree(dt[key], key, root)
        else:
            child = ElementTree.SubElement(root, key)
            child.text = dt[key]
    return root


def json2xml(js: str) -> str:
    return ElementTree.tostring(dict2etree(json2dict(js)), encoding=CONFIG.Coding).decode(CONFIG.Coding)


def dict2xml(dt: dict) -> str:
    return ElementTree.tostring(dict2etree(dt), encoding=CONFIG.Coding).decode(CONFIG.Coding)


def extract_multi_values_from_keys(dt, *args):
    depth = len(args)
    if depth < 1:
        return dt
    if isinstance(dt, list):
        lt = []
        for d in dt:
            rt = extract_value_from_key(d, *args)
            # print("RETURN:{0} DATA:{1} ARGS:{2}".format(rt, d, *args))
            if rt not in lt:
                if isinstance(rt, list):
                    lt.extend(rt)
                else:
                    lt.append(rt)
        return remove_duplicated_value(lt)
    else:
        return extract_value_from_key(dt, *args)


def remove_duplicated_value(lt: list):
    dt = []
    for l in lt:
        if (l not in dt) and (l is not None):
            dt.append(l)
    return dt


def extract_value_from_key(dt, *args):
    depth = len(args)
    if depth < 1:
        return dt
    else:
        if isinstance(dt, dict):
            if args[0] in dt.keys():
                rt = extract_value_from_key(dt[args[0]], *args[1:])
                return rt
        if isinstance(dt, list):
            if len(dt) == 0:
                return None
            if len(dt) == 1:
                if isinstance(dt[0], dict) or isinstance(dt[0], list):
                    return extract_value_from_key(dt[0], *args)
                else:
                    return dt[0]
            if len(dt) > 1:
                return extract_multi_values_from_keys(dt, *args)


def extract_from_key(*args, **kwargs):
    rt = extract_value_from_key(*args, **kwargs)
    if CONFIG.XML_IS_STR_VALUE:
        if isinstance(rt, list):
            if len(rt) == 0:
                return "None"
            if len(rt) == 1:
                return str(rt[0])
            return ",".join(rt)
        return str(rt)
    else:
        if isinstance(rt, list):
            if len(rt) == 0:
                return None
            if len(rt) == 1:
                return rt[0]
        return rt
