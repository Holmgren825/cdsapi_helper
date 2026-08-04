import hashlib
import os
from typing import Dict, List, Union

import pandas as pd


def request_to_df(request: dict, reply: dict, req_hash: str) -> pd.DataFrame:
    df = pd.DataFrame([request])
    df["request_hash"] = req_hash
    df["request_id"] = reply["request_id"]
    df["state"] = reply["state"]
    return df


# TODO: This does not work for ADS (Haris datasets.)
def build_filename(request: pd.DataFrame, filename_spec: list) -> str:
    filetype = ".nc" if request.data_format == "netcdf" else ".grib"
    filename_parts = []
    for var in filename_spec:
        parts = str_to_list(getattr(request, var))
        part = "_".join(parts)
        part = part.replace("/", "-")
        filename_parts.append(part)

    filename = "-".join(filename_parts) + filetype
    filename = os.path.join(os.path.curdir, filename)
    return filename


# https://github.com/schollii/sandals/blob/master/json_sem_hash.py
JsonType = Union[str, int, float, List["JsonType"], "JsonTree"]
JsonTree = Dict[str, JsonType]
StrTreeType = Union[str, List["StrTreeType"], "StrTree"]
StrTree = Dict[str, StrTreeType]


def sorted_dict_str(data: JsonType) -> StrTreeType:
    if type(data) == dict:
        return {k: sorted_dict_str(data[k]) for k in sorted(data.keys())}
    elif type(data) == list:
        return [sorted_dict_str(val) for val in data]
    else:
        return str(data)


def get_json_sem_hash(data: JsonTree, hasher=hashlib.sha256) -> str:
    return hasher(bytes(repr(sorted_dict_str(data)), "UTF-8")).hexdigest()


def str_to_list(string: str) -> list:
    return string.strip("[]").replace("'", "").replace(" ", "").split(",")
