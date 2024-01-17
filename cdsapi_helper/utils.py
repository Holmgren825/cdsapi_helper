import hashlib
import os
from typing import Dict, List, Union

import pandas as pd


def build_request(
    variable: str,
    year: str,
    month: str,
    day: str = None,
    pressure_levels: list[str] = None,
    time_steps: list[str] = None,
    area: list[str] = None,
) -> dict:
    year = str(year).zfill(2)

    if month == 0:
        month = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    else:
        month = str(month).zfill(2)

    if day is None:
        day = [
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
            "07",
            "08",
            "09",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24",
            "25",
            "26",
            "27",
            "28",
            "29",
            "30",
            "31",
        ]
    else:
        day = str(day).zfill(2)
    # Defaults.
    if pressure_levels is None:
        pressure_levels = ["300", "400", "500", "600", "700", "800", "900", "1000"]
    if time_steps is None:
        time_steps = ["00:00", "06:00", "12:00", "18:00"]
    if area is None:
        area = [90, -180, 40, 180]

    request = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "variable": variable,
        "pressure_level": pressure_levels,
        "year": year,
        "month": month,
        "day": day,
        "time": time_steps,
        "area": area,
    }
    return request


def request_to_df(request: dict, reply: dict, req_hash: str) -> pd.DataFrame:
    df = pd.DataFrame([request])
    df["request_hash"] = req_hash
    df["request_id"] = reply["request_id"]
    df["state"] = reply["state"]
    return df


def build_filename(request: dict) -> str:
    filetype = ".nc" if request.format == "netcdf" else ".grib"
    variable = request.variable
    year = request.year
    # FIX: This could be better, assumes that we have one month, or one full year.
    month = str_to_list(request.month)
    month = month if len(month) == 1 else None
    day = str_to_list(request.day)
    day = day if len(day) == 1 else None
    psls = str_to_list(request.pressure_level)
    pressure_level = f"psl_{psls[0]}_{psls[-1]}"
    # FIX: What to do with time?
    # time =
    area = str_to_list(request.area)
    area = "_".join(area)
    area = "ext_" + area

    filename = f"{variable}-{year}-{month}-{day}-{pressure_level}-{area}{filetype}"
    filename = os.path.join(os.path.curdir, filename)
    print(filename)
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
