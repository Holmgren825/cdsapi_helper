from datetime import datetime

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


def request_to_df(request: dict, reply: dict) -> pd.Dataframe:
    return pd.DataFrame(
        {
            "r_id": reply["request_id"],
            "state": reply["state"],
            "variable": request["variable"],
            "year": request["year"],
            "month": request["month"],
            "area": "_".join(str(i) for i in request["area"]),
            "submited": datetime.now().strftime("%Y-%m-%d-%H:%M:%S"),
        }
    )
