from typing import Union

import cdsapi
import pandas as pd

from .utils import request_to_df


def send_request(dataset: str, request: Union[dict, list[dict]]) -> None:
    client = cdsapi.Client(wait_until_complete=False, delete=False)

    try:
        df = pd.read_csv("./cds_requests.csv")
    except FileNotFoundError:
        df = pd.DataFrame()

    if isinstance(request, dict):
        request = [request]

    for req in request:
        result = client.retrieve(dataset, req)
        r_df = request_to_df(request, result)
        pd.concat([df, r_df])

    # Save it.
    df = df.reset_index()
    df.to_csv("./cds_requests.csv")


def update_request():
    df = pd.read_csv("./cds_requests.csv")
    client = cdsapi.Client(wait_until_complete=False, delete=False)
    for i, request in df.iterrows():
        if request.state != "completed":
            new_result = cdsapi.api.Result(client, {"request_id": request.r_id})
            new_result.update()
            df.at[i, "state"] = new_result.reply["state"]

    df.to_csv("./cds_requests.csv")


def download_requests():
    df = pd.read_csv("./cds_requests.csv")
