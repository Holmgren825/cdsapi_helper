from typing import Union

import cdsapi
import pandas as pd

from .utils import request_to_df, get_json_sem_hash


def send_request(dataset: str, request: Union[dict, list[dict]]) -> None:
    client = cdsapi.Client(wait_until_complete=False, delete=False)

    try:
        df = pd.read_csv("./cds_requests.csv", index_col=0)
    except FileNotFoundError:
        df = pd.DataFrame()

    if isinstance(request, dict):
        request = [request]

    for req in request:
        req_hash = get_json_sem_hash(req)
        try:
            duplicate = df["request_hash"].isin([req_hash]).any()
        except KeyError:
            duplicate = False
        if not duplicate:
            result = client.retrieve(dataset, req)
            r_df = request_to_df(req, result.reply, req_hash)
            df = pd.concat([df, r_df])
        else:
            print("Request already sent.")

    # Save it.
    df = df.reset_index(drop=True)
    df.to_csv("./cds_requests.csv")


def update_request(df, client):
    print("Updating requests...")
    for i, request in df.iterrows():
        if request.state != "completed":
            pass
            # new_result = cdsapi.api.Result(client, {"request_id": request.r_id})
            # new_result.update()
            # df.at[i, "state"] = new_result.reply["state"]

    df.to_csv("./cds_requests.csv")


def download_requests():
    df = pd.read_csv("./cds_requests.csv")
