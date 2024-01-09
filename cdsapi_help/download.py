from typing import Union

import cdsapi
import pandas as pd
from requests.exceptions import HTTPError

from .utils import get_json_sem_hash, request_to_df


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


def update_request() -> None:
    print("hello")
    client = cdsapi.Client(wait_until_complete=False, delete=False)
    try:
        df = pd.read_csv("./cds_requests.csv", index_col=0)
    except FileNotFoundError:
        print("Nothing to update.")
        return

    print("Updating requests...")
    for i, request in df.iterrows():
        if request.state != "completed":
            try:
                new_result = cdsapi.api.Result(
                    client, {"request_id": request.request_id}
                )
                new_result.update()
                df.at[i, "state"] = new_result.reply["state"]
            except HTTPError:
                print("Request not found")
                df.at[i, "state"] = "deleted"

    df.to_csv("./cds_requests.csv")


def download_requests():
    df = pd.read_csv("./cds_requests.csv")
