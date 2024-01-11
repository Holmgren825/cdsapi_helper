from functools import partial
from multiprocessing.pool import ThreadPool
from typing import Union

import cdsapi
import pandas as pd
from requests.exceptions import HTTPError

from .utils import build_filename, get_json_sem_hash, request_to_df


def send_request(dataset: str, request: Union[dict, list[dict]], dry_run: bool) -> None:
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
            if not dry_run:
                result = client.retrieve(dataset, req)
                reply = result.reply
            else:
                reply = {"state": "test_state", "request_id": "test_id"}
            r_df = request_to_df(req, reply, req_hash)
            df = pd.concat([df, r_df])
        else:
            print("Request already sent.")

    # Save it.
    df = df.reset_index(drop=True)
    df.to_csv("./cds_requests.csv")


def update_request(dry_run: bool) -> None:
    client = cdsapi.Client(wait_until_complete=False, delete=False)
    try:
        df = pd.read_csv("./cds_requests.csv", index_col=0)
    except FileNotFoundError:
        print("Nothing to update.")
        return

    print("Updating requests...")
    for request in df.itertuples():
        if request.state != "completed" and request.state != "downloaded":
            try:
                if not dry_run:
                    result = cdsapi.api.Result(
                        client, {"request_id": request.request_id}
                    )
                    result.update()
                    df.at[request.Index, "state"] = result.reply["state"]
            except HTTPError:
                print("Request not found")
                df.at[request.Index, "state"] = "deleted"

    df.to_csv("./cds_requests.csv")


def download_request(n_jobs: int = 5, dry_run: bool = False) -> None:
    try:
        df = pd.read_csv("./cds_requests.csv")
    except FileNotFoundError:
        return
    client = cdsapi.Client(wait_until_complete=False, delete=False)
    print("Downloading completed requests...")
    # Some parallel downloads.
    download_helper_p = partial(download_helper, client=client, dry_run=dry_run)
    with ThreadPool(processes=n_jobs) as p:
        results = p.map(download_helper_p, df.itertuples())

    # Write new states.
    df.state = results
    # Save them.
    df.to_csv("./cds_requests.csv")


def download_helper(
    request: pd.core.frame.pandas, client: cdsapi.Client, dry_run: bool = False
) -> str:
    if request.state == "completed":
        try:
            result = cdsapi.api.Result(client, {"request_id": request.request_id})
            result.update()
            filename = build_filename(request)
            if not dry_run:
                result.download(filename)
                return "downloaded"
            else:
                return request.state
        except HTTPError:
            print("Request not found")
            return request.state
    else:
        # No change to state.
        return request.state
