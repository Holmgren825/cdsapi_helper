import os
from functools import partial
from multiprocessing.pool import ThreadPool
from typing import Union

import cdsapi
import click
import pandas as pd
from requests.exceptions import HTTPError

from .utils import build_filename, get_json_sem_hash, request_to_df

REQUEST_STATUS_CSV = "./cds_requests.csv"


def send_requests(
    dataset: str, request: Union[dict, list[dict]], queue_limit: int, dry_run: bool
) -> None:
    """Send requests to the CDS server."""
    client = cdsapi.Client(wait_until_complete=False, delete=False)

    if os.path.exists(REQUEST_STATUS_CSV):
        df = pd.read_csv(REQUEST_STATUS_CSV, index_col=0, dtype=str)
        current_online_queue = (~df.state.isin(["downloaded", "completed"])).sum()
    else:
        df = pd.DataFrame()
        current_online_queue = 0

    if isinstance(request, dict):
        request = [request]

    for req in request:
        req_hash = get_json_sem_hash(req)
        try:
            duplicate = df["request_hash"].isin([req_hash]).any()
        except KeyError:
            duplicate = False
        if current_online_queue < queue_limit:
            if not duplicate:
                if not dry_run:
                    result = client.retrieve(dataset, req)
                    reply = result.reply
                    current_online_queue += 1
                else:
                    reply = {"state": "test_state", "request_id": "test_id"}
                r_df = request_to_df(req, reply, req_hash)
                df = pd.concat([df, r_df])
            else:
                click.echo("Request already submitted.")

    # Save it.
    df = df.reset_index(drop=True)
    df.to_csv(REQUEST_STATUS_CSV)


def update_requests(dry_run: bool) -> None:
    """Update the status of the requests stored in the `cds_requests.csv` file."""
    client = cdsapi.Client(timeout=600, wait_until_complete=False, delete=False)
    try:
        df = pd.read_csv("./cds_requests.csv", index_col=0, dtype=str)
    except FileNotFoundError:
        click.echo("Nothing to update.")
        return

    click.echo("Updating requests...")
    for request in df.itertuples():
        if request.state not in ("completed", "downloaded", "deleted"):
            try:
                if not dry_run:
                    result = client.client.get_remote(request.request_id)
                    result.update()
                    df.at[request.Index, "state"] = result.reply["state"]
            except HTTPError as err:
                click.echo(f"Request {request.Index} not found")
                click.echo(err)
                df.at[request.Index, "state"] = "deleted"

    df.to_csv("./cds_requests.csv")


def download_requests(
    filename_spec: list[str], n_jobs: int = 5, dry_run: bool = False
) -> None:
    """Download requests which are marked as completed.

    Arguments:
    ---------
    filename_spec: list[str]
        List of parts of request from which to build the filename.
    n_jobs: int, default=5
        Number of files to download in parallel.
    dry_run: bool, default=False
        For testing purposes. No files are downloaded.

    """
    try:
        df = pd.read_csv("./cds_requests.csv", index_col=0, dtype=str)
    except FileNotFoundError as err:
        raise FileNotFoundError("`cds_requests.csv not found.") from err
    client = cdsapi.Client(timeout=600, wait_until_complete=False, delete=False)
    click.echo("Downloading completed requests...")
    # Some parallel downloads.
    download_helper_p = partial(
        _download_helper, filename_spec=filename_spec, client=client, dry_run=dry_run
    )
    with ThreadPool(processes=n_jobs) as p:
        results = p.map(download_helper_p, df.itertuples())

    # Write new states.
    df.state = results
    # Save them.
    df.to_csv("./cds_requests.csv")


def _download_helper(
    request: pd.core.frame.pandas,
    filename_spec: list,
    client: cdsapi.Client,
    dry_run: bool = False,
) -> str:
    if request.state == "completed":
        try:
            result = client.client.get_remote(request.request_id)
            result.update()
            filename = build_filename(request, filename_spec)
            if not dry_run:
                result.download(filename)
                return "downloaded"
            else:
                return request.state
        except HTTPError as e:
            print("Request not found")
            print(e)
            return request.state
    else:
        # No change to state.
        return request.state
