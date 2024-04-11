#!/usr/bin/env python
import os
import sys
from copy import deepcopy
from itertools import product
from pathlib import Path
from time import sleep
from typing import List, Optional
import datetime

import cdsapi
import click
import pandas as pd
import tomli

from .download import (
    download_request,
    get_json_sem_hash,
    send_request,
    update_request,
    RequestEntry,
)
from .utils import (
    build_filename,
    build_request,
    resolve_and_get_local_cache,
    print_files_and_size,
    format_bytes,
)


@click.command()
@click.argument(
    "variable",
    nargs=1,
    type=click.STRING,
)
@click.argument("year", nargs=1, type=click.INT)
@click.argument("month", nargs=1, type=click.INT)
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    show_default=True,
    default=False,
    help="Dry run, only prints the request. No download.",
)
def download_era5(variable: str, year: str, month: str, dry_run: bool) -> None:
    """Download a ERA5 dataset.

    VARIABLE: e.g. specific_humidity, u_component_of_wind

    YEAR: e.g. 2020, 1987

    MONTH: e.g. 5, 8. 0 for all months.

    """
    c = cdsapi.Client()
    request = build_request(variable, year, month)

    if not dry_run:
        c.retrieve(
            "reanalysis-era5-pressure-levels",
            request,
            # FIX: For now, pressure levels and area are fixed.
            f"era5-{variable}-{year}_{month}-psl_1000_300-lat_90_40.nc",
        )
    else:
        print(request)


@click.command()
@click.argument("spec_paths", type=click.Path(exists=True), nargs=-1)
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    show_default=True,
    default=False,
    help="Dry run: no download and no symlinks.",
)
@click.option(
    "--list-dangling",
    "list_dangling",
    is_flag=True,
    show_default=True,
    default=False,
    help="List files in stdout for files that are not accounted for by spec file and exit",
)
@click.option(
    "--list-files",
    "list_files",
    is_flag=True,
    show_default=True,
    default=False,
    help="List cache files expected by the specification and exit.",
)
@click.option(
    "--n-jobs",
    "n_jobs",
    show_default=True,
    default=5,
    type=click.INT,
)
@click.option(
    "--wait",
    "wait",
    is_flag=True,
    type=click.BOOL,
    help="Keep running, waiting for requests to be processed.",
)
@click.option(
    "--cache-dir",
    "cache_dir",
    show_default=True,
    default=Path("./cache"),
    type=Path,
    help="Directory for local cache where downloads are stored and output files are linked to",
)
def download_cds(
    spec_paths: List[str],
    n_jobs: int,
    wait: bool,
    list_dangling: bool,
    list_files: bool,
    dry_run: bool,
    cache_dir: Path,
) -> None:
    request_entries = []
    filename_specs = []
    for spec_path in spec_paths:
        num_request_for_spec = 0
        click.echo(
            f"Reading specification: {click.format_filename(spec_path)}", err=True
        )
        with open(spec_path, mode="rb") as fp:
            spec = tomli.load(fp)

        dataset = spec["dataset"]
        request = spec["request"]
        filename_specs.append(spec["filename_spec"])
        to_permutate = [request[var] for var in spec["looping_variables"]]
        for perm_spec in product(*to_permutate):
            perm_spec = {
                spec["looping_variables"][i]: perm_spec[i]
                for i in range(len(spec["looping_variables"]))
            }
            sub_request = deepcopy(request)
            for key, value in perm_spec.items():
                sub_request[key] = value
            request_entries.append(
                RequestEntry(
                    dataset=dataset,
                    request=sub_request,
                    filename_spec=spec["filename_spec"],
                )
            )
            num_request_for_spec += 1

        click.echo(
            f"{num_request_for_spec} request(s) generated by {spec_path}.", err=True
        )

    click.echo(f"{len(request_entries)} request(s) generated in total.", err=True)

    # Remove requests with invalid dates
    def has_valid_date(req: RequestEntry):
        year = int(req.request["year"])
        month = int(req.request["month"])
        day = int(req.request["day"])

        try:
            _ = datetime.datetime(year=year, month=month, day=day)
        except ValueError:
            return False

        return True

    request_entries = list(filter(has_valid_date, request_entries))
    click.echo(
        f"{len(request_entries)} request(s) remain after removing invalid dates.",
        err=True,
    )

    local_cache = resolve_and_get_local_cache(cache_dir)
    remaining_requests = list(
        filter(
            lambda r: r.get_sha256() not in local_cache,
            request_entries,
        )
    )

    click.echo(
        f"{len(request_entries)-len(remaining_requests)} local cache hits", err=True
    )
    click.echo(f"{len(remaining_requests)} local cache misses", err=True)

    if list_dangling:
        print_files_and_size([cache_dir / file for file in dangling_cache_files])
        sys.exit(0)

    if list_files:
        print_files_and_size([cache_dir / r.get_sha256() for r in request_entries])
        sys.exit(0)

    send_request(remaining_requests, dry_run)

    # Check or wait for remaining_requests
    check_request_again = True
    while check_request_again:
        # First we try to download, likely in queue.
        download_request(cache_dir, n_jobs=n_jobs, dry_run=dry_run)
        # Then we update the request.
        update_request(dry_run)
        # How should we wait?
        if wait:
            try:
                df = pd.read_csv("./cds_requests.csv", index_col=0, dtype=str)
            # This shouldn't happen at this point.
            except FileNotFoundError:
                print("This shouldn't happen.")
            # Anything in the queue ready for download?
            if (df.state == "completed").any():
                # Should go back up to download_request.
                continue
            # Everything is in the queue.
            elif (df.state == "queued").any():
                # Wait 30 minutes before checking the status again.
                sleep(60 * 30)
            else:
                check_request_again = False
        else:
            check_request_again = False

    # Check that all requests are downloaded.
    for req_entry in request_entries:
        cache_file = cache_dir / req_entry.get_sha256()
        if not cache_file.exists():
            print(f"All requests are not downloaded. Exiting.")
            print(
                f"Missing expected cache file {cache_file} for request {req_entry.request}"
            )
            sys.exit(1)

    # Create links to cached files according to filename_spec
    num_links_per_spec = {s: 0 for s in filename_specs}
    count_missing = 0
    for req_entry in request_entries:
        output_file = Path(
            build_filename(
                req_entry.dataset, req_entry.request, req_entry.filename_spec
            )
        )
        cache_file = cache_dir / req_entry.get_sha256()

        if not cache_file.exists():
            print(f"Warning: Missing entry {cache_file} for {req_entry.request}")
            count_missing = count_missing + 1

        output_file.parent.mkdir(parents=True, exist_ok=True)
        if output_file.exists():
            os.remove(output_file)

        os.symlink(cache_file.absolute(), output_file)
        num_links_per_spec[req_entry.filename_spec] += 1

    for spec, num in num_links_per_spec.items():
        print(f'Created {num} symlinks for filename_spec "{spec}"')

    assert count_missing == 0, "There were missing files!"

    # List summary of files not declared by input specs
    local_cache = resolve_and_get_local_cache(cache_dir)
    dangling_cache_files = set(local_cache) - {r.get_sha256() for r in request_entries}
    if len(dangling_cache_files) > 0:
        dangling_bytes = 0
        for file in dangling_cache_files:
            dangling_bytes += (cache_dir / file).stat().st_size
        print(
            f"There are {len(dangling_cache_files)} ({format_bytes(dangling_bytes)}) dangling cache files not accounted for by input spec files."
        )
        print(f"Use `--list-dangling` to display these files.")
