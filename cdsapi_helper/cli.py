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


def generate_request_entries_from_specs(spec_paths):
    request_entries = []
    for spec_path in spec_paths:
        num_request_for_spec = 0
        with open(spec_path, mode="rb") as fp:
            spec = tomli.load(fp)

        dataset = spec["dataset"]
        request = spec["request"]
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

    return request_entries


@click.group()
@click.option(
    "--cache-dir",
    "cache_dir",
    show_default=True,
    default=Path("./cache"),
    type=Path,
    help="Directory for local cache where downloads are stored and output files are linked to",
)
@click.pass_context
def download_cds(
    ctx,
    cache_dir: Path,
) -> None:
    ctx.ensure_object(dict)
    ctx.obj["cache_dir"] = cache_dir


@download_cds.command(
    help="List files in stdout for files that are not accounted for by spec files and exit",
)
@click.argument("spec_paths", type=click.Path(exists=True), nargs=-1)
@click.pass_context
def list_dangling(
    ctx,
    spec_paths: List[str],
) -> None:
    cache_dir = ctx.obj["cache_dir"]
    request_entries = generate_request_entries_from_specs(spec_paths)
    local_cache = resolve_and_get_local_cache(cache_dir)
    dangling_cache_files = set(local_cache) - {r.get_sha256() for r in request_entries}
    print_files_and_size([cache_dir / file for file in dangling_cache_files])
    sys.exit(0)


@download_cds.command(
    help="List cache files in stdout expected by the specifications and exit. Exit success if all files exist.",
)
@click.argument("spec_paths", type=click.Path(exists=True), nargs=-1)
@click.pass_context
def list_files(
    ctx,
    spec_paths: List[str],
) -> None:
    cache_dir = ctx.obj["cache_dir"]

    request_entries = generate_request_entries_from_specs(spec_paths)
    expected_files = {cache_dir / file.get_sha256() for file in request_entries}

    for file in expected_files:
        click.echo(file)

    # Summary about size and potentially missing files
    local_cache = {cache_dir / file for file in resolve_and_get_local_cache(cache_dir)}
    expected_existing = local_cache.intersection(expected_files)

    num_bytes_existing = sum((f.stat().st_size for f in expected_existing))
    click.echo(f"Existing files amount to {format_bytes(num_bytes_existing)}", err=True)

    expected_missing = expected_files - expected_existing
    click.echo(
        f"There are {len(expected_existing)} expected files that exist.",
        err=True,
    )
    click.echo(
        click.style(
            f"There are {len(expected_missing)} expected files that are missing.",
            fg="green" if len(expected_missing) == 0 else "red",
        ),
        err=True,
    )
    if len(expected_missing) > 0:
        sys.exit(1)

    sys.exit(0)


@download_cds.command(
    help="Download files and create output directories according to spec files."
)
@click.pass_context
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
    "--output-dir",
    "output_dir",
    show_default=True,
    default=Path("./output"),
    type=Path,
    help="Directory from which to create files according to filename_spec in spec files.",
)
def download(
    ctx,
    spec_paths: List[str],
    n_jobs: int,
    wait: bool,
    dry_run: bool,
    output_dir: Path,
) -> None:
    cache_dir = ctx.obj["cache_dir"]

    request_entries = generate_request_entries_from_specs(spec_paths)
    click.echo(f"{len(request_entries)} request(s) generated in total.", err=True)

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
            click.echo(f"All requests are not downloaded. Exiting.", err=True)
            click.echo(
                click.style(
                    f"Missing expected cache file {cache_file} for request {req_entry.request}",
                    fg="red",
                ),
                err=True,
            )
            sys.exit(1)

    # Create links to cached files according to filename_spec
    num_links = 0
    count_missing = 0
    for req_entry in request_entries:
        output_file = output_dir / build_filename(
            req_entry.dataset, req_entry.request, req_entry.filename_spec
        )
        cache_file = cache_dir / req_entry.get_sha256()

        if not cache_file.exists():
            click.echo(
                f"Warning: Missing entry {cache_file} for {req_entry.request}", err=True
            )
            count_missing = count_missing + 1

        output_file.parent.mkdir(parents=True, exist_ok=True)
        if output_file.exists():
            os.remove(output_file)

        os.symlink(cache_file.absolute(), output_file)
        num_links += 1

    click.echo(f"Created {num_links} symlinks.", err=True)

    assert count_missing == 0, "There were missing files!"

    # List summary of files not declared by input specs
    local_cache = resolve_and_get_local_cache(cache_dir)
    dangling_cache_files = set(local_cache) - {r.get_sha256() for r in request_entries}
    if len(dangling_cache_files) > 0:
        dangling_bytes = 0
        for file in dangling_cache_files:
            dangling_bytes += (cache_dir / file).stat().st_size
        click.echo(
            f"There are {len(dangling_cache_files)} ({format_bytes(dangling_bytes)}) dangling cache files not accounted for by input spec files.",
            err=True,
        )
        click.echo(f"Use `list-dangling` subcommand to display these files.", err=True)
