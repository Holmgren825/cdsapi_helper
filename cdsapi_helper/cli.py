#!/usr/bin/env python
from copy import deepcopy
from itertools import product

import cdsapi
import click
import tomli

from .utils import build_request
from .download import send_request, update_request


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
@click.argument("spec_path", type=click.Path(exists=True))
def download_cds(spec_path: str) -> None:
    click.echo(f"Reading specification: {click.format_filename(spec_path)}")
    with open(spec_path, mode="rb") as fp:
        spec = tomli.load(fp)

    dataset = spec["dataset"]
    request = spec["request"]
    click.echo(f"Requesting variable: {request['variable']}.")

    to_permutate = [request[var] for var in spec["looping_variables"]]
    requests = []
    for perm_spec in product(*to_permutate):
        perm_spec = {
            spec["looping_variables"][i]: perm_spec[i]
            for i in range(len(spec["looping_variables"]))
        }
        sub_request = deepcopy(request)
        for key, value in perm_spec.items():
            sub_request[key] = value
        requests.append(sub_request)

    # Send the request
    send_request(dataset, requests)
    # Update request
    update_request()
