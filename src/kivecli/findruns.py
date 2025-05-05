#! /usr/bin/env python3

import argparse
import json
from typing import Sequence, Iterator, Mapping
import sys

from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .usererror import UserError
from .logger import logger
from .kiverun import KiveRun

import kiveapi


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search for Kive container runs.")

    parser.add_argument(
        "--filter", action='append', nargs=2, metavar=('key', 'val'),
        help="Filter key and value pair used for search, "
        "e.g., `--filter states F` - for runs that failed.")

    parser.add_argument("--page_size", type=int, default=1000,
                        help="Number of results per page (default is 1000).")

    parser.add_argument("--json", action='store_true',
                        help="Print all info for the matching runs.")

    return parser


def build_search_query(args: argparse.Namespace) -> Mapping[str, object]:
    query: dict[str, object] = {'page_size': int(str(args.page_size))}

    if args.filter:
        for i, (key, val) in enumerate(args.filter):
            query[f'filters[{i}][key]'] = key
            query[f'filters[{i}][val]'] = val

    return query


def fetch_paginated_results(query: Mapping[str, object]) \
        -> Iterator[KiveRun]:

    with login() as kive:
        url = None
        while True:
            try:
                if url:
                    response = kive.get(url)
                    response.raise_for_status()
                    data = response.json()
                else:
                    data = kive.endpoints.containerruns.get(params=query)

                for run in data['results']:
                    yield KiveRun.from_json(run)
                sys.stdout.flush()

                url = data.get('next')
                if not url:
                    break
            except KeyError as err:
                logger.error("Unexpected response structure: %s", err)
                break
            except kiveapi.KiveServerException as err:
                logger.error("Failed to retrieve container runs: %s", err)
                break
            except kiveapi.KiveClientException as err:
                logger.error("Failed to retrieve container runs: %s", err)
                break


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)

    query = build_search_query(args)
    logger.debug("Built search query %r.", query)

    try:
        containerruns = fetch_paginated_results(query)
    except Exception as err:
        raise UserError("An error occurred while searching: %s", err)

    is_json: bool = args.json

    if is_json:
        sys.stdout.write("[")

    for i, run in enumerate(containerruns):
        if is_json:
            if i > 0:
                sys.stdout.write(",")
            run.dump(sys.stdout)
        else:
            print(run.id.value)

    if is_json:
        sys.stdout.write("]")

    sys.stdout.flush()

    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
