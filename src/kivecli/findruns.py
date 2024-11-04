#! /usr/bin/env python3

import argparse
import json
from typing import Dict, Sequence, Iterator

from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .usererror import UserError
from .logger import logger


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search for Kive container runs.")

    parser.add_argument(
        "--filter", action='append', nargs=2, metavar=('key', 'val'),
        help="Filter key and value pair used for search, "
        "e.g., `--filter state F` - for runs that failed.")

    parser.add_argument("--page_size", type=int, default=1000,
                        help="Number of results per page (default is 1000).")

    return parser


def build_search_query(args: argparse.Namespace) -> Dict[str, object]:
    query: Dict[str, object] = {'page_size': int(str(args.page_size))}

    if args.filter:
        for i, (key, val) in enumerate(args.filter):
            query[f'filters[{i}][key]'] = key
            query[f'filters[{i}][val]'] = val

    return query


def fetch_paginated_results(query: Dict[str, object]) \
        -> Iterator[Dict[str, object]]:

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

                yield from data['results']

                url = data.get('next')
                if not url:
                    break
            except KeyError as err:
                logger.error("Unexpected response structure: %s", err)
                break
            except RuntimeError as err:
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

    for run in containerruns:
        print(json.dumps(run, indent=2))

    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
