#! /usr/bin/env python3

import argparse
import json
from typing import Dict, Sequence, Iterator, Optional
import sys

from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .usererror import UserError
from .logger import logger

import kiveapi


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search for a Kive batch.")

    parser.add_argument("--name", help="Name of the batch contains.")
    parser.add_argument("--description",
                        help="Description of the batch contains.")

    parser.add_argument("--page_size", type=int, default=1000,
                        help="Number of results per page (default is 1000).")
    parser.add_argument("--json", action='store_true',
                        help="Print all info for the matching runs.")

    return parser


def build_search_query(name: Optional[str],
                       description: Optional[str],
                       page_size: int,
                       ) -> Dict[str, object]:
    query: Dict[str, object] = {'page_size': page_size}

    for i, (key, val) in enumerate([('name', name),
                                    ('description', description),
                                    ]):
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
                    data = kive.endpoints.batches.get(params=query)

                yield from data['results']
                sys.stdout.flush()

                url = data.get('next')
                if not url:
                    break
            except KeyError as err:
                logger.error("Unexpected response structure: %s", err)
                break
            except kiveapi.KiveServerException as err:
                logger.error("Failed to retrieve batches: %s", err)
                break
            except kiveapi.KiveClientException as err:
                logger.error("Failed to retrieve batches: %s", err)
                break


def main_typed(name: Optional[str],
               description: Optional[str],
               page_size: int = 1000,
               is_json: bool = False,
               ) -> None:

    query = build_search_query(name=name,
                               description=description,
                               page_size=page_size,
                               )
    logger.debug("Built search query %r.", query)

    try:
        batches = fetch_paginated_results(query)
    except Exception as err:
        raise UserError("An error occurred while searching: %s", err)

    if is_json:
        sys.stdout.write("[")

    for i, run in enumerate(batches):
        if is_json:
            if i > 0:
                sys.stdout.write(",")
            json.dump(run, sys.stdout, indent=2)
        else:
            print(run["id"])

    if is_json:
        sys.stdout.write("]")

    sys.stdout.flush()


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    main_typed(args.name, args.description, args.page_size, args.json)
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
