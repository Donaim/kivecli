#! /usr/bin/env python3

import argparse
from typing import Mapping, Sequence, Iterator, Optional, MutableMapping
import sys
import json

from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .usererror import UserError
from .logger import logger
from .dataset import Dataset
from .md5checksum import MD5Checksum

import kiveapi


DEFAULT_PAGESIZE = 10


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search for a Kive dataset.")

    parser.add_argument("--name", help="Name of the dataset contains.")
    parser.add_argument("--md5", help="Checksum of the dataset.")

    parser.add_argument("--page_size", type=int, default=DEFAULT_PAGESIZE,
                        help="Number of results per page.")
    parser.add_argument("--json", action='store_true',
                        help="Print all info for the matching datasets.")

    return parser


def build_search_query(page_size: int = DEFAULT_PAGESIZE,
                       name: Optional[str] = None,
                       md5: Optional[MD5Checksum] = None,
                       ) -> Mapping[str, object]:
    query: MutableMapping[str, object] = {'page_size': page_size}

    i = 0
    for (key, val) in [('name', name),
                       ('md5', md5),
                       ]:
        if val is None:
            continue

        query[f'filters[{i}][key]'] = key
        query[f'filters[{i}][val]'] = str(val)
        i += 1

    return query


def fetch_paginated_results(query: Mapping[str, object]) \
        -> Iterator[Dataset]:

    with login() as kive:
        url = None
        while True:
            try:
                if url:
                    response = kive.get(url)
                    response.raise_for_status()
                    data = response.json()
                else:
                    data = kive.endpoints.containerapps.get(params=query)

                for raw in data['results']:
                    yield raw

                url = data.get('next')
                if not url:
                    break
            except KeyError as err:
                logger.error("Unexpected response structure: %s", err)
                break
            except kiveapi.KiveServerException as err:
                logger.error("Failed to retrieve datasets: %s", err)
                break
            except kiveapi.KiveClientException as err:
                logger.error("Failed to retrieve datasets: %s", err)
                break


def findapps(name: Optional[str] = None,
                 md5: Optional[MD5Checksum] = None,
                 page_size: int = DEFAULT_PAGESIZE,
                 ) -> Iterator[Dataset]:
    query = build_search_query(name=name,
                               md5=md5,
                               page_size=page_size,
                               )
    logger.debug("Built search query %r.", query)

    try:
        yield from fetch_paginated_results(query)
    except Exception as err:
        raise UserError("An error occurred while searching: %s", err)


def main_typed(name: Optional[str] = None,
               md5: Optional[MD5Checksum] = None,
               page_size: int = DEFAULT_PAGESIZE,
               is_json: bool = False,
               ) -> None:

    datasets = findapps(name=name,
                        md5=md5,
                        page_size=page_size,
                        )

    if is_json:
        sys.stdout.write("[")

    for i, dataset in enumerate(datasets):
        if is_json:
            if i > 0:
                sys.stdout.write(",")
            print(json.dumps(dataset))
            sys.stdout.flush()
        else:
            print(dataset["id"])

    if is_json:
        sys.stdout.write("]")

    sys.stdout.flush()


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    md5 = args.md5 and MD5Checksum(args.md5)
    main_typed(args.name, md5, args.page_size, args.json)
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
