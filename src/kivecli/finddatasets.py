#! /usr/bin/env python3

import argparse
from typing import Mapping, Sequence, Iterator, Optional, MutableMapping
import sys

from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .usererror import UserError
from .logger import logger
from .dataset import Dataset

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


def build_search_query(name: Optional[str],
                       md5: Optional[str],
                       page_size: int,
                       ) -> Mapping[str, object]:
    query: MutableMapping[str, object] = {'page_size': page_size}

    for i, (key, val) in enumerate([('name', name),
                                    ('md5', md5),
                                    ]):
        if val is not None:
            query[f'filters[{i}][key]'] = key
            query[f'filters[{i}][val]'] = val

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
                    data = kive.endpoints.datasets.get(params=query)

                for raw in data['results']:
                    yield Dataset._from_json(raw)

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


def finddatasets(name: Optional[str] = None,
                 md5: Optional[str] = None,
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
               md5: Optional[str] = None,
               page_size: int = DEFAULT_PAGESIZE,
               is_json: bool = False,
               ) -> None:

    datasets = finddatasets(name=name,
                            md5=md5,
                            page_size=page_size,
                            )

    if is_json:
        sys.stdout.write("[")

    for i, dataset in enumerate(datasets):
        if is_json:
            if i > 0:
                sys.stdout.write(",")
            dataset.dump(sys.stdout)
            sys.stdout.flush()
        else:
            print(dataset.id)

    if is_json:
        sys.stdout.write("]")

    sys.stdout.flush()


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    main_typed(args.name, args.md5, args.page_size, args.json)
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
