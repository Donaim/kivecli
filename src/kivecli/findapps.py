#! /usr/bin/env python3

import argparse
from typing import Mapping, Sequence, Iterator, Optional, MutableMapping
import sys

from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .usererror import UserError
from .logger import logger
from .containerapp import ContainerApp

import kiveapi


DEFAULT_PAGESIZE = 10


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search for a Kive container app.")

    parser.add_argument("--name", help="Name of the container app contains.")

    parser.add_argument("--page_size", type=int, default=DEFAULT_PAGESIZE,
                        help="Number of results per page.")
    parser.add_argument("--json", action='store_true',
                        help="Print all info for the matching container apps.")

    return parser


def build_search_query(page_size: int = DEFAULT_PAGESIZE,
                       name: Optional[str] = None,
                       ) -> Mapping[str, object]:
    query: MutableMapping[str, object] = {'page_size': page_size}

    i = 0
    for (key, val) in [('name', name),
                       ]:
        if val is None:
            continue

        query[f'filters[{i}][key]'] = key
        query[f'filters[{i}][val]'] = str(val)
        i += 1

    return query


def fetch_paginated_results(query: Mapping[str, object]) \
        -> Iterator[ContainerApp]:

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
                    yield ContainerApp._from_json(raw)

                url = data.get('next')
                if not url:
                    break
            except KeyError as err:
                logger.error("Unexpected response structure: %s", err)
                break
            except kiveapi.KiveServerException as err:
                logger.error("Failed to retrieve container apps: %s", err)
                break
            except kiveapi.KiveClientException as err:
                logger.error("Failed to retrieve container apps: %s", err)
                break


def findapps(name: Optional[str] = None,
             page_size: int = DEFAULT_PAGESIZE,
             ) -> Iterator[ContainerApp]:
    query = build_search_query(name=name,
                               page_size=page_size,
                               )
    logger.debug("Built search query %r.", query)

    try:
        yield from fetch_paginated_results(query)
    except Exception as err:
        raise UserError("An error occurred while searching: %s", err)


def main_typed(name: Optional[str] = None,
               page_size: int = DEFAULT_PAGESIZE,
               is_json: bool = False,
               ) -> None:

    apps = findapps(name=name,
                    page_size=page_size,
                    )

    if is_json:
        sys.stdout.write("[")

    for i, app in enumerate(apps):
        if is_json:
            if i > 0:
                sys.stdout.write(",")
            app.dump(sys.stdout)
            sys.stdout.flush()
        else:
            print(app.id)

    if is_json:
        sys.stdout.write("]")

    sys.stdout.flush()


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    main_typed(args.name, args.page_size, args.json)
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
