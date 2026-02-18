#! /usr/bin/env python3

import argparse
import sys
from typing import Iterator, Optional, Sequence

from .kivebatch import DEFAULT_PAGESIZE, KiveBatch
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .usererror import UserError


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search for a Kive batch.")

    parser.add_argument("--name", help="Name of the batch contains.")
    parser.add_argument("--description", help="Description of the batch contains.")

    parser.add_argument(
        "--page_size",
        type=int,
        default=DEFAULT_PAGESIZE,
        help="Number of results per page.",
    )
    parser.add_argument(
        "--json", action="store_true", help="Print all info for the matching runs."
    )

    return parser


def findbatches(
    name: Optional[str] = None,
    description: Optional[str] = None,
    page_size: int = DEFAULT_PAGESIZE,
) -> Iterator[KiveBatch]:
    try:
        yield from KiveBatch.search(
            name=name, description=description, page_size=page_size
        )
    except Exception as err:
        raise UserError("An error occurred while searching: %s", err)


def main_typed(
    name: Optional[str] = None,
    description: Optional[str] = None,
    page_size: int = DEFAULT_PAGESIZE,
    is_json: bool = False,
) -> None:

    batches = findbatches(
        name=name,
        description=description,
        page_size=page_size,
    )

    if is_json:
        sys.stdout.write("[")

    for i, run in enumerate(batches):
        if is_json:
            if i > 0:
                sys.stdout.write(",")
            run.dump(sys.stdout)
            sys.stdout.flush()
        else:
            print(run.id)

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


if __name__ == "__main__":
    cli()
