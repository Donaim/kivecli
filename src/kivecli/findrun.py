
import argparse
from typing import Dict, Sequence
import kiveapi
import sys
import json

from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .usererror import UserError
from .logger import logger
from .escape import escape
from .url import URL


def find_run(kive: kiveapi.KiveAPI, run_id: int) -> Dict[str, object]:
    try:
        containerrun: Dict[str, object] \
            = kive.endpoints.containerruns.get(run_id)
    except kiveapi.errors.KiveServerException as ex:
        raise UserError("Run with id %s not found: %s", run_id, ex) from ex

    url: str = str(containerrun["url"])
    name: str = str(containerrun["name"])
    logger.debug("Found run with id %s and name %s at %s.",
                 run_id, escape(name), escape(URL(url)))
    return containerrun


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


def main_typed(run_id: int) -> None:
    with login() as kive:
        try:
            run = find_run(kive, run_id)
        except Exception as err:
            raise UserError("An error occurred while searching: %s", err)

        json.dump(run, sys.stdout, indent=2)


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    main_typed(args.run_id)
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
