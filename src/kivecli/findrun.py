
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
        description="Search for a particular Kive container run.")
    parser.add_argument("--run_id", type=int, required=True,
                        help="Run ID of the target Kive run.")
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
