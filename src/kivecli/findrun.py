
import argparse
from typing import Sequence
import sys

from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .usererror import UserError
from .logger import logger
from .escape import escape
from .kiverun import KiveRun


def find_run(run_id: int) -> KiveRun:
    run = KiveRun.get(run_id)
    if run is None:
        raise UserError("Run with id %s not found.", run_id)

    logger.debug("Found run with id %s and name %s at %s.",
                 run_id, escape(run.name), escape(run.url))
    return run


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search for a particular Kive container run.")
    parser.add_argument("--run_id", type=int, required=True,
                        help="Run ID of the target Kive run.")
    return parser


def main_typed(run_id: int) -> None:
    with login():
        try:
            run = find_run(run_id)
        except Exception as err:
            raise UserError("An error occurred while searching: %s", err)

        run.dump(sys.stdout)


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    main_typed(args.run_id)
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
