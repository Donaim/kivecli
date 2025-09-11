#! /usr/bin/env python3

import argparse
from typing import Sequence

from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .findrun import find_run
from .logger import logger
from .kiverun import KiveRun


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stop (cancel) a Kive run.")
    parser.add_argument("--run_id", type=int, required=True,
                        help="Run ID of the target Kive run.")
    return parser


def print_run(run: KiveRun) -> None:
    state = run.state
    end_time = run.end_time
    logger.info("Run in state %r with end_time %s.",
                state.value, end_time)


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)

    with login():
        containerrun = find_run(args.run_id)
        print_run(containerrun)
        if containerrun.is_finished:
            logger.info("Already finished.")
            return 0

        data = {
            "is_stop_requested": True,
        }
        runid = containerrun.id.value
        result = KiveRun.post(runid, data)
        print_run(result)
        return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
