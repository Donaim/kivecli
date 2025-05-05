#! /usr/bin/env python

import argparse
from typing import Sequence


from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .findrun import find_run
from .await_containerrrun import await_containerrun


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Watch a Kive run.")

    parser.add_argument("run_id", type=int,
                        help="Run ID of the target Kive run.")

    return parser


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)

    with login():
        containerrun = find_run(args.run_id)
        await_containerrun(containerrun)
        return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
