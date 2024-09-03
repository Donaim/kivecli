#! /usr/bin/env python3

import argparse
import sys
import logging
from typing import Sequence

import kivecli.runkive as runkive
import kivecli.zip as kiveclizip


# Set up the logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class UserError(RuntimeError):
    def __init__(self, fmt: str, *fmt_args: object):
        self.fmt = fmt
        self.fmt_args = fmt_args
        self.code = 1


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a script on Kive.")

    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument('--verbose', action='store_true',
                                 help='Increase output verbosity.')
    verbosity_group.add_argument('--no-verbose', action='store_true',
                                 help='Normal output verbosity.', default=True)
    verbosity_group.add_argument('--debug', action='store_true',
                                 help='Maximum output verbosity.')
    verbosity_group.add_argument('--quiet', action='store_true',
                                 help='Minimize output verbosity.')

    parser.add_argument("program",
                        choices=["run", "zip"],
                        help="Program to run.")

    parser.add_argument("arguments", nargs="*",
                        help="Arguments to the script.")

    return parser


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parser.parse_args(argv)
    if args.quiet:
        logger.setLevel(logging.ERROR)
    elif args.verbose:
        logger.setLevel(logging.INFO)
    elif args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARN)

    if args.program == "run":
        return runkive.main(args.inputs)
    elif args.program == "run":
        return kiveclizip.main(args.inputs)
    else:
        raise RuntimeError(f"Unknown program value {repr(args.program)}")


if __name__ == '__main__':
    try:
        rc = main(sys.argv[1:])
        logger.debug("Done.")
    except BrokenPipeError:
        logger.debug("Broken pipe.")
        rc = 1
    except KeyboardInterrupt:
        logger.debug("Interrupted.")
        rc = 1
    except UserError as e:
        logger.fatal(e.fmt, *e.fmt_args)
        rc = e.code

    sys.exit(rc)
