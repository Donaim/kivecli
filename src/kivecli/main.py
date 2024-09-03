#! /usr/bin/env python3

import argparse
import sys
from typing import Sequence

import kivecli.runkive as runkive
import kivecli.zip as kiveclizip


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Use Kive via CLI.")
    parser.add_argument("program",
                        choices=["run", "zip"],
                        help="Program to run.")
    parser.add_argument("arguments", nargs="*",
                        help="Arguments to the script.")

    return parser


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parser.parse_args(argv)

    if args.program == "run":
        return runkive.main(args.arguments)
    elif args.program == "run":
        return kiveclizip.main(args.arguments)
    else:
        raise RuntimeError(f"Unknown program value {repr(args.program)}")


if __name__ == '__main__':
    try:
        rc = main(sys.argv[1:])
    except BrokenPipeError:
        rc = 1
    except KeyboardInterrupt:
        rc = 1

    sys.exit(rc)
