#! /usr/bin/env python3

import sys
from typing import Sequence

import kivecli.runkive as runkive
import kivecli.zip as kiveclizip
import kivecli.rerun as rerun
from .mainwrap import mainwrap

PROGRAMS = ["run", "zip", "rerun"]

HELP_MESSAGE = """\
usage: kivecli [-h] {programs} [arguments ...]

Use Kive via CLI.

positional arguments:
  {programs} Program to run.
  arguments Arguments to the script.

options:
  -h, --help  show this help message and exit
""".format(programs='{' + ','.join(PROGRAMS) + '}')

PROGRAM_ERROR_MESSAGE = """\
usage: main.py [-h] {programs} [arguments ...]
main.py: error: argument program: invalid choice: 'hello' (choose from {lst})
""".format(programs='{' + ','.join(PROGRAMS) + '}',
           lst=', '.join(map(repr, PROGRAMS)))

PROGRAM_MISSING_MESSAGE = """\
usage: main.py [-h] {programs} [arguments ...]
kivecli: error: the following arguments are required: program, arguments
""".format(programs='{' + ','.join(PROGRAMS) + '}')


def main(argv: Sequence[str]) -> int:
    if len(argv) <= 1:
        print(PROGRAM_MISSING_MESSAGE, file=sys.stderr, end='')
        return 1

    program = argv[0]
    if program == "-h" or program == "--help":
        print(HELP_MESSAGE, file=sys.stdout, end='')
        return 0

    if program not in PROGRAMS:
        print(PROGRAM_ERROR_MESSAGE, file=sys.stderr, end='')
        return 1

    arguments = argv[1:]
    if program == "run":
        return runkive.main(arguments)
    elif program == "zip":
        return kiveclizip.main(arguments)
    elif program == "rerun":
        return rerun.main(arguments)
    else:
        raise RuntimeError(f"Unknown program value {repr(program)}")


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
