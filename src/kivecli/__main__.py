#! /usr/bin/env python3

import sys
from typing import Sequence

import kivecli.check_rerun as check_rerun
import kivecli.createzipapp as createzipapp
import kivecli.download as kivedownload
import kivecli.findapps as findapps
import kivecli.findbatches as findbatches
import kivecli.findcontainerfamilies as findcontainerfamilies
import kivecli.finddatasets as finddatasets
import kivecli.findrun as findrun
import kivecli.findruns as findruns
import kivecli.makecontainer as makecontainer
import kivecli.rerun as rerun
import kivecli.runkive as runkive
import kivecli.stop as stop
import kivecli.upload_dataset as upload_dataset
import kivecli.watch as watch
import kivecli.zip as kiveclizip

from .mainwrap import mainwrap

PROGRAMS = ["run", "rerun", "check_rerun", "download", "watch",
            "createzipapp", "zip", "findruns", "findrun", "stop",
            "findbatches", "finddatasets", "findapps", "upload_dataset",
            "makecontainer", "findcontainerfamilies"]

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
usage: kivecli [-h] {programs} [arguments ...]
kivecli: error: argument program: invalid choice: {choice} (choose from {lst})
"""

PROGRAM_MISSING_MESSAGE = """\
usage: kivecli [-h] {programs} [arguments ...]
kivecli: error: the following arguments are required: program, arguments
""".format(programs='{' + ','.join(PROGRAMS) + '}')


def main(argv: Sequence[str]) -> int:
    if len(argv) < 1:
        print(PROGRAM_MISSING_MESSAGE, file=sys.stderr, end='')
        return 1

    program = argv[0]
    if program == "-h" or program == "--help":
        print(HELP_MESSAGE, file=sys.stdout, end='')
        return 0

    if program not in PROGRAMS:
        msg = PROGRAM_ERROR_MESSAGE.format(
            choice=repr(program),
            programs='{' + ','.join(PROGRAMS) + '}',
            lst=', '.join(map(repr, PROGRAMS)),
        )

        print(msg, file=sys.stderr, end='')
        return 1

    arguments = argv[1:]
    if program == "run":
        return runkive.main(arguments)
    elif program == "zip":
        return kiveclizip.main(arguments)
    elif program == "rerun":
        return rerun.main(arguments)
    elif program == "check_rerun":
        return check_rerun.main(arguments)
    elif program == "watch":
        return watch.main(arguments)
    elif program == "createzipapp":
        return createzipapp.main(arguments)
    elif program == "download":
        return kivedownload.main(arguments)
    elif program == "findruns":
        return findruns.main(arguments)
    elif program == "findrun":
        return findrun.main(arguments)
    elif program == "stop":
        return stop.main(arguments)
    elif program == "findbatches":
        return findbatches.main(arguments)
    elif program == "finddatasets":
        return finddatasets.main(arguments)
    elif program == "findapps":
        return findapps.main(arguments)
    elif program == "upload_dataset":
        return upload_dataset.main(arguments)
    elif program == "makecontainer":
        return makecontainer.main(arguments)
    elif program == "findcontainerfamilies":
        return findcontainerfamilies.main(arguments)
    else:
        raise RuntimeError(f"Unknown program value {repr(program)}")


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
