#! /usr/bin/env python3

import os
import argparse
from typing import Sequence, BinaryIO
import tempfile

from .zip import zip_directory_to_stream
from .createpipelinejson import print_pipeline_json
from .mainwrap import mainwrap
from .parsecli import parse_cli


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Create a "free" Kive app definition.')

    parser.add_argument('--output', type=argparse.FileType("wb"),
                        required=True, help='Path to the output zip archive.')
    parser.add_argument("--ninputs", type=int, required=True,
                        help="Number of input arguments this app supports.")
    parser.add_argument("--noutputs", type=int, required=True,
                        help="Number of output arguments this app supports.")

    return parser


def create_app_zip(output: BinaryIO, ninputs: int, noutputs: int) -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        kivesubdir = os.path.join(tmpdirname, 'kive')
        pipelinepath = os.path.join(tmpdirname, 'kive', 'pipeline1.json')
        driverpath = os.path.join(tmpdirname, 'main.sh')

        os.makedirs(kivesubdir, exist_ok=True)
        with open(pipelinepath, "wt") as writer:
            print_pipeline_json(ninputs=ninputs,
                                noutputs=noutputs,
                                output=writer)

        with open(driverpath, "wt") as writer:
            print("""\
#! /bin/sh

INPUTSCRIPT="$1"
shift

chmod a+x -- "$INPUTSCRIPT"
"$INPUTSCRIPT" "$@"

""", file=writer)

        zip_directory_to_stream(tmpdirname, output)


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    create_app_zip(output=args.output,
                   ninputs=args.ninputs,
                   noutputs=args.noutputs)

    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
