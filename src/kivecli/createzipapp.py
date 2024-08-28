#! /usr/bin/env python3

import os
import argparse
import sys
import logging
from typing import Sequence, BinaryIO
import tempfile

from .zip import zip_directory_to_stream
from .createpipelinejson import print_pipeline_json


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
    parser = argparse.ArgumentParser(
        description='Create a "free" Kive app definition.')

    parser.add_argument('--output', type=argparse.FileType("wb"),
                        required=True, help='Path to the output zip archive.')
    parser.add_argument("--ninputs", type=int, required=True,
                        help="Number of input arguments this app supports.")
    parser.add_argument("--noutputs", type=int, required=True,
                        help="Number of output arguments this app supports.")

    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument('--verbose', action='store_true',
                                 help='Increase output verbosity.')
    verbosity_group.add_argument('--no-verbose', action='store_true',
                                 help='Normal output verbosity.', default=True)
    verbosity_group.add_argument('--debug', action='store_true',
                                 help='Maximum output verbosity.')
    verbosity_group.add_argument('--quiet', action='store_true',
                                 help='Minimize output verbosity.')

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
    args = parser.parse_args()
    if args.quiet:
        logger.setLevel(logging.ERROR)
    elif args.verbose:
        logger.setLevel(logging.INFO)
    elif args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARN)

    create_app_zip(output=args.output,
                   ninputs=args.ninputs,
                   noutputs=args.noutputs)

    return 0


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
