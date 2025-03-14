#! /usr/bin/env python3

import argparse
import sys
from typing import Sequence, Iterator, List, Dict

import kiveapi

import kivecli.runkive as runkive
from .logger import logger
from .pathorurl import PathOrURL
from .url import URL
from .dirpath import dir_path
from .inputfileorurl import input_file_or_url
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .findrun import find_run
from .collect_run_files import collect_run_files
from .runfilesfilter import RunFilesFilter
from .argumenttype import ArgumentType


def collect_run_inputs(kive: kiveapi.KiveAPI,
                       containerrun: Dict[str, object]) -> Iterator[URL]:

    filefilter = RunFilesFilter.make([ArgumentType.INPUT], '.*')
    for dataset in collect_run_files(containerrun, filefilter):
        yield dataset.url


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rerun a Kive run.")

    parser.add_argument("--output", type=dir_path,
                        help="Output folder where results are downloaded."
                        " Not downloading by default.")

    parser.add_argument("--nowait", action='store_true', default=False,
                        help="Do not wait until the run is finished.")
    parser.add_argument("--filefilter", type=RunFilesFilter.parse,
                        default=RunFilesFilter.default(),
                        help="Filter for files to be downloaded.")
    parser.add_argument("--batch", help="Unique name for the batch.")
    parser.add_argument("--stdout",
                        default=sys.stdout.buffer,
                        type=argparse.FileType('wb'),
                        help="Redirected stdout to file.")
    parser.add_argument("--stderr",
                        default=sys.stderr.buffer,
                        type=argparse.FileType('wb'),
                        help="Redirected stderr to file.")

    parser.add_argument("--run_id", type=int, required=True,
                        help="Run ID of the target Kive run.")

    parser.add_argument("--app_id", type=int, required=True,
                        help="App ID on which the run will be restarted.")

    parser.add_argument("--prefix",
                        nargs="*",
                        type=input_file_or_url,
                        help="Files that will be added to the list of"
                        " inputs (prepended at the beginning).")

    return parser


def get_run_name(orig_run_name: str) -> str:
    name = f'Rerun {orig_run_name!r}'

    while len(name) >= 60:
        orig_run_name = orig_run_name[:-1]
        name = f'Rerun {orig_run_name!r}...'

    return name


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)

    with login() as kive:
        containerrun = find_run(kive, args.run_id)
        orig_run_name = str(containerrun["name"])
        run_name = get_run_name(orig_run_name)

        urls = list(collect_run_inputs(kive, containerrun))
        logger.debug("Collected %s datasets.", len(urls))

        prefix: List[PathOrURL] = args.prefix or []
        inputs = prefix + urls

        return runkive.main_parsed(
            output=args.output,
            batch=args.batch,
            run_name=run_name,
            stdout=args.stdout,
            stderr=args.stderr,
            app_id=args.app_id,
            inputs=inputs,
            nowait=args.nowait,
            filefilter=args.filefilter,
        )


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
