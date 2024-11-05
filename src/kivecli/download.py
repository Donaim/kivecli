#! /usr/bin/env python

import argparse
import os
from typing import Sequence, Dict, Iterable

import kiveapi

from .logger import logger
from .dirpath import dir_path, DirPath
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .findrun import find_run
from .collect_run_files import collect_run_files
from .dataset import Dataset
from .escape import escape
from .usererror import UserError
from .await_containerrrun import await_containerrun
from .runfilesfilter import RunFilesFilter


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download run outputs.")

    parser.add_argument("--run_id", type=int, required=True,
                        help="Run ID of the target Kive run.")
    parser.add_argument("--output", type=dir_path, required=True,
                        help="Output folder where results are downloaded."
                        " Not downloading by default.")
    parser.add_argument("--nowait", action='store_true', default=False,
                        help="Do not wait until the run is finished.")
    parser.add_argument("--runfilter", type=RunFilesFilter.parse,
                        default=RunFilesFilter.default(),
                        help="Filter for files to be downloaded.")

    return parser


def download_results(datasets: Iterable[Dataset],
                     output: DirPath) -> None:
    if output is not None:
        logger.debug("Making output directory at %s.", escape(output))
        os.makedirs(output, exist_ok=True)

    for dataset in datasets:
        dataset.download(output)


def main_after_wait(kive: kiveapi.KiveAPI,
                    output: DirPath,
                    containerrun: Dict[str, object],
                    runfilter: RunFilesFilter,
                    ) -> int:

    datasets = list(collect_run_files(containerrun, runfilter))
    if len(datasets) <= 0:
        raise UserError("Could not find any outputs for run with id %s.",
                        containerrun["id"])

    download_results(datasets, output)

    return 0


def main_with_run(kive: kiveapi.KiveAPI,
                  output: DirPath,
                  containerrun: Dict[str, object],
                  nowait: bool,
                  runfilter: RunFilesFilter,
                  ) -> int:

    if not nowait:
        await_containerrun(kive, containerrun)

    return main_after_wait(
        kive=kive,
        output=output,
        containerrun=containerrun,
        runfilter=runfilter,
    )


def main_parsed(kive: kiveapi.KiveAPI,
                output: DirPath,
                run_id: int,
                nowait: bool,
                runfilter: RunFilesFilter,
                ) -> int:

    containerrun = find_run(kive, run_id)
    return main_with_run(kive, output, containerrun, nowait, runfilter)


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    with login() as kive:
        return main_parsed(kive=kive,
                           output=args.output,
                           run_id=args.run_id,
                           nowait=args.nowait,
                           runfilter=args.runfilter,
                           )


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
