#! /usr/bin/env python

import argparse
import os
from typing import Sequence, Iterable

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
from .kiverun import KiveRun


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download run files.")

    parser.add_argument("--run_id", type=int, required=True,
                        help="Run ID of the target Kive run.")
    parser.add_argument("--output", type=dir_path, required=True,
                        help="Output folder where results are downloaded."
                        " Not downloading by default.")
    parser.add_argument("--nowait", action='store_true', default=False,
                        help="Do not wait until the run is finished.")
    parser.add_argument("--filefilter", type=RunFilesFilter.parse,
                        default=RunFilesFilter.default(),
                        help="Filter for files to be downloaded."
                        " All outputs are downloaded by default.")

    return parser


def download_results(datasets: Iterable[Dataset],
                     output: DirPath) -> None:
    if output is not None:
        logger.debug("Making output directory at %s.", escape(output))
        os.makedirs(output, exist_ok=True)

    for dataset in datasets:
        dataset.download(output)


def main_after_wait(output: DirPath,
                    containerrun: KiveRun,
                    filefilter: RunFilesFilter,
                    ) -> int:

    datasets = list(collect_run_files(containerrun, filefilter))
    if len(datasets) <= 0:
        raise UserError("Could not find any outputs for run with id %s.",
                        containerrun.id)

    download_results(datasets, output)

    return 0


def main_with_run(output: DirPath,
                  containerrun: KiveRun,
                  nowait: bool,
                  filefilter: RunFilesFilter,
                  ) -> int:

    if not nowait:
        await_containerrun(containerrun)

    return main_after_wait(
        output=output,
        containerrun=containerrun,
        filefilter=filefilter,
    )


def main_parsed(output: DirPath,
                run_id: int,
                nowait: bool,
                filefilter: RunFilesFilter,
                ) -> int:
    with login():
        containerrun = find_run(run_id)
        return main_with_run(output, containerrun, nowait, filefilter)


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    return main_parsed(output=args.output,
                       run_id=args.run_id,
                       nowait=args.nowait,
                       filefilter=args.filefilter,
                       )


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
