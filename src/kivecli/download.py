#! /usr/bin/env python

import argparse
import os
from typing import Sequence, Dict, Iterable, Iterator, Mapping
import time

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


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download run outputs.")

    parser.add_argument("--run_id", type=int, required=True,
                        help="Run ID of the target Kive run.")
    parser.add_argument("--output", type=dir_path, required=True,
                        help="Output folder where results are downloaded."
                        " Not downloading by default.")
    parser.add_argument("--nowait", action='store_true', default=False,
                        help="Do not wait until the run is finished.")

    return parser


def collect_run_outputs(kive: kiveapi.KiveAPI,
                        containerrun: Dict[str, object]) -> Iterator[Dataset]:

    def matches(run_dataset: Mapping[str, object]) -> bool:
        return run_dataset.get("argument_type") == "O"

    yield from collect_run_files(kive, matches, containerrun)


def download_results(datasets: Iterable[Dataset],
                     output: DirPath) -> None:
    if output is not None:
        logger.debug("Making output directory at %s.", escape(output))
        os.makedirs(output, exist_ok=True)

    for dataset in datasets:
        dataset.download(output)


def await_containerrun(session: kiveapi.KiveAPI,
                       containerrun: Dict[str, object]) \
        -> Dict[str, object]:
    """
    Given a `KiveAPI instance and a container run, monitor the run
    for completion and return the completed run.
    """

    ACTIVE_STATES = ["N", "S", "L", "R"]
    FAIL_STATES = ["X", "F"]
    INTERVAL = 1.0
    MAX_WAIT = float("inf")

    starttime = time.time()
    elapsed = 0.0

    runid = containerrun["id"]
    logger.debug("Waiting for run %s to finish.", runid)

    last_state: str = ""
    while elapsed < MAX_WAIT:
        containerrun = session.endpoints.containerruns.get(runid)

        state_obj = containerrun["state"]
        assert isinstance(state_obj, str)
        state: str = state_obj

        elapsed = round(time.time() - starttime, 2)

        if state != last_state:
            last_state = state
            logger.debug("Run %s in state %s after %s seconds elapsed.",
                         runid, state, elapsed)

        if state in ACTIVE_STATES:
            time.sleep(INTERVAL)
            continue

        if state == "C":
            logger.debug("Run finished after %s seconds.", elapsed)
        elif state in FAIL_STATES:
            logger.warning("Run failed after %s seconds.", elapsed)
        else:
            logger.warning("Run failed catastrophically after %s seconds.",
                           elapsed)

        break
    else:
        logger.warning("Run timed out after %s seconds.", elapsed)
        return containerrun

    return containerrun


def main_after_wait(kive: kiveapi.KiveAPI,
                    output: DirPath,
                    containerrun: Dict[str, object],
                    ) -> int:

    datasets = list(collect_run_outputs(kive, containerrun))
    download_results(datasets, output)

    return 0


def main_with_run(kive: kiveapi.KiveAPI,
                  output: DirPath,
                  containerrun: Dict[str, object],
                  nowait: bool,
                  ) -> int:

    if not nowait:
        await_containerrun(kive, containerrun)

    return main_after_wait(
        kive=kive,
        output=output,
        containerrun=containerrun)


def main_parsed(kive: kiveapi.KiveAPI,
                output: DirPath,
                run_id: int,
                nowait: bool,
                ) -> int:

    containerrun = find_run(kive, run_id)
    return main_with_run(kive, output, containerrun, nowait)


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    with login() as kive:
        return main_parsed(kive=kive,
                           output=args.output,
                           run_id=args.run_id,
                           nowait=args.nowait,
                           )


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
