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
from .urlargument import url_argument
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .escape import escape


def find_run(kive: kiveapi.KiveAPI, run_id: int) -> Dict[str, object]:
    containerrun: Dict[str, object] = kive.endpoints.containerruns.get(run_id)
    url: str = str(containerrun["url"])
    logger.debug("Found run with id %s at %s.", run_id, escape(URL(url)))
    return containerrun


def collect_run_inputs(kive: kiveapi.KiveAPI,
                       containerrun: Dict[str, object]) -> Iterator[URL]:

    run_datasets = kive.get(containerrun["dataset_list"]).json()
    for run_dataset in run_datasets:
        if run_dataset.get("argument_type") == "I":
            dataset = kive.get(run_dataset["dataset"]).json()
            checksum = dataset['MD5_checksum']
            name = str(run_dataset['argument_name'])
            logger.debug("Input %s has MD5 hash %s.", escape(name), checksum)
            filename = str(dataset["name"])
            logger.debug("File name %s corresponds to Kive argument name %s.",
                         escape(filename), escape(name))

            url_string: str = run_dataset["dataset"]
            url = url_argument(url_string)

            yield url


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rerun a Kive run.")

    parser.add_argument("--output", type=dir_path,
                        help="Output folder where results are downloaded."
                        " Not downloading by default.")

    parser.add_argument("--nowait", action='store_true', default=False,
                        help="Do not wait until the run is finished.")
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


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)

    with login() as kive:
        containerrun = find_run(kive, args.run_id)
        orig_run_name = str(containerrun["name"])
        run_name = f'Rerun {orig_run_name!r}'

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
        )


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
