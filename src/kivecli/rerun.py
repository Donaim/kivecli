#! /usr/bin/env python3

import argparse
import sys
import os
import logging
from typing import Sequence, Iterator

import kiveapi

from .usererror import UserError
from .logger import logger
from .pathorurl import PathOrURL
from .url import URL
from .dirpath import dir_path
from .inputfileorurl import input_file_or_url
from .urlargument import url_argument
from .mainwrap import mainwrap


def collect_run_inputs(kive: kiveapi.KiveAPI, run_id: int) -> Iterator[URL]:
    containerrun = kive.endpoints.containerruns.get(run_id)
    logger.debug("Got run %s.", containerrun)

    run_datasets = kive.get(containerrun["dataset_list"]).json()
    for run_dataset in run_datasets:
        if run_dataset.get("argument_type") == "I":
            dataset = kive.get(run_dataset["dataset"]).json()
            checksum = dataset['MD5_checksum']
            name = run_dataset['argument_name']
            logger.debug("Input %r has MD5 hash %s.", name, checksum)
            filename = dataset["name"]
            logger.debug("File name %r corresponds to Kive argument name %r.",
                         filename, name)

            url_string: str = run_dataset["dataset"]
            url = url_argument(url_string)

            yield url


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rerun a Kive run.")

    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument('--verbose', action='store_true',
                                 help='Increase output verbosity.')
    verbosity_group.add_argument('--no-verbose', action='store_true',
                                 help='Normal output verbosity.', default=True)
    verbosity_group.add_argument('--debug', action='store_true',
                                 help='Maximum output verbosity.')
    verbosity_group.add_argument('--quiet', action='store_true',
                                 help='Minimize output verbosity.')

    parser.add_argument("--output", type=dir_path,
                        help="Output folder where results are downloaded."
                        " Not downloading by default.")

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
    args = parser.parse_args(argv)
    if args.quiet:
        logger.setLevel(logging.ERROR)
    elif args.verbose:
        logger.setLevel(logging.INFO)
    elif args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARN)

    logger.debug("Start.")

    server = os.environ.get("MICALL_KIVE_SERVER")
    user = os.environ.get("MICALL_KIVE_USER")
    password = os.environ.get("MICALL_KIVE_PASSWORD")

    if server is None:
        raise UserError("Must set $MICALL_KIVE_SERVER environment variable.")
    if user is None:
        raise UserError("Must set $MICALL_KIVE_USER environment variable.")
    if password is None:
        raise UserError("Must set $MICALL_KIVE_PASSWORD environment variable.")

    kive = kiveapi.KiveAPI(server)
    try:
        kive.login(user, password)
    except kiveapi.KiveAuthException as e:
        raise UserError("Login failed: %s", str(e))

    logger.debug("Logged in as %r on server %r.", user, server)

    prefix: Sequence[PathOrURL] = args.prefix
    assert prefix or not prefix

    urls = list(collect_run_inputs(kive, args.app_id))
    assert urls

    logger.debug("Got datasets: %s", urls)

    return 1


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
