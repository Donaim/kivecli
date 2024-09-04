#! /usr/bin/env python3

import argparse
import sys
import os
import hashlib
from typing import cast, Sequence, BinaryIO, Dict, Iterable, Optional, NoReturn
from pathlib import Path
import time

import kiveapi
from kiveapi.dataset import Dataset

from kivecli.usererror import UserError
from .logger import logger
from .pathorurl import PathOrURL
from .dirpath import dir_path, DirPath
from .inputfileorurl import input_file_or_url
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a Kive app.")

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
    parser.add_argument("--app_id", type=int, required=True,
                        help="App id of the target pipeline.")
    parser.add_argument("inputs",
                        nargs="+",
                        type=input_file_or_url,
                        help="Path or URLs of the input files.")

    return parser


ALLOWED_GROUPS = ['Everyone']


def download_results(kive: kiveapi.KiveAPI,
                     containerrun: Dict[str, object],
                     output: Optional[Path]) -> None:
    # Retrieve outputs and save to files.

    run_datasets = kive.get(containerrun["dataset_list"]).json()
    for run_dataset in run_datasets:
        if run_dataset.get("argument_type") == "O":
            dataset = kive.get(run_dataset["dataset"]).json()
            checksum = dataset['MD5_checksum']
            name = run_dataset['argument_name']
            logger.debug("Output %r has MD5 hash %s.", name, checksum)
            filename = dataset["name"]
            logger.debug("File name %r corresponds to Kive argument name %r.",
                         filename, name)

            if output is None:
                continue

            filepath = output / filename
            logger.debug("Downloading %r to %r.", filename, str(filepath))
            with open(filepath, "wb") as outf:
                kive.download_file(outf, dataset["download_url"])


def find_name_and_permissions_match(items: Iterable[Dict[str, object]],
                                    name: Optional[str],
                                    type_name: str) \
                                -> Optional[Dict[str, object]]:
    needed_groups = set(ALLOWED_GROUPS)
    for item in items:
        groups = cast(Iterable[str], item['groups_allowed'])
        missing_groups = needed_groups - set(groups)
        if (name is None or item['name'] == name) and not missing_groups:
            return item

    return None


def create_batch(kive: kiveapi.KiveAPI, name: str) -> Dict[str, object]:
    description = ''
    old_batches = kive.endpoints.batches.filter('name', name)
    batch = find_name_and_permissions_match(old_batches, name, 'batch')

    if batch is None:
        batch = kive.endpoints.batches.post(json=dict(
            name=name,
            description=description,
            groups_allowed=ALLOWED_GROUPS))
        logger.debug("Created new batch named %r.", name)
    else:
        logger.debug("Found existing batch named %r.", name)

    return batch


def calculate_md5_hash(source_file: BinaryIO) -> str:
    chunk_size = 4096
    digest = hashlib.md5()
    for chunk in iter(lambda: source_file.read(chunk_size), b""):
        digest.update(chunk)
    return digest.hexdigest()


def find_kive_dataset(self: kiveapi.KiveAPI,
                      source_file: BinaryIO,
                      dataset_name: str) \
                      -> Optional[Dict[str, object]]:
    """
    Search for a dataset in Kive by name and checksum.

    :param source_file: open file object to read from
    :param str dataset_name: dataset name to search for
    :return: the dataset object from the Kive API wrapper, or None
    """

    checksum = calculate_md5_hash(source_file)
    datasets = self.endpoints.datasets.filter(
        # 'name', dataset_name, # check the name match.
        'md5', checksum,
        'uploaded', True)

    return find_name_and_permissions_match(
        datasets,
        name=None,
        type_name='dataset')


def find_kive_containerapp(kive: kiveapi.KiveAPI,
                           app_id: Optional[str],
                           ) \
                           -> Dict[str, object]:

    if app_id is not None:
        ret: Dict[str, object] = kive.endpoints.containerapps.get(app_id)
        return ret

    raise UserError("Value for app id must be provided.")


def upload_or_retrieve_dataset(session: kiveapi.KiveAPI,
                               name: str,
                               inputpath: PathOrURL,
                               users: Optional[Sequence[str]] = None,
                               groups: Optional[Sequence[str]] = None) \
                               -> Optional[Dataset]:
    """Create a dataset by uploading a file to Kive."""

    if users is None and groups is None:
        raise ValueError("A list of users or a list of groups is required.")

    if isinstance(inputpath, Path):
        with open(inputpath, "rb") as inputfile:
            found = find_kive_dataset(session, inputfile, name)
    elif isinstance(inputpath, str):
        found = session.get(inputpath).json()
    else:
        _x: NoReturn = inputpath
        assert _x

    if found:
        logger.debug("Found existing dataset for %r.", name)
        return Dataset(found, session)
    elif isinstance(inputpath, str):
        return None

    try:
        with open(inputpath, "rb") as inputfile:
            dataset = session.add_dataset(name, 'None',
                                          inputfile, None,
                                          users, groups)
            logger.debug("Uploaded new dataset for %r.", name)
            return dataset

    except kiveapi.KiveMalformedDataException:
        dataset = session.find_dataset(name=name)[0]
        if dataset is not None:
            logger.debug("Found existing dataset for %r.", name)
            return dataset

    return None


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
    logger.debug("Waiting for run %r to finish.", runid)

    last_state: str = ""
    while elapsed < MAX_WAIT:
        containerrun = session.endpoints.containerruns.get(runid)

        state_obj = containerrun["state"]
        assert isinstance(state_obj, str)
        state: str = state_obj

        elapsed = round(time.time() - starttime, 2)

        if state != last_state:
            last_state = state
            logger.debug("Run %r in state %s after %s seconds elapsed.",
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


def get_input_datasets(kive: kiveapi.KiveAPI,
                       inputs: Iterable[PathOrURL]) \
        -> Iterable[Dataset]:

    for arg in inputs:
        if isinstance(arg, Path):
            name = arg.name
        else:
            name = arg

        dataset = upload_or_retrieve_dataset(
            kive, name, arg, groups=ALLOWED_GROUPS)

        assert dataset is not None, "Expected a dataset."
        yield dataset


def main_logged_in(kive: kiveapi.KiveAPI,
                   output: Optional[DirPath],
                   batch: Optional[str],
                   stdout: BinaryIO,
                   stderr: BinaryIO,
                   app_id: int,
                   inputs: Sequence[PathOrURL],
                   ) -> int:

    if output is not None:
        logger.debug("Making output directory at %r.", str(output))
        os.makedirs(output, exist_ok=True)

    # Get the app from a container family.
    app = find_kive_containerapp(kive, str(app_id))
    app_link = kive.server_url + app["absolute_url"]
    app_name = app["name"]
    app_container = app["container_name"]
    logger.debug("Using app %r.", app_link)
    logger.debug("App name is %r.", app_name)
    logger.debug("App container is %r.", app_container)

    appid = app['id']
    appargs = kive.endpoints.containerapps.get(f"{appid}/argument_list")
    input_appargs = [x for x in appargs if x["type"] == "I"]
    if len(inputs) > len(input_appargs):
        raise UserError("At most %r inputs supported, but got %r.",
                        len(input_appargs), len(inputs))
    if len(inputs) < len(input_appargs):
        raise UserError("At least %r inputs supported, but got %r.",
                        len(input_appargs), len(inputs))

    for (x, y) in zip(input_appargs, inputs):
        kive_name = x["name"]
        if isinstance(y, Path):
            filename = y.name
        else:
            filename = y
        logger.debug("File name %r corresponds to Kive argument name %r.",
                     filename, kive_name)

    appargs_urls = [x["url"] for x in input_appargs]
    input_datasets = list(get_input_datasets(kive, inputs))
    scriptname = input_datasets[0].raw["name"]

    datasets_urls = [x.raw["url"] for x in input_datasets]
    dataset_list = [
        {
            "argument": x,
            "dataset": y,
        } for (x, y) in zip(appargs_urls, datasets_urls)
    ]

    for (apparg, dataset) in zip(input_appargs, input_datasets):
        name = apparg["name"]
        checksum = dataset.raw['MD5_checksum']
        logger.debug("Input %r has MD5 hash %s.", name, checksum)

    runspec = {
        "name": f"Free {scriptname}",
        "app": app["url"],
        "groups_allowed": ALLOWED_GROUPS,
        "datasets": dataset_list,
    }

    if batch is not None:
        kivebatch = create_batch(kive, batch)
        runspec["batch"] = kivebatch["url"]

    logger.debug("Starting the run.")
    containerrun = kive.endpoints.containerruns.post(json=runspec)
    url = kive.server_url + containerrun["absolute_url"]
    logger.debug("Started at %r.", url)

    containerrun = await_containerrun(kive, containerrun)
    download_results(kive, containerrun, output)

    log_list = kive.get(containerrun["log_list"]).json()
    for log in log_list:
        if log["size"] == 0:
            continue

        if log["type"] == "O":
            logger.debug("Displaying stdout now.")
            kive.download_file(stdout, log["download_url"])
            stdout.flush()
            logger.debug("Done with stdout.")

        if log["type"] == "E":
            logger.debug("Displaying stderr now.")
            kive.download_file(stderr, log["download_url"])
            stderr.flush()
            logger.debug("Done with stderr.")

    if containerrun["state"] == "C":
        return 0
    else:
        return 1


def main_parsed(output: Optional[DirPath],
                batch: Optional[str],
                stdout: BinaryIO,
                stderr: BinaryIO,
                app_id: int,
                inputs: Sequence[PathOrURL],
                ) -> int:

    kive = login()
    return main_logged_in(kive=kive,
                          output=output,
                          batch=batch,
                          stdout=stdout,
                          stderr=stderr,
                          app_id=app_id,
                          inputs=inputs,
                          )


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    inputs = args.inputs or []
    return main_parsed(output=args.output,
                       batch=args.batch,
                       stdout=args.stdout,
                       stderr=args.stderr,
                       app_id=args.app_id,
                       inputs=inputs,
                       )


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
