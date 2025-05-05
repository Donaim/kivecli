#! /usr/bin/env python3

import argparse
import sys
import hashlib
from typing import cast, Sequence, BinaryIO, Mapping, Iterable, Optional, \
    NoReturn, Union, Iterator
from pathlib import Path

import kiveapi
from kiveapi.dataset import Dataset

from .usererror import UserError
from .logger import logger
from .pathorurl import PathOrURL
from .dirpath import dir_path, DirPath
from .inputfileorurl import input_file_or_url
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .url import URL
from .escape import escape
from .await_containerrrun import await_containerrun
from .runfilesfilter import RunFilesFilter
from .findbatches import findbatches
from .kiverun import KiveRun
from .runstate import RunState
import kivecli.download as kivedownload


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a Kive app.")

    parser.add_argument("--output", type=dir_path,
                        help="Output folder where results are downloaded."
                        " Not downloading by default.")

    parser.add_argument("--nowait", action='store_true', default=False,
                        help="Do not wait until the run is finished.")
    parser.add_argument("--filefilter", type=RunFilesFilter.parse,
                        default=RunFilesFilter.default(),
                        help="Filter for files to be downloaded.")
    parser.add_argument("--batch", help="Unique name for the batch.")
    parser.add_argument("--run_name", help="A name for the run.")
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


def find_name_and_permissions_match(items: Iterable[Mapping[str, object]],
                                    type_name: str) \
                                -> Optional[Mapping[str, object]]:
    needed_groups = set(ALLOWED_GROUPS)
    for item in items:
        groups = cast(Iterable[str], item['groups_allowed'])
        missing_groups = needed_groups - set(groups)
        if not missing_groups:
            return item

    return None


def create_batch(name: str) -> Mapping[str, object]:
    with login() as kive:
        description = ''
        old_batches = [batch.raw for batch in findbatches(name=name)]
        batch = find_name_and_permissions_match(old_batches, 'batch')

        if batch is None:
            batch = kive.endpoints.batches.post(json=dict(
                name=name,
                description=description,
                groups_allowed=ALLOWED_GROUPS))
            logger.debug("Created new batch named %s.", escape(name))
        else:
            logger.debug("Found existing batch named %s.", escape(name))

    return batch


def calculate_md5_hash(source_file: BinaryIO) -> str:
    chunk_size = 4096
    digest = hashlib.md5()
    for chunk in iter(lambda: source_file.read(chunk_size), b""):
        digest.update(chunk)
    return digest.hexdigest()


def find_kive_dataset(self: kiveapi.KiveAPI,
                      source_file: BinaryIO) \
                      -> Optional[Mapping[str, object]]:
    """
    Search for a dataset in Kive by name and checksum.

    :param source_file: open file object to read from
    :return: the dataset object from the Kive API wrapper, or None
    """

    checksum = calculate_md5_hash(source_file)
    datasets = self.endpoints.datasets.filter(
        'md5', checksum,
        'uploaded', True)

    return find_name_and_permissions_match(datasets, type_name='dataset')


def find_kive_containerapp(app_id: Optional[str]) -> Mapping[str, object]:
    if app_id is not None:
        with login() as kive:
            ret: Mapping[str, object] \
                = kive.endpoints.containerapps.get(app_id)
        return ret

    raise UserError("Value for app id must be provided.")


def upload_or_retrieve_dataset(session: kiveapi.KiveAPI,
                               name: Union[str, URL],
                               inputpath: PathOrURL,
                               users: Optional[Sequence[str]] = None,
                               groups: Optional[Sequence[str]] = None) \
                               -> Optional[Dataset]:
    """Create a dataset by uploading a file to Kive."""

    def report_found(dataset: Dataset) -> None:
        url = URL(str(dataset.raw['url']))
        printname = str(dataset.raw['name'])

        if isinstance(name, str):
            logger.debug("Found existing dataset for %s at %s.",
                         escape(name), escape(url))

        elif isinstance(name, URL):
            logger.debug("Found existing dataset at %s for %s.",
                         escape(url), escape(printname))

        else:
            x: NoReturn = name
            assert x

    if users is None and groups is None:
        raise ValueError("A list of users or a list of groups is required.")

    if isinstance(inputpath, Path):
        with open(inputpath, "rb") as inputfile:
            found = find_kive_dataset(session, inputfile)
    elif isinstance(inputpath, URL):
        found = session.get(inputpath.value).json()
    else:
        x: NoReturn = inputpath
        assert x

    if found:
        dataset = Dataset(found, session)
        report_found(dataset)
        return dataset
    elif isinstance(inputpath, URL):
        return None

    try:
        with open(inputpath, "rb") as inputfile:
            dataset = session.add_dataset(name=name,
                                          description='None',
                                          handle=inputfile,
                                          cdt=None,
                                          users=users,
                                          groups=groups)
            url = URL(str(dataset.raw["url"]))
            logger.debug("Uploaded new dataset for %s at %s.",
                         escape(name), escape(url))
            return dataset

    except kiveapi.KiveMalformedDataException as e:
        logger.warning("Upload of %s failed: %s", escape(inputpath), e)

        dataset = session.find_dataset(name=name)[0]
        if dataset is not None:
            report_found(dataset)
            return dataset

    return None


def get_input_datasets(inputs: Iterable[PathOrURL]) -> Iterator[Dataset]:
    with login() as kive:
        for arg in inputs:
            if isinstance(arg, Path):
                name: Union[str, URL] = arg.name
            else:
                name = arg

            dataset = upload_or_retrieve_dataset(kive,
                                                 name, arg,
                                                 users=None,
                                                 groups=ALLOWED_GROUPS)
            if dataset is None:
                raise UserError("Could not find dataset for %s.", escape(arg))

            yield dataset


def main_logged_in(kive: kiveapi.KiveAPI,
                   output: Optional[DirPath],
                   batch: Optional[str],
                   run_name: Optional[str],
                   stdout: BinaryIO,
                   stderr: BinaryIO,
                   app_id: int,
                   inputs: Sequence[PathOrURL],
                   nowait: bool,
                   filefilter: RunFilesFilter,
                   ) -> int:
    # Get the app from a container family.
    app = find_kive_containerapp(str(app_id))
    app_link = URL(kive.server_url + app["absolute_url"])
    app_name: str = str(app["name"])
    app_container: str = str(app["container_name"])
    logger.debug("Using app %s.", escape(app_link))
    logger.debug("App name is %s.", escape(app_name))
    logger.debug("App container is %s.", escape(app_container))

    appid = app['id']
    appargs = kive.endpoints.containerapps.get(f"{appid}/argument_list")
    input_appargs = [x for x in appargs if x["type"] == "I"]
    if len(inputs) > len(input_appargs):
        raise UserError("At most %s inputs supported, but got %s.",
                        len(input_appargs), len(inputs))
    if len(inputs) < len(input_appargs):
        raise UserError("At least %s inputs supported, but got %s.",
                        len(input_appargs), len(inputs))

    for (x, y) in zip(input_appargs, inputs):
        kive_name: str = x["name"]
        if isinstance(y, Path):
            filename: Union[str, URL] = y.name
        else:
            filename = y
        logger.debug("File %s corresponds to Kive argument name %s.",
                     escape(filename), escape(kive_name))

    appargs_urls = [x["url"] for x in input_appargs]
    input_datasets = list(get_input_datasets(inputs))

    datasets_urls = [x.raw["url"] for x in input_datasets]
    dataset_list = [
        {
            "argument": x,
            "dataset": y,
        } for (x, y) in zip(appargs_urls, datasets_urls)
    ]

    for (apparg, dataset) in zip(input_appargs, input_datasets):
        name: str = apparg["name"]
        checksum = dataset.raw['MD5_checksum']
        logger.debug("Input %s has MD5 hash %s.", escape(name), checksum)

    run_name_top: str = run_name if run_name is not None else 'A kivecli run'
    runspec = {
        "name": run_name_top,
        "app": app["url"],
        "groups_allowed": ALLOWED_GROUPS,
        "datasets": dataset_list,
    }

    if batch is not None:
        kivebatch = create_batch(batch)
        runspec["batch"] = kivebatch["url"]

    logger.debug("Starting the run.")
    containerrun = KiveRun.from_json(
        kive.endpoints.containerruns.post(json=runspec))
    url = URL(kive.server_url + containerrun.absolute_url.value)
    logger.debug("Started run named %s at %s.",
                 escape(run_name_top), escape(url))

    if not nowait:
        await_containerrun(containerrun)
        if output is not None:
            kivedownload.main_after_wait(
                output=output,
                containerrun=containerrun,
                filefilter=filefilter,
            )

    log_list = kive.get(containerrun.log_list).json()
    for log in log_list:
        if log["size"] == 0:
            logger.debug("Empty log of type %s.", escape(log["type"]))
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

    if nowait or containerrun.state == RunState.COMPLETE:
        return 0
    else:
        return 1


def main_parsed(output: Optional[DirPath],
                batch: Optional[str],
                run_name: Optional[str],
                stdout: BinaryIO,
                stderr: BinaryIO,
                app_id: int,
                inputs: Sequence[PathOrURL],
                nowait: bool,
                filefilter: RunFilesFilter,
                ) -> int:

    with login() as kive:
        return main_logged_in(kive=kive,
                              output=output,
                              batch=batch,
                              run_name=run_name,
                              stdout=stdout,
                              stderr=stderr,
                              app_id=app_id,
                              inputs=inputs,
                              nowait=nowait,
                              filefilter=filefilter,
                              )


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    inputs = args.inputs or []
    return main_parsed(output=args.output,
                       batch=args.batch,
                       run_name=args.run_name,
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
