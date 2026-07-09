#! /usr/bin/env python3

import argparse
import sys
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator, Optional, Sequence, Union

import kiveapi
from kiveapi.dataset import Dataset

import kivecli.download as kivedownload

from .app import App
from .await_containerrrun import await_containerrun
from .dirpath import DirPath, dir_path
from .escape import escape
from .find_dataset import ALLOWED_GROUPS
from .inputfileorurl import input_file_or_url
from .kivebatch import KiveBatch
from .kiverun import KiveRun
from .logger import logger
from .login import login
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .pathorurl import PathOrURL
from .runfilesfilter import RunFilesFilter
from .runstate import RunState
from .url import URL
from .usererror import UserError
from .upload_dataset import upload_or_retrieve_dataset


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a Kive app.")

    parser.add_argument(
        "--output",
        type=dir_path,
        help="Output folder where results are downloaded. Not downloading by default.",
    )

    parser.add_argument(
        "--nowait",
        action="store_true",
        default=False,
        help="Do not wait until the run is finished.",
    )
    parser.add_argument(
        "--filefilter",
        type=RunFilesFilter.parse,
        default=RunFilesFilter.default(),
        help="Filter for files to be downloaded.",
    )
    parser.add_argument("--batch", help="Unique name for the batch.")
    parser.add_argument("--run_name", help="A name for the run.")
    parser.add_argument(
        "--stdout",
        default=sys.stdout.buffer,
        type=argparse.FileType("wb"),
        help="Redirected stdout to file.",
    )
    parser.add_argument(
        "--stderr",
        default=sys.stderr.buffer,
        type=argparse.FileType("wb"),
        help="Redirected stderr to file.",
    )
    parser.add_argument(
        "--app_id", type=int, required=True, help="App id of the target pipeline."
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        type=input_file_or_url,
        help="Path or URLs of the input files.",
    )

    return parser


def create_batch(name: str) -> KiveBatch:
    return KiveBatch.find_or_create(name, ALLOWED_GROUPS)


def find_kive_containerapp(app_id: Optional[str]) -> App:
    """Find and return a Kive container app by ID.

    Args:
        app_id: The ID of the container app

    Returns:
        App object with typed fields

    Raises:
        UserError: If app_id is not provided
    """
    if app_id is not None:
        return App.get_by_id(int(app_id))

    raise UserError("Value for app id must be provided.")


def get_input_datasets(inputs: Iterable[PathOrURL]) -> Iterator[Dataset]:
    with login() as kive:
        for arg in inputs:
            if isinstance(arg, Path):
                name: Union[str, URL] = arg.name
            else:
                name = arg

            dataset = upload_or_retrieve_dataset(
                kive, name, arg, users=None, groups=ALLOWED_GROUPS
            )
            if dataset is None:
                raise UserError("Could not find dataset for %s.", escape(arg))

            yield dataset


def _map_inputs_to_args(
    input_appargs: list[dict[str, object]],
    input_datasets: list[Dataset],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Map resolved datasets to app arguments.

    Args:
        input_appargs: List of input argument dicts from the server.
        input_datasets: Resolved Dataset objects for each input file.

    Returns:
        (input_appargs_mapped, dataset_list):
            input_appargs_mapped: one arg dict per dataset (for logging).
            dataset_list: payload entries for the run's "datasets" field.
    """
    multi_appargs = [x for x in input_appargs if x.get("allow_multiple")]

    if not multi_appargs:
        num_inputs = len(input_datasets)
        if num_inputs > len(input_appargs):
            raise UserError(
                "At most %s inputs supported, but got %s.",
                len(input_appargs), num_inputs,
            )
        if num_inputs < len(input_appargs):
            logger.warning(
                "At least %s inputs supported, but got %s.",
                len(input_appargs), num_inputs,
            )

        input_appargs_mapped = input_appargs[:num_inputs]
        dataset_list = [
            {"argument": arg["url"], "dataset": ds.raw["url"]}
            for arg, ds in zip(input_appargs_mapped, input_datasets)
        ]
        return input_appargs_mapped, dataset_list

    if len(multi_appargs) > 1:
        raise UserError(
            "Cannot determine which input files to assign to which "
            "argument. Multiple arguments accept multiple files."
        )

    multi_arg = multi_appargs[0]
    single_appargs = [x for x in input_appargs if not x.get("allow_multiple")]

    def _sort_key(arg: dict[str, object]) -> tuple[int, int]:
        pos = arg.get("position")
        if pos is not None:
            assert isinstance(pos, int)
            return (0, pos)
        return (1, 0)

    single_sorted = sorted(single_appargs, key=_sort_key)
    num_inputs = len(input_datasets)

    if single_sorted:
        if num_inputs < len(single_sorted):
            logger.warning(
                "At least %s non-multiple input arguments, but got %s.",
                len(single_sorted), num_inputs,
            )
            bound_single = single_sorted[:num_inputs]
            input_appargs_mapped = bound_single
            dataset_list = [
                {"argument": arg["url"], "dataset": ds.raw["url"]}
                for arg, ds in zip(bound_single, input_datasets)
            ]
            return input_appargs_mapped, dataset_list

        num_multi = num_inputs - len(single_sorted)
        multi_name = multi_arg["name"]
        assert isinstance(multi_name, str)
        if num_multi == 0:
            logger.warning(
                "Multiple input argument %s received no inputs.",
                escape(multi_name),
            )
        else:
            logger.debug(
                "Binding %s input files to multiple input argument %s.",
                num_multi, escape(multi_name),
            )

        input_appargs_mapped = list(single_sorted) + [multi_arg] * num_multi
        dataset_list = []
        for i, arg in enumerate(single_sorted):
            dataset_list.append({
                "argument": arg["url"],
                "dataset": input_datasets[i].raw["url"],
            })
        for i, ds in enumerate(input_datasets[len(single_sorted):], start=1):
            dataset_list.append({
                "argument": multi_arg["url"],
                "dataset": ds.raw["url"],
                "multi_position": i,
            })
        return input_appargs_mapped, dataset_list
    else:
        multi_name = multi_arg["name"]
        assert isinstance(multi_name, str)
        if num_inputs == 0:
            raise UserError(
                "Multiple input argument %s requires at least one input.",
                escape(multi_name),
            )

        logger.debug(
            "Binding %s input files to multiple input argument %s.",
            num_inputs, escape(multi_name),
        )

        input_appargs_mapped = [multi_arg] * num_inputs
        dataset_list = [
            {
                "argument": multi_arg["url"],
                "dataset": ds.raw["url"],
                "multi_position": i + 1,
            }
            for i, ds in enumerate(input_datasets)
        ]
        return input_appargs_mapped, dataset_list


def _build_run_datasets(
    input_appargs: list[dict[str, object]],
    inputs: Sequence[PathOrURL],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[Dataset]]:
    """Resolve datasets and build the dataset payload for a run.

    Args:
        input_appargs: List of input argument dicts from the server.
        inputs: Input file paths or URLs from the CLI.

    Returns:
        (input_appargs_mapped, dataset_list, input_datasets):
            input_appargs_mapped: one arg dict per input file (for logging).
            dataset_list: payload for the run's "datasets" field.
            input_datasets: resolved Dataset objects.
    """
    input_datasets = list(get_input_datasets(inputs))
    input_appargs_mapped, dataset_list = _map_inputs_to_args(
        input_appargs, input_datasets,
    )
    return input_appargs_mapped, dataset_list, input_datasets


def main_logged_in(
    kive: kiveapi.KiveAPI,
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
    app_link = URL(kive.server_url + app.absolute_url)
    app_name: str = app.name
    app_container: str = app.container_name
    logger.debug("Using app %s.", escape(app_link))
    logger.debug("App name is %s.", escape(app_name))
    logger.debug("App container is %s.", escape(app_container))

    appid = app.id.value
    appargs = kive.endpoints.containerapps.get(f"{appid}/argument_list")
    input_appargs = [x for x in appargs if x["type"] == "I"]

    input_appargs_mapped, dataset_list, input_datasets = _build_run_datasets(
        input_appargs, inputs
    )

    for x, y in zip(input_appargs_mapped, inputs):
        kive_name = x["name"]
        assert isinstance(kive_name, str)
        if isinstance(y, Path):
            filename: Union[str, URL] = y.name
        else:
            filename = y
        logger.debug(
            "File %s corresponds to Kive argument name %s.",
            escape(filename),
            escape(kive_name),
        )

    for apparg, dataset in zip(input_appargs_mapped, input_datasets):
        name = apparg["name"]
        assert isinstance(name, str)
        checksum = dataset.raw["MD5_checksum"]
        logger.debug("Input %s has MD5 hash %s.", escape(name), checksum)

    run_name_top: str = run_name if run_name is not None else "A kivecli run"
    runspec = {
        "name": run_name_top,
        "app": app.url.value,
        "groups_allowed": ALLOWED_GROUPS,
        "datasets": dataset_list,
    }

    if batch is not None:
        kivebatch = create_batch(batch)
        runspec["batch"] = kivebatch.url.value

    logger.debug("Starting the run.")
    containerrun = KiveRun.from_json(kive.endpoints.containerruns.post(json=runspec))
    url = URL(kive.server_url + containerrun.absolute_url)
    logger.debug("Started run named %s at %s.", escape(run_name_top), escape(url))

    if not nowait:
        containerrun = await_containerrun(containerrun)
        if output is not None:
            kivedownload.main_after_wait(
                output=output,
                containerrun=containerrun,
                filefilter=filefilter,
            )

    log_list = kive.get(containerrun.log_list.value).json()
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


def main_parsed(
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

    with login() as kive:
        return main_logged_in(
            kive=kive,
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
    return main_parsed(
        output=args.output,
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


if __name__ == "__main__":
    cli()
