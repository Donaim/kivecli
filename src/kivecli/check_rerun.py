#! /usr/bin/env python3

import argparse
import sys
from typing import Sequence, List, NamedTuple
from dataclasses import dataclass

from .logger import logger
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .findrun import find_run
from .runfilesfilter import RunFilesFilter
from .argumenttype import ArgumentType
from .kiverun import KiveRun
from .dataset import Dataset
from .escape import escape
from .runid import RunId


@dataclass(frozen=True)
class DatasetStatus:
    """Status of a dataset for rerun."""

    dataset: Dataset
    argument_name: str
    is_available: bool
    alternative_dataset: Dataset | None


class RerunCheckResult(NamedTuple):
    """Result of checking if a rerun is possible."""

    run_id: RunId
    run_name: str
    all_available: bool
    dataset_statuses: List[DatasetStatus]

    @property
    def unavailable_count(self) -> int:
        """Count of datasets that cannot be retrieved."""
        return sum(1 for status in self.dataset_statuses
                   if not status.is_available)

    @property
    def purged_with_alternatives_count(self) -> int:
        """Count of purged datasets that have alternatives."""
        return sum(
            1
            for status in self.dataset_statuses
            if (status.dataset.is_purged and
                status.alternative_dataset is not None)
        )


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=("Check if all inputs for a rerun can be retrieved "
                     "(not purged).")
    )

    parser.add_argument(
        "--run_id", type=int, required=True,
        help="Run ID of the target Kive run."
    )
    parser.add_argument(
        "--json", action="store_true", help="Print detailed JSON output."
    )
    parser.add_argument(
        "--verbose-output",
        action="store_true",
        help="Print detailed status for each input.",
    )

    return parser


def check_dataset_availability(dataset: Dataset,
                               argument_name: str) -> DatasetStatus:
    """
    Check if a dataset can be retrieved, finding alternatives if purged.

    Args:
        dataset: The dataset to check
        argument_name: Name of the argument this dataset corresponds to

    Returns:
        DatasetStatus with availability information
    """
    if not dataset.is_purged:
        logger.debug(
            "Dataset %s (arg: %s) is available.",
            escape(dataset.name),
            escape(argument_name),
        )
        return DatasetStatus(
            dataset=dataset,
            argument_name=argument_name,
            is_available=True,
            alternative_dataset=None,
        )

    logger.debug(
        "Dataset %s (arg: %s) is purged, searching for alternative.",
        escape(dataset.name),
        escape(argument_name),
    )

    alternative = dataset.update()
    if alternative is not None:
        logger.debug(
            "Found alternative dataset %s for %s.",
            escape(alternative.name),
            escape(dataset.name),
        )
        return DatasetStatus(
            dataset=dataset,
            argument_name=argument_name,
            is_available=True,
            alternative_dataset=alternative,
        )

    logger.warning(
        "Dataset %s (arg: %s, hash: %s) is purged and no alternative found.",
        escape(dataset.name),
        escape(argument_name),
        escape(str(dataset.md5checksum)),
    )
    return DatasetStatus(
        dataset=dataset,
        argument_name=argument_name,
        is_available=False,
        alternative_dataset=None,
    )


def check_rerun_inputs(containerrun: KiveRun) -> RerunCheckResult:
    """
    Check if all input datasets for a rerun can be retrieved.

    Args:
        containerrun: The Kive run to check inputs for

    Returns:
        RerunCheckResult with detailed status information
    """
    filefilter = RunFilesFilter.make([ArgumentType.INPUT], ".*")
    dataset_statuses: List[DatasetStatus] = []

    from .datasetinfo import DatasetInfo

    dataset_list = containerrun.dataset_list

    with login():
        for run_dataset in DatasetInfo.from_run(dataset_list):
            if filefilter.matches(run_dataset):
                dataset = Dataset.get(run_dataset.url)
                status = check_dataset_availability(
                    dataset, run_dataset.argument_name)
                dataset_statuses.append(status)

    all_available = all(status.is_available for status in dataset_statuses)

    return RerunCheckResult(
        run_id=containerrun.id,
        run_name=containerrun.name,
        all_available=all_available,
        dataset_statuses=dataset_statuses,
    )


def print_result_json(result: RerunCheckResult) -> None:
    """Print check result as JSON."""
    import json

    output = {
        "run_id": result.run_id,
        "run_name": result.run_name,
        "all_available": result.all_available,
        "total_inputs": len(result.dataset_statuses),
        "unavailable_count": result.unavailable_count,
        "purged_with_alternatives": result.purged_with_alternatives_count,
        "datasets": [
            {
                "argument_name": status.argument_name,
                "dataset_id": status.dataset.id.value,
                "dataset_name": status.dataset.name,
                "is_purged": status.dataset.is_purged,
                "is_available": status.is_available,
                "alternative_dataset_id":
                    status.alternative_dataset.id.value
                    if status.alternative_dataset
                    else None,
                "alternative_dataset_name":
                    status.alternative_dataset.name
                    if status.alternative_dataset
                    else None,
            }
            for status in result.dataset_statuses
        ],
    }

    json.dump(output, sys.stdout, indent=2)
    sys.stdout.write("\n")


def print_result_verbose(result: RerunCheckResult) -> None:
    """Print detailed human-readable result."""
    print(f"Rerun check for run {result.run_id}: {result.run_name}")
    print(f"Total inputs: {len(result.dataset_statuses)}")
    status_text = (
        '✓ All inputs available' if result.all_available
        else '✗ Some inputs unavailable'
    )
    print(f"Status: {status_text}")

    if result.purged_with_alternatives_count > 0:
        print(
            f"Note: {result.purged_with_alternatives_count} purged "
            f"dataset(s) have alternatives"
        )

    if result.unavailable_count > 0:
        print(
            f"Warning: {result.unavailable_count} input(s) cannot be "
            f"retrieved"
        )

    print("\nInput details:")
    for status in result.dataset_statuses:
        avail_marker = "✓" if status.is_available else "✗"
        print(
            f"  {avail_marker} {status.argument_name}: "
            f"{status.dataset.name} (ID: {status.dataset.id})"
        )

        if status.dataset.is_purged:
            if status.alternative_dataset:
                alt = status.alternative_dataset
                print(
                    f"    → Purged, using alternative: {alt.name} "
                    f"(ID: {alt.id})"
                )
            else:
                print("    → Purged, no alternative found")


def print_result_simple(result: RerunCheckResult) -> None:
    """Print simple result - just OK or ERROR."""
    if result.all_available:
        pass
    else:
        for status in result.dataset_statuses:
            if not status.is_available:
                print(
                    f"  - {status.argument_name}: {status.dataset.name} "
                    f"(purged, no alternative)"
                )


def main_typed(run_id: int, is_json: bool, verbose_output: bool) -> int:
    """
    Main entry point for check_rerun command.

    Args:
        run_id: ID of the run to check
        is_json: Whether to output JSON format
        verbose_output: Whether to print detailed status

    Returns:
        Exit code (0 if all inputs available, 1 otherwise)
    """
    with login():
        containerrun = find_run(run_id)
        result = check_rerun_inputs(containerrun)

    if is_json:
        print_result_json(result)
    elif verbose_output:
        print_result_verbose(result)
    else:
        print_result_simple(result)

    return 0 if result.all_available else 1


def main(argv: Sequence[str]) -> int:
    """
    Parse command line arguments and execute check_rerun command.

    Args:
        argv: Command line arguments

    Returns:
        Exit code
    """
    parser = cli_parser()
    args = parse_cli(parser, argv)

    return main_typed(
        run_id=args.run_id,
        is_json=args.json,
        verbose_output=args.verbose_output,
    )


def cli() -> None:
    """CLI wrapper for check_rerun command."""
    mainwrap(main)


if __name__ == "__main__":
    cli()
