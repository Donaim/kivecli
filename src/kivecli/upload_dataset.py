#! /usr/bin/env python3

import argparse
from pathlib import Path
from typing import Sequence, Optional

from .logger import logger
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .escape import escape
from .usererror import UserError
from .dataset import Dataset as LocalDataset

import kiveapi
from kiveapi.dataset import Dataset as KiveDataset


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Upload a file as a dataset to Kive.")

    parser.add_argument("--file", type=Path, required=True,
                        help="Path to the file to upload.")
    parser.add_argument("--name", type=str, required=True,
                        help="Name for the dataset in Kive.")
    parser.add_argument("--description", type=str, default='',
                        help="Description for the dataset.")
    parser.add_argument("--users", type=str, nargs='*',
                        help="List of users to grant access.")
    parser.add_argument("--groups", type=str, nargs='*',
                        help="List of groups to grant access.")
    parser.add_argument("--json", action='store_true',
                        help="Print full dataset info as JSON.")

    return parser


def upload_dataset_file(file_path: Path,
                        name: str,
                        description: str,
                        users: Optional[Sequence[str]],
                        groups: Optional[Sequence[str]],
                        ) -> LocalDataset:
    """
    Upload a file as a dataset to Kive.

    Args:
        file_path: Path to the file to upload
        name: Name for the dataset
        description: Description for the dataset
        users: List of users to grant access
        groups: List of groups to grant access

    Returns:
        Dataset object for the uploaded dataset

    Raises:
        UserError: If upload fails or file doesn't exist
    """
    if not file_path.exists():
        raise UserError("File does not exist: %s", escape(str(file_path)))

    if not file_path.is_file():
        raise UserError("Path is not a file: %s", escape(str(file_path)))

    if users is None and groups is None:
        raise UserError(
            "Must specify at least one user or group for permissions.")

    with login() as kive:
        try:
            logger.debug("Uploading file %s as dataset %s.",
                         escape(str(file_path)), escape(name))

            with open(file_path, "rb") as handle:
                kive_dataset: KiveDataset = kive.add_dataset(
                    name=name,
                    description=description,
                    handle=handle,
                    cdt=None,
                    users=users,
                    groups=groups
                )

            dataset = LocalDataset._from_json(kive_dataset.raw)
            logger.info("Successfully uploaded dataset %s with ID %s.",
                        escape(name), dataset.id)

            return dataset

        except kiveapi.KiveMalformedDataException as e:
            raise UserError("Failed to upload dataset: %s", str(e))
        except kiveapi.KiveServerException as e:
            raise UserError("Server error while uploading dataset: %s", str(e))
        except kiveapi.KiveClientException as e:
            raise UserError("Client error while uploading dataset: %s", str(e))


def main_typed(file_path: Path,
               name: str,
               description: str,
               users: Optional[Sequence[str]],
               groups: Optional[Sequence[str]],
               is_json: bool,
               ) -> int:
    """
    Main entry point for upload_dataset command.

    Args:
        file_path: Path to the file to upload
        name: Name for the dataset
        description: Description for the dataset
        users: List of users to grant access
        groups: List of groups to grant access
        is_json: Whether to output full JSON info

    Returns:
        Exit code (0 for success)
    """
    dataset = upload_dataset_file(
        file_path=file_path,
        name=name,
        description=description,
        users=users,
        groups=groups,
    )

    if is_json:
        import sys
        dataset.dump(sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()
    else:
        print(dataset.id)

    return 0


def main(argv: Sequence[str]) -> int:
    """
    Parse command line arguments and execute upload_dataset command.

    Args:
        argv: Command line arguments

    Returns:
        Exit code
    """
    parser = cli_parser()
    args = parse_cli(parser, argv)

    return main_typed(
        file_path=args.file,
        name=args.name,
        description=args.description,
        users=args.users,
        groups=args.groups,
        is_json=args.json,
    )


def cli() -> None:
    """CLI wrapper for upload_dataset command."""
    mainwrap(main)


if __name__ == '__main__':
    cli()
