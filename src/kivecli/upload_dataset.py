#! /usr/bin/env python3

import argparse
from pathlib import Path
from typing import Optional, Sequence, Union, NoReturn

import kiveapi
from kiveapi.dataset import Dataset as KiveDataset

from .escape import escape
from .find_dataset import find_kive_dataset
from .logger import logger
from .login import login
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .pathorurl import PathOrURL
from .url import URL
from .usererror import UserError

from .dataset import Dataset as LocalDataset


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


def upload_or_retrieve_dataset(
    session: kiveapi.KiveAPI,
    name: Union[str, URL],
    inputpath: PathOrURL,
    users: Optional[Sequence[str]] = None,
    groups: Optional[Sequence[str]] = None,
) -> Optional[KiveDataset]:
    """Create a dataset by uploading a file to Kive."""

    def report_found(dataset: KiveDataset) -> None:
        url = URL(str(dataset.raw["url"]))
        printname = str(dataset.raw["name"])

        if isinstance(name, str):
            logger.debug(
                "Found existing dataset for %s at %s.", escape(name), escape(url)
            )

        elif isinstance(name, URL):
            logger.debug(
                "Found existing dataset at %s for %s.", escape(url), escape(printname)
            )

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
        dataset: KiveDataset = KiveDataset(found, session)
        report_found(dataset)
        return dataset
    elif isinstance(inputpath, URL):
        return None

    try:
        return upload_dataset_file(
            file_path=inputpath,
            name=str(name),
            description="None",
            users=users,
            groups=groups,
        )

    except kiveapi.KiveMalformedDataException as e:
        logger.warning("Upload of %s failed: %s", escape(inputpath), e)

        dataset = session.find_dataset(name=name)[0]
        if dataset is not None:
            report_found(dataset)
            return dataset

    return None


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
                raw = kive.post(
                    "/api/datasets/",
                    {
                        "name": name,
                        "description": description,
                        "users_allowed": list(users or []),
                        "groups_allowed": list(groups or []),
                        "save_in_db": "true",
                    },
                    files={"dataset_file": handle},
                ).json()

            dataset = LocalDataset._from_json(raw)
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
