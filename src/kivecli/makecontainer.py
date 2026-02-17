#! /usr/bin/env python3

import argparse
from pathlib import Path
from typing import Sequence, Optional, Any

from .logger import logger
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .login import login
from .escape import escape
from .usererror import UserError
from .container import Container as LocalContainer

import kiveapi


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Upload a singularity container to Kive."
    )

    parser.add_argument(
        "--family", type=str, required=True, help="Name or ID of the container family."
    )
    parser.add_argument(
        "--image",
        type=Path,
        required=True,
        help="Path to the singularity container file.",
    )
    parser.add_argument(
        "--tag", type=str, required=True, help="Tag for the container (e.g., version)."
    )
    parser.add_argument(
        "--description", type=str, default="", help="Description for the container."
    )
    parser.add_argument(
        "--users", type=str, nargs="*", help="List of users to grant access."
    )
    parser.add_argument(
        "--groups", type=str, nargs="*", help="List of groups to grant access."
    )
    parser.add_argument(
        "--json", action="store_true", help="Print full container info as JSON."
    )

    return parser


def find_container_family(family_name_or_id: str) -> Any:
    """
    Find a container family by name or ID.

    Args:
        family_name_or_id: Name or ID of the container family

    Returns:
        Container family object

    Raises:
        UserError: If family is not found or multiple matches exist
    """
    with login() as kive:
        # Try to interpret as ID first
        try:
            family_id = int(family_name_or_id)
            family = kive.endpoints.containerfamilies.get(family_id)
            logger.debug(
                "Found container family by ID %s: %s", family_id, escape(family["name"])
            )
            return family
        except (ValueError, kiveapi.KiveClientException):
            pass

        # Search by name
        families = kive.endpoints.containerfamilies.filter("name", family_name_or_id)

        if not families:
            raise UserError("Container family not found: %s", escape(family_name_or_id))

        if len(families) > 1:
            raise UserError(
                "Multiple container families found with name %s. "
                "Please use the family ID instead.",
                escape(family_name_or_id),
            )

        logger.debug(
            "Found container family by name '%s': ID %s",
            escape(family_name_or_id),
            families[0]["id"],
        )
        return families[0]


def upload_container(
    image_path: Path,
    family_name_or_id: str,
    tag: str,
    description: str,
    users: Optional[Sequence[str]],
    groups: Optional[Sequence[str]],
) -> LocalContainer:
    """
    Upload a singularity container to Kive.

    Args:
        image_path: Path to the singularity container file
        family_name_or_id: Name or ID of the container family
        tag: Tag for the container (e.g., version)
        description: Description for the container
        users: List of users to grant access
        groups: List of groups to grant access

    Returns:
        Container object for the uploaded container

    Raises:
        UserError: If upload fails or file doesn't exist
    """
    if not image_path.exists():
        raise UserError("File does not exist: %s", escape(str(image_path)))

    if not image_path.is_file():
        raise UserError("Path is not a file: %s", escape(str(image_path)))

    if users is None and groups is None:
        raise UserError("Must specify at least one user or group for permissions.")

    # Find the container family
    family = find_container_family(family_name_or_id)

    with login() as kive:
        try:
            logger.debug(
                "Uploading singularity container %s to family %s with tag %s.",
                escape(str(image_path)),
                escape(family["name"]),
                escape(tag),
            )

            with open(image_path, "rb") as handle:
                # Prepare the metadata for the container
                metadata_dict = {
                    "family": family["url"],
                    "tag": tag,
                    "description": description,
                    "users_allowed": users or [],
                    "groups_allowed": groups or [],
                }

                # Upload the container using multipart/form-data
                container = kive.endpoints.containers.post(
                    data=metadata_dict, files={"file": handle}
                )

            local_container = LocalContainer._from_json(container)
            logger.info(
                "Successfully uploaded container with ID %s.", local_container.id
            )

            return local_container

        except kiveapi.KiveMalformedDataException as e:
            raise UserError("Failed to upload container: %s", str(e))
        except kiveapi.KiveServerException as e:
            raise UserError("Server error while uploading container: %s", str(e))
        except kiveapi.KiveClientException as e:
            raise UserError("Client error while uploading container: %s", str(e))


def main_typed(
    image_path: Path,
    family_name_or_id: str,
    tag: str,
    description: str,
    users: Optional[Sequence[str]],
    groups: Optional[Sequence[str]],
    is_json: bool,
) -> int:
    """
    Main entry point for makecontainer command.

    Args:
        image_path: Path to the singularity container file
        family_name_or_id: Name or ID of the container family
        tag: Tag for the container
        description: Description for the container
        users: List of users to grant access
        groups: List of groups to grant access
        is_json: Whether to output full JSON info

    Returns:
        Exit code (0 for success)
    """
    container = upload_container(
        image_path=image_path,
        family_name_or_id=family_name_or_id,
        tag=tag,
        description=description,
        users=users,
        groups=groups,
    )

    if is_json:
        import sys

        container.dump(sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()
    else:
        print(container.id)

    return 0


def main(argv: Sequence[str]) -> int:
    """
    Parse command line arguments and execute makecontainer command.

    Args:
        argv: Command line arguments

    Returns:
        Exit code
    """
    parser = cli_parser()
    args = parse_cli(parser, argv)

    return main_typed(
        image_path=args.image,
        family_name_or_id=args.family,
        tag=args.tag,
        description=args.description,
        users=args.users,
        groups=args.groups,
        is_json=args.json,
    )


def cli() -> None:
    """CLI wrapper for makecontainer command."""
    mainwrap(main)


if __name__ == "__main__":
    cli()
