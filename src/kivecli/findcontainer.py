#! /usr/bin/env python3

import argparse
import sys
from typing import Iterator, Optional, Sequence

import kiveapi

from .container import Container
from .containerfamily import ContainerFamily
from .escape import escape
from .logger import logger
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .usererror import UserError

DEFAULT_PAGESIZE = 10


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search for Kive containers.")

    parser.add_argument("--id", type=int, help="ID of a specific container.")
    parser.add_argument("--family_id", type=int, help="ID of the container family.")
    parser.add_argument("--family_name", help="Name of the container family.")
    parser.add_argument("--tag", help="Tag of the container.")
    parser.add_argument(
        "--smart_filter",
        help="Smart filter that searches across family name and tag.",
    )

    parser.add_argument(
        "--page_size",
        type=int,
        default=DEFAULT_PAGESIZE,
        help="Number of results per page.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print all info for the matching containers.",
    )

    return parser


def findcontainer(
    container_id: Optional[int] = None,
    family_id: Optional[int] = None,
    family_name: Optional[str] = None,
    tag: Optional[str] = None,
    smart_filter: Optional[str] = None,
    page_size: int = DEFAULT_PAGESIZE,
) -> Iterator[Container]:
    """Search for containers matching the given criteria.

    Args:
        container_id: Get a specific container by ID
        family_id: Filter by container family ID
        family_name: Filter by container family name (resolved to ID)
        tag: Filter by container tag
        smart_filter: Smart filter across family name and tag
        page_size: Number of results per page

    Yields:
        Container objects that match the search criteria

    Raises:
        UserError: If an error occurs during search
    """
    try:
        # If a specific container ID is requested, fetch it directly
        if container_id is not None:
            yield Container.get_by_id(container_id)
            return

        # Resolve family_name to family_id if needed
        resolved_family_id = family_id
        if family_name is not None:
            families = list(ContainerFamily.search(name=family_name))
            if not families:
                raise UserError("Container family not found: %s", escape(family_name))
            if len(families) > 1:
                raise UserError(
                    "Multiple container families found with name %s. "
                    "Please use --family_id instead.",
                    escape(family_name),
                )
            resolved_family_id = families[0].id.value

        # Perform search
        yield from Container.search(
            family=resolved_family_id,
            tag=tag,
            smart_filter=smart_filter,
            page_size=page_size,
        )
    except kiveapi.KiveClientException as err:
        raise UserError("Failed to retrieve containers: %s", err)
    except kiveapi.KiveServerException as err:
        raise UserError("Server error while retrieving containers: %s", err)


def main_typed(
    container_id: Optional[int] = None,
    family_id: Optional[int] = None,
    family_name: Optional[str] = None,
    tag: Optional[str] = None,
    smart_filter: Optional[str] = None,
    page_size: int = DEFAULT_PAGESIZE,
    json: bool = False,
) -> None:
    """Main function for findcontainer command.

    Args:
        container_id: Get a specific container by ID
        family_id: Filter by container family ID
        family_name: Filter by container family name
        tag: Filter by container tag
        smart_filter: Smart filter across family name and tag
        page_size: Number of results per page
        json: If True, print full JSON output

    Raises:
        UserError: If search fails or no results found
    """
    count = 0
    for container in findcontainer(
        container_id=container_id,
        family_id=family_id,
        family_name=family_name,
        tag=tag,
        smart_filter=smart_filter,
        page_size=page_size,
    ):
        count += 1
        if json:
            container.dump(sys.stdout, expand_apps=False)
        else:
            print(
                f"Container {container.id.value}: "
                f"{escape(container.family_name)} / {escape(container.tag)}"
            )

    if count == 0:
        raise UserError("No containers found matching the criteria.")

    logger.info("Found %d container(s).", count)


def main(argv: Sequence[str]) -> int:
    """CLI entry point for findcontainer command."""
    parser = cli_parser()
    args = parse_cli(parser, argv)
    main_typed(
        container_id=args.id,
        family_id=args.family_id,
        family_name=args.family_name,
        tag=args.tag,
        smart_filter=args.smart_filter,
        page_size=args.page_size,
        json=args.json,
    )
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == "__main__":
    cli()
