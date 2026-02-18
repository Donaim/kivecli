#! /usr/bin/env python3

import argparse
import sys
from typing import Iterator, Mapping, MutableMapping, Optional, Sequence

import kiveapi

from .app import App
from .container import Container
from .escape import escape
from .logger import logger
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .usererror import UserError

DEFAULT_PAGESIZE = 10


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search for a Kive container app.")

    parser.add_argument("--name", help="Name of the container app contains.")
    parser.add_argument(
        "--container_name", help="Name or tag of the parent container contains."
    )
    parser.add_argument("--container_id", type=int, help="ID of the parent container.")

    parser.add_argument(
        "--page_size",
        type=int,
        default=DEFAULT_PAGESIZE,
        help="Number of results per page.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print all info for the matching container apps.",
    )

    return parser


def fetch_containers_by_id(container_id: int, page_size: int) -> Iterator[Container]:
    """Fetch a specific container by ID."""
    try:
        yield Container.get_by_id(container_id)
    except kiveapi.KiveClientException as err:
        logger.error("Failed to retrieve container %d: %s", container_id, err)
    except kiveapi.KiveServerException as err:
        logger.error("Failed to retrieve container %d: %s", container_id, err)


def fetch_containers_by_name(
    container_name: str, page_size: int
) -> Iterator[Container]:
    """Search for containers by family name or tag."""
    # Use Container.search() with smart filter to search both family name and tag
    logger.debug("Searching containers with name %r.", container_name)
    yield from Container.search(smart_filter=container_name, page_size=page_size)


def fetch_apps_from_container(container: Container) -> Iterator[App]:
    """Fetch all apps from a specific container."""
    # Use the Container's built-in method which uses the typed app_list_url field
    yield from container.fetch_apps()


def filter_apps_by_name(apps: Iterator[App], name: str) -> Iterator[App]:
    """Filter apps by name (case-insensitive substring match)."""
    name_lower = name.lower()
    for app in apps:
        if name_lower in app.name.lower():
            yield app


def build_search_query(
    page_size: int = DEFAULT_PAGESIZE,
    name: Optional[str] = None,
    container_name: Optional[str] = None,
) -> Mapping[str, object]:
    query: MutableMapping[str, object] = {"page_size": page_size}

    i = 0
    for key, val in [
        ("name", name),
        ("container_name", container_name),
    ]:
        if val is None:
            continue

        query[f"filters[{i}][key]"] = key
        query[f"filters[{i}][val]"] = str(val)
        i += 1

    return query


def fetch_paginated_results(query: Mapping[str, object]) -> Iterator[App]:
    """Search for apps using the given query parameters.

    Extracts name filter from query and uses App.search().
    """
    # Extract name filter if present
    name_filter = None
    for i in range(10):  # Check up to 10 filters
        key_param = f"filters[{i}][key]"
        val_param = f"filters[{i}][val]"
        if key_param in query and query[key_param] == "name":
            name_filter = str(query[val_param])
            break

    # Use App.search()
    yield from App.search(name=name_filter)


def findapps(
    name: Optional[str] = None,
    container_name: Optional[str] = None,
    container_id: Optional[int] = None,
    page_size: int = DEFAULT_PAGESIZE,
) -> Iterator[App]:
    """
    Find apps by searching for containers first, then fetching their apps.

    Args:
        name: Filter apps by name (case-insensitive substring match)
        container_name: Search containers by family name or tag
        container_id: Get apps from a specific container ID
        page_size: Number of results per page when searching

    Yields:
        App instances matching the search criteria
    """
    try:
        # If both container filters are specified, prefer container_id
        if container_id is not None:
            logger.debug("Searching for apps in container ID %d", container_id)
            containers = fetch_containers_by_id(container_id, page_size)
        elif container_name is not None:
            logger.debug(
                "Searching for apps in containers matching '%s'", escape(container_name)
            )
            containers = fetch_containers_by_name(container_name, page_size)
        else:
            # No container filter specified - fetch all apps directly
            logger.debug("Searching all apps (no container filter)")
            query = build_search_query(name=name, page_size=page_size)
            logger.debug("Built search query %r.", query)
            yield from fetch_paginated_results(query)
            return

        # Fetch apps from each container
        for container in containers:
            apps = fetch_apps_from_container(container)

            # Filter by name if specified
            if name is not None:
                apps = filter_apps_by_name(apps, name)

            yield from apps

    except Exception as err:
        raise UserError("An error occurred while searching: %s", err)


def main_typed(
    name: Optional[str] = None,
    container_name: Optional[str] = None,
    container_id: Optional[int] = None,
    page_size: int = DEFAULT_PAGESIZE,
    is_json: bool = False,
) -> None:

    apps = findapps(
        name=name,
        container_name=container_name,
        container_id=container_id,
        page_size=page_size,
    )

    if is_json:
        sys.stdout.write("[")

    for i, app in enumerate(apps):
        if is_json:
            if i > 0:
                sys.stdout.write(",")
            app.dump(sys.stdout)
            sys.stdout.flush()
        else:
            print(app.id)

    if is_json:
        sys.stdout.write("]")

    sys.stdout.flush()


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    main_typed(
        args.name, args.container_name, args.container_id, args.page_size, args.json
    )
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == "__main__":
    cli()
