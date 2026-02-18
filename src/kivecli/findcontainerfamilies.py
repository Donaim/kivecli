#! /usr/bin/env python3

import argparse
import sys
from typing import Iterator, Mapping, MutableMapping, Optional, Sequence

from .containerfamily import ContainerFamily
from .logger import logger
from .mainwrap import mainwrap
from .parsecli import parse_cli
from .usererror import UserError

DEFAULT_PAGESIZE = 10


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search for a Kive container family.")

    parser.add_argument("--name", help="Name of the container family contains.")
    parser.add_argument("--git", help="Git URL of the container family contains.")
    parser.add_argument(
        "--description", help="Description of the container family contains."
    )
    parser.add_argument("--user", help="Username of the creator contains.")

    parser.add_argument(
        "--page_size",
        type=int,
        default=DEFAULT_PAGESIZE,
        help="Number of results per page.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print all info for the matching container families.",
    )

    return parser


def build_search_query(
    page_size: int = DEFAULT_PAGESIZE,
    name: Optional[str] = None,
    git: Optional[str] = None,
    description: Optional[str] = None,
    user: Optional[str] = None,
) -> Mapping[str, object]:
    query: MutableMapping[str, object] = {"page_size": page_size}

    i = 0
    for key, val in [
        ("name", name),
        ("git", git),
        ("description", description),
        ("user", user),
    ]:
        if val is None:
            continue

        query[f"filters[{i}][key]"] = key
        query[f"filters[{i}][val]"] = str(val)
        i += 1

    return query


def fetch_paginated_results(query: Mapping[str, object]) -> Iterator[ContainerFamily]:
    """Search for container families using the given query parameters.

    Extracts filters from query and uses ContainerFamily.search().
    """
    # Extract filters
    name_filter = None
    git_filter = None
    description_filter = None
    user_filter = None
    page_size_obj: object = query.get("page_size", 200)
    if isinstance(page_size_obj, int):
        page_size = page_size_obj
    elif isinstance(page_size_obj, str):
        page_size = int(page_size_obj)
    else:
        raise TypeError(f"Invalid type for page_size: {type(page_size_obj)}")

    for i in range(10):  # Check up to 10 filters
        key_param = f"filters[{i}][key]"
        val_param = f"filters[{i}][val]"
        if key_param not in query:
            continue

        filter_key = query[key_param]
        filter_val = str(query[val_param])

        if filter_key == "name":
            name_filter = filter_val
        elif filter_key == "git":
            git_filter = filter_val
        elif filter_key == "description":
            description_filter = filter_val
        elif filter_key == "user":
            user_filter = filter_val

    # Use ContainerFamily.search()
    yield from ContainerFamily.search(
        name=name_filter,
        git=git_filter,
        description=description_filter,
        user=user_filter,
        page_size=page_size,
    )


def findcontainerfamilies(
    name: Optional[str] = None,
    git: Optional[str] = None,
    description: Optional[str] = None,
    user: Optional[str] = None,
    page_size: int = DEFAULT_PAGESIZE,
) -> Iterator[ContainerFamily]:
    query = build_search_query(
        name=name,
        git=git,
        description=description,
        user=user,
        page_size=page_size,
    )
    logger.debug("Built search query %r.", query)

    try:
        yield from fetch_paginated_results(query)
    except Exception as err:
        raise UserError("An error occurred while searching: %s", err)


def main_typed(
    name: Optional[str] = None,
    git: Optional[str] = None,
    description: Optional[str] = None,
    user: Optional[str] = None,
    page_size: int = DEFAULT_PAGESIZE,
    is_json: bool = False,
) -> None:

    families = findcontainerfamilies(
        name=name,
        git=git,
        description=description,
        user=user,
        page_size=page_size,
    )

    if is_json:
        sys.stdout.write("[")

    for i, family in enumerate(families):
        if is_json:
            if i > 0:
                sys.stdout.write(",")
            family.dump(sys.stdout)
            sys.stdout.flush()
        else:
            print(family.id)

    if is_json:
        sys.stdout.write("]")

    sys.stdout.flush()


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    main_typed(
        args.name, args.git, args.description, args.user, args.page_size, args.json
    )
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == "__main__":
    cli()
