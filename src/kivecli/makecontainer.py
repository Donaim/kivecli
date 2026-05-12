#! /usr/bin/env python3

import argparse
from pathlib import Path
from typing import Optional, Sequence

from .container import Container as LocalContainer
from .mainwrap import mainwrap
from .parsecli import parse_cli


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
    container = LocalContainer.create(
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
