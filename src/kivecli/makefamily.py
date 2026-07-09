import argparse
from typing import Optional, Sequence

from .containerfamily import ContainerFamily
from .mainwrap import mainwrap
from .parsecli import parse_cli


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a container family on Kive."
    )

    parser.add_argument(
        "--name", type=str, required=True, help="Name for the container family."
    )
    parser.add_argument(
        "--description",
        type=str,
        default="",
        help="Description for the container family.",
    )
    parser.add_argument(
        "--git",
        type=str,
        default="",
        help="URL of the Git repository.",
    )
    parser.add_argument(
        "--users", type=str, nargs="*", help="List of users to grant access."
    )
    parser.add_argument(
        "--groups", type=str, nargs="*", help="List of groups to grant access."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print full container family info as JSON.",
    )

    return parser


def main_typed(
    name: str,
    description: str,
    git: str,
    users: Optional[Sequence[str]],
    groups: Optional[Sequence[str]],
    is_json: bool,
) -> None:
    """Create a container family on Kive.

    Args:
        name: Name for the container family.
        description: Description for the container family.
        git: URL of the Git repository.
        users: List of users to grant access.
        groups: List of groups to grant access.
        is_json: Whether to output full JSON info.
    """
    family = ContainerFamily.create(
        name=name,
        description=description,
        git=git,
        users=users,
        groups=groups,
    )

    if is_json:
        import sys

        family.dump(sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()
    else:
        print(family.id.value)


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)

    main_typed(
        name=args.name,
        description=args.description,
        git=args.git,
        users=args.users,
        groups=args.groups,
        is_json=args.json,
    )
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == "__main__":
    cli()
