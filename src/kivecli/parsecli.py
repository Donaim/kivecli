
from argparse import ArgumentParser, Namespace
from typing import Sequence
import logging

from .logger import logger


def parse_cli(parser: ArgumentParser, args: Sequence[str]) -> Namespace:
    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument('--verbose', action='store_true',
                                 help='Increase output verbosity.')
    verbosity_group.add_argument('--no-verbose', action='store_true',
                                 help='Normal output verbosity.', default=True)
    verbosity_group.add_argument('--debug', action='store_true',
                                 help='Maximum output verbosity.')
    verbosity_group.add_argument('--quiet', action='store_true',
                                 help='Minimize output verbosity.')

    ret = parser.parse_args(args)
    if ret.quiet:
        logger.setLevel(logging.ERROR)
    elif ret.verbose:
        logger.setLevel(logging.INFO)
    elif ret.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARN)

    logger.debug("Start.")

    return ret
