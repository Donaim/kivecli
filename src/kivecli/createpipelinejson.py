#! /usr/bin/env python

import argparse
import json
from typing import Dict, Sequence, List, Union
import logging


# Set up the logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class UserError(RuntimeError):
    def __init__(self, fmt: str, *fmt_args: object):
        self.fmt = fmt
        self.fmt_args = fmt_args
        self.code = 1


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a pipeline.json file for Kive.")

    parser.add_argument("--ninputs", type=int, required=True,
                        help="Number of input arguments this app supports.")

    parser.add_argument("--noutputs", type=int, required=True,
                        help="Number of output arguments this app supports.")

    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument('--verbose', action='store_true',
                                 help='Increase output verbosity.')
    verbosity_group.add_argument('--no-verbose', action='store_true',
                                 help='Normal output verbosity.', default=True)
    verbosity_group.add_argument('--debug', action='store_true',
                                 help='Maximum output verbosity.')
    verbosity_group.add_argument('--quiet', action='store_true',
                                 help='Minimize output verbosity.')

    return parser


def make_step_input(orig: Dict[str, str]) -> Dict[str, Union[str, int]]:
    ret: Dict[str, Union[str, int]] = {}
    ret['dataset_name'] = orig['dataset_name']
    ret['source_dataset_name'] = orig['dataset_name']
    ret['source_step'] = 0
    return ret


def process(ninputs: int, noutputs: int) -> int:
    inputs: List[Dict[str, str]] = [
        {
            "dataset_name": f"input{i + 1}",
        } for i in range(ninputs)
    ]

    outputs: List[Dict[str, Union[str, int]]] = [
        {
            "source_dataset_name": f"output{i + 1}",
            "dataset_name": f"output{i + 1}",
            "source_step": 1,
        } for i in range(noutputs)
    ]

    ret = {
        "inputs": inputs,
        "outputs": outputs,
        "steps": [
            {
                "driver": "main.sh",
                "inputs": [make_step_input(x) for x in inputs],
                "outputs": [x["dataset_name"] for x in outputs],
            }
        ],
        "default_config": {
            "threads": 1,
            "memory": 6000,
        },
    }

    print(json.dumps(ret, indent='\t'))

    return 0


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parser.parse_args(argv)
    if args.quiet:
        logger.setLevel(logging.ERROR)
    elif args.verbose:
        logger.setLevel(logging.INFO)
    elif args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARN)

    return process(ninputs=args.ninputs, noutputs=args.noutputs)


if __name__ == '__main__':
    import sys

    try:
        rc = main(sys.argv[1:])
        logger.debug("Done.")
    except BrokenPipeError:
        logger.debug("Broken pipe.")
        rc = 1
    except KeyboardInterrupt:
        logger.debug("Interrupted.")
        rc = 1
    except UserError as e:
        logger.fatal(e.fmt, *e.fmt_args)
        rc = e.code

    sys.exit(rc)
