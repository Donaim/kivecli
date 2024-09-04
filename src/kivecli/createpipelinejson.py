#! /usr/bin/env python

import argparse
import json
from typing import Dict, Sequence, List, Union, TextIO
import sys

from .mainwrap import mainwrap
from .parsecli import parse_cli


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a pipeline.json file for Kive.")

    parser.add_argument("--ninputs", type=int, required=True,
                        help="Number of input arguments this app supports.")
    parser.add_argument("--noutputs", type=int, required=True,
                        help="Number of output arguments this app supports.")

    return parser


def make_step_input(orig: Dict[str, str]) -> Dict[str, Union[str, int]]:
    ret: Dict[str, Union[str, int]] = {}
    ret['dataset_name'] = orig['dataset_name']
    ret['source_dataset_name'] = orig['dataset_name']
    ret['source_step'] = 0
    return ret


def print_pipeline_json(ninputs: int, noutputs: int, output: TextIO) -> None:
    inputs: List[Dict[str, str]] = [
        {
            "dataset_name": "script",
        },
    ] + [
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

    print(json.dumps(ret, indent='\t'), file=output)


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parse_cli(parser, argv)
    print_pipeline_json(ninputs=args.ninputs,
                        noutputs=args.noutputs,
                        output=sys.stdout)
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
