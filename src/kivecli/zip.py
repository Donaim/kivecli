#! /usr/bin/env python3

import os
import zipfile
import argparse
import sys
from typing import BinaryIO, Sequence
from pathlib import Path

from .usererror import UserError
from .mainwrap import mainwrap


def zip_directory_to_stream(directory_path: str,
                            output_stream: BinaryIO) -> None:
    """
    Zips the contents of the specified directory into a provided output stream.

    Parameters:
        directory_path: The path of the directory to zip.
        output_stream:  The output stream where the
                        zip content will be written.
    """

    with zipfile.ZipFile(output_stream, 'w', zipfile.ZIP_STORED) as zip_file:
        # Walk the directory tree
        for root, dirs, files in os.walk(directory_path):
            for file_sub_path in files:
                file_path = os.path.join(root, file_sub_path)
                # Write the file to the zip file.
                # Names arcname makes sure to maintain the directory structure.
                arcname = os.path.relpath(file_path, directory_path)
                zip_file.write(file_path, arcname)


def dir_path(string: str) -> Path:
    if (not os.path.exists(string)) or os.path.isdir(string):
        return Path(string)
    else:
        raise UserError("Path %r is not a directory.", string)


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        description='Zip a directory and write to stdout.')
    parser.add_argument('directory', type=dir_path,
                        help='The path of the directory to zip.')
    args = parser.parse_args()

    zip_directory_to_stream(args.directory, sys.stdout.buffer)
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
