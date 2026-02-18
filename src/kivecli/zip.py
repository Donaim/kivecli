#! /usr/bin/env python3

import argparse
import os
import sys
import zipfile
from typing import BinaryIO, Sequence

from .dirpath import dir_path
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


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        description='Zip a directory and write to stdout.',
        epilog='''\
This program ensures that the resulting archive \
will be accepted as a pipeline definition by Kive.
''')

    parser.add_argument('directory', type=dir_path,
                        help='The path of the directory to zip.')
    args = parser.parse_args()

    zip_directory_to_stream(args.directory, sys.stdout.buffer)
    return 0


def cli() -> None:
    mainwrap(main)


if __name__ == '__main__':
    cli()
