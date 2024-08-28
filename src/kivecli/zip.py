#! /usr/bin/env python3

import os
import zipfile
import argparse
import sys
import logging
from typing import BinaryIO, Sequence
from pathlib import Path


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


if __name__ == '__main__':
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
