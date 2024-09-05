
import argparse
from pathlib import Path

from .pathorurl import PathOrURL
from .urlargument import url_argument
from .usererror import UserError
from .escape import escape


def input_file_or_url(string: str) -> PathOrURL:
    factory = argparse.FileType('r')
    try:
        with factory(string):
            pass
        return Path(string)
    except Exception as e1:
        try:
            return url_argument(string)
        except Exception as e2:
            raise UserError("Argument %s is neither"
                            " an input file (%s) nor a URL (%s).",
                            escape(string), e1, e2)
