
from pathlib import Path
import os

from .usererror import UserError


def dir_path(string: str) -> Path:
    if (not os.path.exists(string)) or os.path.isdir(string):
        return Path(string)
    else:
        raise UserError("Path %r is not a directory.", string)
