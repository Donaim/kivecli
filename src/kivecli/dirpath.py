
import os
from pathlib import Path
from typing import NewType

from .escape import escape
from .usererror import UserError

DirPath = NewType('DirPath', Path)


def dir_path(string: str) -> DirPath:
    if (not os.path.exists(string)) or os.path.isdir(string):
        return DirPath(Path(string))
    else:
        raise UserError("Path %s is not a directory.", escape(string))
