
from pathlib import Path
import os
from typing import NewType

from .usererror import UserError
from .escape import escape


DirPath = NewType('DirPath', Path)


def dir_path(string: str) -> DirPath:
    if (not os.path.exists(string)) or os.path.isdir(string):
        return DirPath(Path(string))
    else:
        raise UserError("Path %s is not a directory.", escape(string))
