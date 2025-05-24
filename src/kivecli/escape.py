
from pathlib import Path
from typing import NoReturn, Union
import json

from .pathorurl import PathOrURL
from .url import URL


def escape(value: Union[PathOrURL, str]) -> str:
    if isinstance(value, Path):
        return json.dumps(str(value))
    elif isinstance(value, URL):
        return str(value)
    elif isinstance(value, str):
        return json.dumps(value)
    else:
        x: NoReturn = value
        assert x is None
