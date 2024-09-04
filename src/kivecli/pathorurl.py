
from typing import TypeAlias, Union
from .url import URL
from pathlib import Path


PathOrURL: TypeAlias = Union[Path, URL]
