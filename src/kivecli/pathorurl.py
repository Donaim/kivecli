
from pathlib import Path
from typing import TypeAlias, Union

from .url import URL

PathOrURL: TypeAlias = Union[Path, URL]
