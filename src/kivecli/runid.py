"""
A wrapper for KiveRun.id that prevents us from doing arithmetic or
anything unorthodox on it.
"""

from typing import NamedTuple


class RunId(NamedTuple):
    value: int
