"""
A wrapper for KiveRun.id that prevents us from doing arithmetic or
anything unorthodox on it.
"""

from typing import NamedTuple


class RunId(NamedTuple):
    value: int

    def __post_init__(self) -> None:
        if self.value < 0:
            raise TypeError("The id of the run cannot be negative.",
                            self.value)

    def __str__(self) -> str:
        return str(self.value)
