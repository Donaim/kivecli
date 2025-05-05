"""
A wrapper for KiveRun.id that prevents us from doing arithmetic or
anything unorthodox on it.
"""

from typing import NewType

RunId = NewType('RunId', int)
