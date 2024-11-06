
from dataclasses import dataclass
import re
from typing import Iterable

from .datasetinfo import DatasetInfo
from .argumenttype import ArgumentType


@dataclass(frozen=True)
class RunFilesFilter:
    pattern: re.Pattern[str]

    @staticmethod
    def make(types: Iterable[ArgumentType], name_pattern: str) \
            -> 'RunFilesFilter':
        type_prefix = '|'.join(f'({x.value})' for x in types)
        pattern = re.compile(type_prefix + name_pattern)
        return RunFilesFilter(pattern)

    def matches(self, info: DatasetInfo) -> bool:
        stringified = f"{info.argument_type.value}: {info.argument_name}"
        return bool(self.pattern.match(stringified))

    def __str__(self) -> str:
        return self.pattern.pattern

    @staticmethod
    def parse(text: str) -> 'RunFilesFilter':
        return RunFilesFilter(re.compile(text))

    @staticmethod
    def default() -> 'RunFilesFilter':
        return RunFilesFilter.make([ArgumentType.OUTPUT], '.*')
