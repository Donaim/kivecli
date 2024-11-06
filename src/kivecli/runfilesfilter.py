
from dataclasses import dataclass
import re

from .datasetinfo import DatasetInfo


@dataclass(frozen=True)
class RunFilesFilter:
    pattern: re.Pattern[str]

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
        pattern = re.compile('O: .*')
        return RunFilesFilter(pattern)
