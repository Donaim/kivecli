
from dataclasses import dataclass
from typing import AbstractSet
import re

from .datasetinfo import DatasetInfo
from .argumenttype import ArgumentType
from .usererror import UserError


TEMPLATE = re.compile(r'[ ]*([^: ]+):[ ]*([^: ]+)[ ]*')


@dataclass(frozen=True)
class RunFilesFilter:
    argument_type: AbstractSet[ArgumentType]
    name: re.Pattern[str]

    def matches(self, info: DatasetInfo) -> bool:
        return info.argument_type in self.argument_type \
            and bool(self.name.match(info.argument_name))

    def __str__(self) -> str:
        argument_type = '|'.join(sorted(x.value for x in self.argument_type))
        name = str(self.name)
        return f"{argument_type}: {name}"

    @staticmethod
    def parse(text: str) -> 'RunFilesFilter':
        m = TEMPLATE.fullmatch(text)
        if m:
            (a, e) = m.groups()
            argument_type = {x for x in ArgumentType
                             if re.match(a, x.value)}
            name = re.compile(e)
            return RunFilesFilter(argument_type=argument_type,
                                  name=name,
                                  )
        else:
            raise UserError("Bad run filter %r. "
                            "Expected something like %s.",
                            text, str(RunFilesFilter.default()))

    @staticmethod
    def default() -> 'RunFilesFilter':
        argument_type = {ArgumentType.OUTPUT}
        name = re.compile('.*')
        return RunFilesFilter(argument_type=argument_type,
                              name=name,
                              )
