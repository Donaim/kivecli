
from typing import Iterator, Dict
from dataclasses import dataclass

from .url import URL
from .login import login
from .argumenttype import ArgumentType


@dataclass(frozen=True)
class DatasetInfo:
    argument_type: ArgumentType
    argument_name: str
    url: URL

    @staticmethod
    def from_run(datasets_link: URL) -> Iterator['DatasetInfo']:
        with login() as kive:
            run_datasets = kive.get(datasets_link.value).json()
            coerced = map(DatasetInfo.coerce, run_datasets)
            yield from coerced

    @staticmethod
    def coerce(data: Dict[str, object]) -> 'DatasetInfo':
        argument_type = ArgumentType(str(data['argument_type']))
        argument_name = str(data['argument_name'])
        url = URL(str(data["dataset"]))
        return DatasetInfo(argument_type=argument_type,
                           argument_name=argument_name,
                           url=url,
                           )
