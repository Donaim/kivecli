
from dataclasses import dataclass
from typing import Mapping, TextIO
import json

from .url import URL
from .login import login
from .containerappid import ContainerAppId


@dataclass(frozen=True)
class ContainerApp:
    id: ContainerAppId
    raw: Mapping[str, object]
    name: str
    url: URL

    @staticmethod
    def get(url: URL) -> 'ContainerApp':
        with login() as kive:
            raw = kive.get(url.value).json()
        return ContainerApp._from_json(raw)

    @staticmethod
    def _from_json(raw: Mapping[str, object]) -> 'ContainerApp':
        id = ContainerAppId(int(str(raw['id'])))
        url = URL(str(raw['url']))
        name = str(raw['name'])
        return ContainerApp(id=id,
                            raw=raw,
                            name=name,
                            url=url,
                            )

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent='\t')
