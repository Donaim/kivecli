
import json
from dataclasses import dataclass
from typing import Mapping, TextIO

from .containerappid import ContainerAppId
from .login import login
from .url import URL


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
