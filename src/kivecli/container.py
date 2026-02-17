
from dataclasses import dataclass
from typing import Mapping, TextIO
import json

from .url import URL
from .login import login
from .containerid import ContainerId


@dataclass(frozen=True)
class Container:
    id: ContainerId
    raw: Mapping[str, object]
    tag: str
    url: URL
    family_name: str
    download_url: URL

    @staticmethod
    def get(url: URL) -> 'Container':
        with login() as kive:
            raw = kive.get(url.value).json()
        return Container._from_json(raw)

    @staticmethod
    def _from_json(raw: Mapping[str, object]) -> 'Container':
        id = ContainerId(int(str(raw['id'])))
        url = URL(str(raw['url']))
        tag = str(raw['tag'])
        family_name = str(raw.get('family_name', ''))
        download_url = URL(str(raw['download_url']))
        return Container(id=id,
                        raw=raw,
                        tag=tag,
                        url=url,
                        family_name=family_name,
                        download_url=download_url,
                        )

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent='\t')
