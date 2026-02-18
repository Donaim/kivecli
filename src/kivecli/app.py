from dataclasses import dataclass
from typing import Mapping, TextIO
import json

from .url import URL
from .login import login
from .containerappid import ContainerAppId


@dataclass(frozen=True)
class App:
    """Represents a Kive container app (application within a container)."""

    id: ContainerAppId
    raw: Mapping[str, object]
    name: str
    url: URL
    absolute_url: str
    container_name: str
    description: str

    @staticmethod
    def get(url: URL) -> "App":
        with login() as kive:
            raw = kive.get(url.value).json()
        return App._from_json(raw)

    @staticmethod
    def _from_json(raw: Mapping[str, object]) -> "App":
        id = ContainerAppId(int(str(raw["id"])))
        url = URL(str(raw["url"]))
        name = str(raw["name"])
        absolute_url = str(raw.get("absolute_url", ""))
        container_name = str(raw.get("container_name", ""))
        description = str(raw.get("description", ""))
        return App(
            id=id,
            raw=raw,
            name=name,
            url=url,
            absolute_url=absolute_url,
            container_name=container_name,
            description=description,
        )

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent="\t")
