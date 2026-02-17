from dataclasses import dataclass
from typing import Mapping, TextIO
import json

from .url import URL
from .login import login
from .containerfamilyid import ContainerFamilyId


@dataclass(frozen=True)
class ContainerFamily:
    id: ContainerFamilyId
    raw: Mapping[str, object]
    name: str
    url: URL
    git: str
    description: str

    @staticmethod
    def get(url: URL) -> "ContainerFamily":
        with login() as kive:
            raw = kive.get(url.value).json()
        return ContainerFamily._from_json(raw)

    @staticmethod
    def _from_json(raw: Mapping[str, object]) -> "ContainerFamily":
        id = ContainerFamilyId(int(str(raw["id"])))
        url = URL(str(raw["url"]))
        name = str(raw["name"])
        git = str(raw.get("git", ""))
        description = str(raw.get("description", ""))
        return ContainerFamily(
            id=id,
            raw=raw,
            name=name,
            url=url,
            git=git,
            description=description,
        )

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent="\t")
