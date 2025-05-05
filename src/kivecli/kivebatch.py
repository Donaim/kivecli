
from dataclasses import dataclass
from typing import Mapping, TextIO, Sequence, AbstractSet
from functools import cached_property
import json

from .batchid import BatchId
from .url import URL
from .kiverun import KiveRun


@dataclass(frozen=True)
class KiveBatch:
    # The original JSON object received from Kive.
    _original_raw: Mapping[str, object]

    # The `id` field of this run.
    id: BatchId

    # Name given to this run
    name: str

    # The URL for this run.
    url: URL

    # The URL for this run.
    groups_allowed: AbstractSet[str]

    # The batch that this KiveBatch was performed at, if any.
    runs: Sequence[KiveRun]

    @staticmethod
    def from_json(raw: Mapping[str, object]) -> 'KiveBatch':
        id_obj = raw['id']
        assert isinstance(id_obj, int)
        id = BatchId(id_obj)
        name = str(raw["name"])
        url = URL(str(raw["url"]))
        runs_obj = raw["runs"]
        assert isinstance(runs_obj, list)

        def parse_run(raw: object) -> KiveRun:
            assert isinstance(raw, dict)
            return KiveRun.from_json(raw)

        groups_allowed_obj = raw["groups_allowed"]
        assert isinstance(groups_allowed_obj, list)
        groups_allowed = frozenset(map(str, groups_allowed_obj))
        runs = tuple(parse_run(run) for run in runs_obj)

        return KiveBatch(_original_raw=raw,
                         id=id,
                         name=name,
                         url=url,
                         groups_allowed=groups_allowed,
                         runs=runs,
                         )

    @cached_property
    def raw(self) -> Mapping[str, object]:
        ret = {k: v for k, v in self._original_raw.items()}
        ret["id"] = self.id.value
        ret["name"] = self.name
        ret["url"] = self.url.value
        ret["groups_allowed"] = list(self.groups_allowed)
        ret["runs"] = list(run.raw for run in self.runs)
        return ret

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent='\t')
