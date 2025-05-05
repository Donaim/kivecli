
from dataclasses import dataclass
from typing import Optional, Mapping, TextIO, Sequence
from functools import cached_property
import json
import kiveapi

from .batchid import BatchId
from .url import URL
from .login import login
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

        runs = tuple(parse_run(run) for run in runs_obj)

        return KiveBatch(_original_raw=raw,
                         id=id,
                         name=name,
                         url=url,
                         runs=runs,
                         )

    @staticmethod
    def get(id: int) -> Optional['KiveBatch']:
        with login() as kive:
            try:
                run = kive.endpoints.containerruns.get(id)
            except kiveapi.errors.KiveServerException:
                return None
        return KiveBatch.from_json(run)

    @cached_property
    def raw(self) -> Mapping[str, object]:
        ret = {k: v for k, v in self._original_raw.items()}
        ret["id"] = self.id.value
        ret["name"] = self.name
        ret["url"] = self.url.value
        ret["runs"] = list(run.raw for run in self.runs)
        return ret

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent='\t')
