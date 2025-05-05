
from dataclasses import dataclass
from typing import Optional, Mapping, TextIO
from datetime import datetime
from functools import cached_property
import json

from .runstate import RunState
from .runid import RunId


@dataclass(frozen=True)
class KiveRun:
    # The original JSON object received from Kive.
    _original_raw: Mapping[str, object]

    # The `id` field of this run.
    id: RunId

    # The `state` field of this run.
    state: RunState

    # The `start_time` is None if the run has not yet started.
    # Ex. if in state "LOADING".
    start_time: Optional[datetime]

    # The `end_time` is None if the run has not yet started or finished.
    # Ex. if in state "RUNNING".
    end_time: Optional[datetime]

    # The container app that this KiveRun is performed by.
    app_name: str

    # The batch that this KiveRun was performed at, if any.
    batch_name: Optional[str]

    @staticmethod
    def from_json(raw: Mapping[str, object]) -> 'KiveRun':
        id_obj = raw['id']
        assert isinstance(id_obj, int)
        id = RunId(id_obj)
        state = RunState(str(raw['state']))
        start_time_obj = raw["start_time"]
        if start_time_obj is None:
            start_time = None
        else:
            assert isinstance(start_time_obj, str)
            start_time = datetime.fromisoformat(start_time_obj)
            end_time_obj = raw["end_time"]
        if end_time_obj is None:
            end_time = None
        else:
            assert isinstance(end_time_obj, str)
            end_time = datetime.fromisoformat(end_time_obj)
            app_name = raw["app_name"]
            batch_name = raw["batch_name"]

        return KiveRun(_original_raw=raw,
                       id=id,
                       state=state,
                       start_time=start_time,
                       end_time=end_time,
                       app_name=str(app_name),
                       batch_name=str(batch_name),
                       )

    @cached_property
    def raw(self) -> Mapping[str, object]:
        ret = {k: v for k, v in self._original_raw.items()}
        ret["id"] = self.id.value
        ret["state"] = self.state.value
        if self.start_time is None:
            ret["start_time"] = None
        else:
            ret["start_time"] = self.start_time.isoformat()
        if self.end_time is None:
            ret["end_time"] = None
        else:
            ret["end_time"] = self.end_time.isoformat()
        ret["app_name"] = self.app_name
        ret["batch_name"] = self.batch_name
        return ret

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent='\t')
