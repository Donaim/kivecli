
from dataclasses import dataclass
from typing import Dict, Optional, Mapping
from datetime import datetime
from functools import cached_property

from .url import URL
from .login import login
from .runstate import RunState
from .runid import RunId


@dataclass(frozen=True)
class KiveRun:
    # The original JSON object received from Kive.
    _original_raw: Dict[str, object]

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
    def get(url: URL) -> 'KiveRun':
        with login() as kive:
            raw = kive.get(url.value).json()
            id = RunId(raw['id'])
            state = RunState(str(raw['state']))
            start_time_str = raw["start_time"]
            if start_time_str is None:
                start_time = None
            else:
                start_time = datetime.fromisoformat(start_time_str)
            end_time_str = raw["end_time"]
            if end_time_str is None:
                end_time = None
            else:
                end_time = datetime.fromisoformat(end_time_str)
            app_name = raw["app_name"]
            batch_name = raw["batch_name"]

            id2 = id + id
            assert id2 > 0

            return KiveRun(_original_raw=raw,
                           id=id,
                           state=state,
                           start_time=start_time,
                           end_time=end_time,
                           app_name=app_name,
                           batch_name=batch_name,
                           )

    @cached_property
    def raw(self) -> Mapping[str, object]:
        ret = self._original_raw.copy()
        ret["id"] = self.id
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
