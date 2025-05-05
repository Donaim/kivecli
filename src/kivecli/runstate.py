
from enum import Enum


class RunState(Enum):
    """
    Enum for possible states of a Kive Run.

    These states are defined in Kive's current database,
    which was populated by the source code that lives at
    <https://github.com/cfe-lab/Kive/blob/f513e6c35b1cf5c0141e66d5ae6b314c2936c4a9/kive/container/migrations/0201_squashed.py>.

    """

    NEW = "N"
    LOADING = "L"
    RUNNING = "R"
    SAVING = "S"
    COMPLETE = "C"
    FAILED = "F"
    CANCELLED = "X"


ACTIVE_STATES = (RunState.NEW,
                 RunState.LOADING,
                 RunState.RUNNING,
                 RunState.SAVING,
                 )

FAIL_STATES = (RunState.FAILED,
               RunState.CANCELLED,
               )
