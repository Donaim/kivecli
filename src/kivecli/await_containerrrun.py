
import time
from typing import Optional

from .logger import logger
from .login import login
from .kiverun import KiveRun
from .runstate import RunState, ACTIVE_STATES, FAIL_STATES


def await_containerrun(containerrun: KiveRun) -> KiveRun:
    """
    Given a `KiveAPI instance and a container run, monitor the run
    for completion and return the completed run.
    """

    INTERVAL = 1.0
    MAX_WAIT = float("inf")

    starttime = time.time()
    elapsed = 0.0

    runid = containerrun.id
    logger.debug("Waiting for run %s to finish.", runid)

    last_state: Optional[RunState] = None
    while elapsed < MAX_WAIT:
        with login() as kive:
            containerrun = kive.endpoints.containerruns.get(runid)

        state: RunState = containerrun.state
        elapsed = round(time.time() - starttime, 2)

        if state != last_state:
            last_state = state
            logger.debug("Run %s in state %r after %s seconds elapsed.",
                         runid, state.value, elapsed)

        if state in ACTIVE_STATES:
            time.sleep(INTERVAL)
            continue

        if state == RunState.COMPLETE:
            logger.debug("Run finished after %s seconds.", elapsed)
        elif state in FAIL_STATES:
            logger.warning("Run failed after %s seconds.", elapsed)
        else:
            logger.warning("Run failed catastrophically after %s seconds.",
                           elapsed)

        break
    else:
        logger.warning("Run timed out after %s seconds.", elapsed)
        return containerrun

    return containerrun
