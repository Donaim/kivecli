
import time
from typing import Dict

from .logger import logger
from .login import login


def await_containerrun(containerrun: Dict[str, object]) -> Dict[str, object]:
    """
    Given a `KiveAPI instance and a container run, monitor the run
    for completion and return the completed run.
    """

    ACTIVE_STATES = ["N", "S", "L", "R"]
    FAIL_STATES = ["X", "F"]
    INTERVAL = 1.0
    MAX_WAIT = float("inf")

    starttime = time.time()
    elapsed = 0.0

    runid = containerrun["id"]
    logger.debug("Waiting for run %s to finish.", runid)

    last_state: str = ""
    while elapsed < MAX_WAIT:
        with login() as kive:
            containerrun = kive.endpoints.containerruns.get(runid)

        state_obj = containerrun["state"]
        assert isinstance(state_obj, str)
        state: str = state_obj

        elapsed = round(time.time() - starttime, 2)

        if state != last_state:
            last_state = state
            logger.debug("Run %s in state %s after %s seconds elapsed.",
                         runid, state, elapsed)

        if state in ACTIVE_STATES:
            time.sleep(INTERVAL)
            continue

        if state == "C":
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
