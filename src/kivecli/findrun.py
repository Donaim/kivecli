
from typing import Dict
import kiveapi

from .escape import escape
from .logger import logger
from .url import URL
from .usererror import UserError


def find_run(kive: kiveapi.KiveAPI, run_id: int) -> Dict[str, object]:
    try:
        containerrun: Dict[str, object] \
            = kive.endpoints.containerruns.get(run_id)
    except kiveapi.errors.KiveServerException as ex:
        raise UserError("Run with id %s not found: %s", run_id, ex) from ex

    url: str = str(containerrun["url"])
    name: str = str(containerrun["name"])
    logger.debug("Found run with id %s and name %s at %s.",
                 run_id, escape(name), escape(URL(url)))
    return containerrun
