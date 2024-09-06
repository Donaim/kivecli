
from typing import Dict
import kiveapi

from .escape import escape
from .logger import logger
from .url import URL


def find_run(kive: kiveapi.KiveAPI, run_id: int) -> Dict[str, object]:
    containerrun: Dict[str, object] = kive.endpoints.containerruns.get(run_id)
    url: str = str(containerrun["url"])
    name: str = str(containerrun["name"])
    logger.debug("Found run with id %s and name %s at %s.",
                 run_id, escape(name), escape(URL(url)))
    return containerrun
