
import sys
from typing import Callable, Sequence

from .usererror import UserError
from .logger import logger


def mainwrap(main: Callable[[Sequence[str]], int]) -> None:
    try:
        rc = main(sys.argv[1:])
        logger.debug("Done (%r).", rc)
    except BrokenPipeError:
        logger.debug("Broken pipe.")
        rc = 1
    except KeyboardInterrupt:
        logger.debug("Interrupted.")
        rc = 1
    except UserError as e:
        logger.fatal(e.fmt, *e.fmt_args)
        rc = e.code

    sys.exit(rc)
