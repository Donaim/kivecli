
import os
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

import kiveapi

from .escape import escape
from .logger import logger
from .url import URL
from .usererror import UserError

session: ContextVar[kiveapi.KiveAPI] = ContextVar("KiveSession")


@contextmanager
def login() -> Iterator[kiveapi.KiveAPI]:
    existing = session.get(None)
    if existing is not None:
        yield existing
        return

    ctx = login_try()
    token = session.set(ctx)
    try:
        yield ctx
    finally:
        session.reset(token)


def login_try() -> kiveapi.KiveAPI:
    server = os.environ.get("MICALL_KIVE_SERVER")
    user = os.environ.get("MICALL_KIVE_USER")
    password = os.environ.get("MICALL_KIVE_PASSWORD")

    if server is None:
        raise UserError("Must set $MICALL_KIVE_SERVER environment variable.")
    if user is None:
        raise UserError("Must set $MICALL_KIVE_USER environment variable.")
    if password is None:
        raise UserError("Must set $MICALL_KIVE_PASSWORD environment variable.")

    serverurl = URL(server)
    kive = kiveapi.KiveAPI(serverurl.value)
    try:
        kive.login(user, password)
    except kiveapi.KiveAuthException as e:
        raise UserError("Login failed: %s", str(e))

    logger.debug("Logged in as %s on server %s.",
                 escape(user), escape(serverurl))

    return kive
