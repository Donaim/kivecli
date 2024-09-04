
import os
from contextvars import ContextVar
from contextlib import contextmanager
from typing import Iterator

import kiveapi

from .usererror import UserError
from .logger import logger

session: ContextVar[kiveapi.KiveAPI] = ContextVar("KiveSession")


@contextmanager
def login() -> Iterator[kiveapi.KiveAPI]:
    existing = session.get(None)
    if existing is not None:
        yield existing

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

    kive = kiveapi.KiveAPI(server)
    try:
        kive.login(user, password)
    except kiveapi.KiveAuthException as e:
        raise UserError("Login failed: %s", str(e))

    logger.debug("Logged in as %r on server %r.", user, server)

    return kive
