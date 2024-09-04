from urllib.parse import urlparse

from .url import URL
from .usererror import UserError


def url_argument(string: str) -> URL:
    try:
        parsed = urlparse(string)
    except Exception:
        raise UserError("Argument %r is not a URL.", string)

    #
    # A URL is considered valid if it has both a scheme and a netloc.
    #

    if not parsed.scheme:
        raise UserError("Argument %r is missing a scheme to be a URL.", string)

    if not parsed.netloc:
        raise UserError("Argument %r is missing a netloc to be a URL.", string)

    return URL(string)
