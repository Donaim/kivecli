
from .usererror import UserError
from urllib.parse import urlparse
from dataclasses import dataclass


@dataclass(frozen=True)
class URL:
    value: str

    def __post_init__(self) -> None:
        try:
            parsed = urlparse(self.value)
        except Exception:
            raise UserError("Argument %r is not a URL.", self.value)

        #
        # A URL is considered valid if it has both a scheme and a netloc.
        #

        if not parsed.scheme:
            raise UserError("Argument %r is missing a scheme to be a URL.",
                            self.value)

        if not parsed.netloc:
            raise UserError("Argument %r is missing a netloc to be a URL.",
                            self.value)

    def __str__(self) -> str:
        return '<' + self.value + '>'
