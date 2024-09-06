
from dataclasses import dataclass


@dataclass(frozen=True)
class MD5Checksum:
    value: str

    def __str__(self) -> str:
        return self.value
