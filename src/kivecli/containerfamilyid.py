from dataclasses import dataclass


@dataclass(frozen=True)
class ContainerFamilyId:
    value: int

    def __str__(self) -> str:
        return str(self.value)
