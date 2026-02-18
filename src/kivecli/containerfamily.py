from dataclasses import dataclass
from typing import Mapping, MutableMapping, TextIO, Iterator, Optional
import json

from .url import URL
from .login import login
from .containerfamilyid import ContainerFamilyId
from .logger import logger
import kiveapi


@dataclass(frozen=True)
class ContainerFamily:
    id: ContainerFamilyId
    raw: Mapping[str, object]
    name: str
    url: URL
    git: str
    description: str

    @staticmethod
    def get_by_id(family_id: int) -> "ContainerFamily":
        """Get a specific container family by ID from the Kive server.

        Args:
            family_id: The numeric ID of the container family

        Returns:
            ContainerFamily object verified to exist on server
        """
        with login() as kive:
            raw = kive.endpoints.containerfamilies.get(family_id)
            return ContainerFamily.__from_json(raw)

    @staticmethod
    def search(
        name: Optional[str] = None,
        git: Optional[str] = None,
        description: Optional[str] = None,
        user: Optional[str] = None,
        page_size: int = 200,
    ) -> Iterator["ContainerFamily"]:
        """Search for container families matching the given criteria.

        Args:
            name: Filter by family name
            git: Filter by git repository
            description: Filter by description
            user: Filter by user
            page_size: Number of results per page

        Yields:
            ContainerFamily objects that match the search criteria
        """
        with login() as kive:
            query: MutableMapping[str, object] = {"page_size": page_size}

            # Build filters list
            i = 0
            for key, val in [
                ("name", name),
                ("git", git),
                ("description", description),
                ("user", user),
            ]:
                if val is not None:
                    query[f"filters[{i}][key]"] = key
                    query[f"filters[{i}][val]"] = str(val)
                    i += 1

            url = None
            while True:
                try:
                    if url:
                        response = kive.get(url)
                        response.raise_for_status()
                        data = response.json()
                    else:
                        data = kive.endpoints.containerfamilies.get(params=query)

                    for raw in data["results"]:
                        yield ContainerFamily.__from_json(raw)

                    url = data.get("next")
                    if not url:
                        break
                except (
                    KeyError,
                    kiveapi.KiveServerException,
                    kiveapi.KiveClientException,
                ) as err:
                    logger.error("Failed to retrieve container families: %s", err)
                    break

    @staticmethod
    def __from_json(raw: Mapping[str, object]) -> "ContainerFamily":
        """Internal method to construct ContainerFamily from JSON. Do not use directly."""
        id = ContainerFamilyId(int(str(raw["id"])))
        url = URL(str(raw["url"]))
        name = str(raw["name"])
        git = str(raw.get("git", ""))
        description = str(raw.get("description", ""))
        return ContainerFamily(
            id=id,
            raw=raw,
            name=name,
            url=url,
            git=git,
            description=description,
        )

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent="\t")
