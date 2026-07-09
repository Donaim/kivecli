import json
from dataclasses import dataclass
from typing import Iterator, Mapping, MutableMapping, Optional, Sequence, TextIO

import kiveapi

from .containerfamilyid import ContainerFamilyId
from .escape import escape
from .logger import logger
from .login import login
from .url import URL
from .usererror import UserError


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
        """Internal method to construct ContainerFamily from JSON.

        Do not use directly.
        """
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

    @staticmethod
    def create(
        name: str,
        description: str = "",
        git: str = "",
        users: Optional[Sequence[str]] = None,
        groups: Optional[Sequence[str]] = None,
    ) -> "ContainerFamily":
        """Create a new container family on the Kive server.

        Args:
            name: Name for the container family.
            description: Description for the container family.
            git: URL of the Git repository.
            users: List of users to grant access.
            groups: List of groups to grant access.

        Returns:
            ContainerFamily object for the created family.

        Raises:
            UserError: If creation fails.
        """
        with login() as kive:
            try:
                payload: dict[str, object] = {
                    "name": name,
                    "description": description,
                    "git": git,
                    "users_allowed": users or [],
                    "groups_allowed": groups or [],
                    "num_containers": 0,
                }

                logger.debug(
                    "Creating container family %s.",
                    escape(name),
                )

                raw = kive.endpoints.containerfamilies.post(json=payload)

                family = ContainerFamily.__from_json(raw)
                logger.info(
                    "Successfully created container family %s with ID %s.",
                    escape(family.name),
                    family.id.value,
                )

                return family

            except kiveapi.KiveMalformedDataException as e:
                raise UserError("Failed to create container family: %s", str(e))
            except kiveapi.KiveServerException as e:
                raise UserError(
                    "Server error while creating container family: %s", str(e)
                )
            except kiveapi.KiveClientException as e:
                raise UserError(
                    "Client error while creating container family: %s", str(e)
                )

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent="\t")
