import json
from dataclasses import dataclass
from typing import Iterator, Mapping, MutableMapping, Optional, TextIO, TYPE_CHECKING

import kiveapi

from .containerappid import ContainerAppId
from .logger import logger
from .login import login
from .url import URL
from .escape import escape


if TYPE_CHECKING:
    from .container import Container


@dataclass(frozen=True)
class App:
    """Represents a Kive container app (application within a container)."""

    id: ContainerAppId
    raw: Mapping[str, object]
    name: str
    url: URL
    absolute_url: str
    container_name: str
    description: str

    @staticmethod
    def get_by_id(app_id: int) -> "App":
        """Get a specific app by ID from the Kive server.

        Args:
            app_id: The numeric ID of the app

        Returns:
            App object verified to exist on server
        """
        with login() as kive:
            raw = kive.endpoints.containerapps.get(app_id)
            return App.__from_json(raw)

    @staticmethod
    def containers(container: "Container") -> Iterator["App"]:
        """Get all apps within a specific container.

        Args:
            container: The Container object to get apps from

        Yields:
            App objects that are within the given container
        """

        """Fetch all apps from this container's app_list URL."""
        # Import here to avoid circular import at module level
        from .app import App

        with login() as kive:
            try:
                logger.debug(
                    "Fetching apps from container %s (family: %s, tag: %s)",
                    container.id,
                    escape(container.family_name),
                    escape(container.tag),
                )

                apps_data = kive.get(container.app_list_url.value).json()

                for app_raw in apps_data:
                    yield App.__from_json(app_raw)

            except kiveapi.KiveServerException as err:
                logger.error(
                    "Failed to retrieve apps from container %s: %s", container.id, err
                )
            except kiveapi.KiveClientException as err:
                logger.error(
                    "Failed to retrieve apps from container %s: %s", container.id, err
                )

    @staticmethod
    def search(
        container_id: Optional[int] = None,
        container_family_id: Optional[int] = None,
        name: Optional[str] = None,
    ) -> Iterator["App"]:
        """Search for apps matching the given criteria.

        Args:
            container_id: Filter by container ID
            container_family_id: Filter by container family ID
            name: Filter by app name

        Yields:
            App objects that match the search criteria
        """
        with login() as kive:
            query: MutableMapping[str, object] = {}
            if container_id is not None:
                query["container"] = container_id
            if container_family_id is not None:
                query["container_family"] = container_family_id
            if name is not None:
                query["name"] = name

            url = None
            while True:
                try:
                    if url:
                        response = kive.get(url)
                        response.raise_for_status()
                        data = response.json()
                    else:
                        data = kive.endpoints.containerapps.get(params=query)

                    for raw in data["results"]:
                        yield App.__from_json(raw)

                    url = data.get("next")
                    if not url:
                        break
                except (
                    KeyError,
                    kiveapi.KiveServerException,
                    kiveapi.KiveClientException,
                ) as err:
                    logger.error("Failed to retrieve apps: %s", err)
                    break

    @staticmethod
    def __from_json(raw: Mapping[str, object]) -> "App":
        """Internal method to construct App from JSON. Do not use directly."""
        id = ContainerAppId(int(str(raw["id"])))
        url = URL(str(raw["url"]))
        name = str(raw["name"])
        absolute_url = str(raw.get("absolute_url", ""))
        container_name = str(raw.get("container_name", ""))
        description = str(raw.get("description", ""))
        return App(
            id=id,
            raw=raw,
            name=name,
            url=url,
            absolute_url=absolute_url,
            container_name=container_name,
            description=description,
        )

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent="\t")
