import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Mapping, MutableMapping, Optional, Sequence, TextIO

import kiveapi

from .app import App
from .containerfamily import ContainerFamily
from .containerid import ContainerId
from .escape import escape
from .logger import logger
from .login import login
from .url import URL
from .usererror import UserError


def find_container_family(family_name_or_id: str) -> ContainerFamily:
    """
    Find a container family by name or ID.

    Args:
        family_name_or_id: Name or ID of the container family

    Returns:
        ContainerFamily object with typed fields

    Raises:
        UserError: If family is not found or multiple matches exist
    """
    # Try to interpret as ID first
    try:
        family_id = int(family_name_or_id)
        family = ContainerFamily.get_by_id(family_id)
        logger.debug(
            "Found container family by ID %s: %s", family_id, escape(family.name)
        )
        return family
    except (ValueError, kiveapi.KiveClientException):
        pass

    # Search by name
    families = list(ContainerFamily.search(name=family_name_or_id))

    if not families:
        raise UserError("Container family not found: %s", escape(family_name_or_id))

    if len(families) > 1:
        raise UserError(
            "Multiple container families found with name %s. "
            "Please use the family ID instead.",
            escape(family_name_or_id),
        )

    family = families[0]
    logger.debug(
        "Found container family by name '%s': ID %s",
        escape(family_name_or_id),
        family.id,
    )
    return family


@dataclass(frozen=True)
class Container:
    id: ContainerId
    raw: Mapping[str, object]
    tag: str
    url: URL
    family_name: str
    download_url: URL
    app_list_url: URL

    @staticmethod
    def get_by_id(container_id: int) -> "Container":
        """Get a specific container by ID from the Kive server.

        Args:
            container_id: The numeric ID of the container

        Returns:
            Container object verified to exist on server
        """
        with login() as kive:
            raw = kive.endpoints.containers.get(container_id)
            return Container.__from_json(raw)

    @staticmethod
    def search(
        family: Optional[int] = None,
        tag: Optional[str] = None,
        smart_filter: Optional[str] = None,
        page_size: int = 200,
    ) -> Iterator["Container"]:
        """Search for containers matching the given criteria.

        Args:
            family: Filter by container family ID
            tag: Filter by container tag
            smart_filter: Smart filter that searches across family name and tag
            page_size: Number of results per page

        Yields:
            Container objects that match the search criteria
        """
        with login() as kive:
            query: MutableMapping[str, object] = {"page_size": page_size}
            if family is not None:
                query["family"] = family
            if tag is not None:
                query["tag"] = tag
            if smart_filter is not None:
                query["filters[0][key]"] = "smart"
                query["filters[0][val]"] = smart_filter

            url = None
            while True:
                try:
                    if url:
                        response = kive.get(url)
                        response.raise_for_status()
                        data = response.json()
                    else:
                        data = kive.endpoints.containers.get(params=query)

                    for raw in data["results"]:
                        yield Container.__from_json(raw)

                    url = data.get("next")
                    if not url:
                        break
                except (
                    KeyError,
                    kiveapi.KiveServerException,
                    kiveapi.KiveClientException,
                ) as err:
                    logger.error("Failed to retrieve containers: %s", err)
                    break

    @staticmethod
    def create(
        image_path: Path,
        family_name_or_id: str,
        tag: str,
        description: str,
        users: Optional[Sequence[str]],
        groups: Optional[Sequence[str]],
    ) -> "Container":
        """
        Upload a singularity container to Kive.

        Args:
            image_path: Path to the singularity container file
            family_name_or_id: Name or ID of the container family
            tag: Tag for the container (e.g., version)
            description: Description for the container
            users: List of users to grant access
            groups: List of groups to grant access

        Returns:
            Container object for the uploaded container

        Raises:
            UserError: If upload fails or file doesn't exist
        """
        if not image_path.exists():
            raise UserError("File does not exist: %s", escape(str(image_path)))

        if not image_path.is_file():
            raise UserError("Path is not a file: %s", escape(str(image_path)))

        if users is None and groups is None:
            raise UserError("Must specify at least one user or group for permissions.")

        # Find the container family
        family = find_container_family(family_name_or_id)

        with login() as kive:
            try:
                logger.debug(
                    "Uploading singularity container %s to family %s with tag %s.",
                    escape(str(image_path)),
                    escape(family.name),
                    escape(tag),
                )

                with open(image_path, "rb") as handle:
                    # Prepare the metadata for the container
                    metadata_dict = {
                        "family": family.url.value,
                        "tag": tag,
                        "description": description,
                        "users_allowed": users or [],
                        "groups_allowed": groups or [],
                    }

                    # Upload the container using multipart/form-data
                    container = kive.endpoints.containers.post(
                        data=metadata_dict, files={"file": handle}
                    )

                local_container = Container.__from_json(container)
                logger.info(
                    "Successfully uploaded container with ID %s.",
                    local_container.id.value,
                )

                # For singularity containers, trigger app creation
                # The Kive REST API doesn't automatically create apps for
                # SIMG containers. We need to GET the content (which parses
                # the deffile) and create apps manually.
                container_id = local_container.id.value
                try:
                    logger.debug("Fetching container content to create apps...")
                    content_url = (
                        f"{kive.server_url}/api/containers/{container_id}/content/"
                    )
                    content_response = kive.get(content_url)
                    content_response.raise_for_status()
                    content_data = content_response.json()

                    applist = content_data.get("applist", [])
                    if applist:
                        logger.debug(
                            "Found %s app(s) in container deffile.",
                            len(applist),
                        )
                        for app_info in applist:
                            if "appname" not in app_info:
                                raise UserError(
                                    "App info missing 'appname' field: %s", app_info
                                )
                            if "description" not in app_info:
                                logger.debug(
                                    "App info missing 'description' field, "
                                    "defaulting to empty string."
                                )
                            if "numthreads" not in app_info:
                                raise UserError(
                                    "App info missing 'numthreads' field: %s", app_info
                                )
                            if "memory" not in app_info:
                                raise UserError(
                                    "App info missing 'memory' field: %s", app_info
                                )

                            # Create each app via the containerapps endpoint
                            app_data = {
                                "container": local_container.url.value,
                                "name": app_info.get("appname", ""),
                                "appname": app_info.get("appname", ""),
                                "description": app_info.get("description", ""),
                                "threads": app_info.get("numthreads", 1),
                                "numthreads": app_info.get("numthreads", 1),
                                "memory": app_info.get("memory", 5000),
                            }

                            # Add inputs/outputs if present
                            if "inputs" in app_info:
                                app_data["inputs"] = app_info["inputs"]
                            if "outputs" in app_info:
                                app_data["outputs"] = app_info["outputs"]

                            logger.debug(f"Creating app: {app_data['name']}")
                            kive.endpoints.containerapps.post(json=app_data)

                        logger.info(
                            "Successfully created %s app(s) for container.",
                            len(applist),
                        )
                    else:
                        logger.warning("No apps found in container deffile.")
                except kiveapi.KiveServerException as e:
                    logger.warning("Failed to create apps from container: %s", e)
                except kiveapi.KiveClientException as e:
                    logger.warning("Failed to create apps from container: %s", e)
                except Exception as e:
                    logger.warning("Unexpected error creating apps: %s", e)

                return local_container

            except kiveapi.KiveMalformedDataException as e:
                raise UserError("Failed to upload container: %s", str(e))
            except kiveapi.KiveServerException as e:
                raise UserError("Server error while uploading container: %s", str(e))
            except kiveapi.KiveClientException as e:
                raise UserError("Client error while uploading container: %s", str(e))

    @staticmethod
    def __from_json(raw: Mapping[str, object]) -> "Container":
        """Internal method to construct Container from JSON. Do not use directly."""
        id = ContainerId(int(str(raw["id"])))
        url = URL(str(raw["url"]))
        tag = str(raw["tag"])
        family_name = str(raw.get("family_name", ""))
        download_url = URL(str(raw["download_url"]))
        app_list_url = URL(str(raw["app_list"]))
        return Container(
            id=id,
            raw=raw,
            tag=tag,
            url=url,
            family_name=family_name,
            download_url=download_url,
            app_list_url=app_list_url,
        )

    def get_apps_list(self) -> Sequence["App"]:
        """Get all apps from this container as a list."""
        return tuple(App.containers(self))

    def dump(self, out: TextIO, expand_apps: bool = True) -> None:
        """Dump container as JSON, optionally expanding the app_list.

        Args:
            out: Output text stream
            expand_apps: If True, fetch and expand apps in the app_list field
        """
        if expand_apps:
            # Import here to avoid circular dependency
            from .app import App  # noqa: F401

            # Create a copy of raw data and expand app_list
            expanded = dict(self.raw)
            apps = self.get_apps_list()
            expanded["app_list"] = [app.raw for app in apps]
            json.dump(expanded, out, indent="\t")
        else:
            json.dump(self.raw, out, indent="\t")
