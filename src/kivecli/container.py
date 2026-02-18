from dataclasses import dataclass
from typing import Mapping, TextIO, List, Iterator
import json

from .url import URL
from .login import login
from .containerid import ContainerId
from .logger import logger
from .escape import escape
from .app import App
import kiveapi


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
    def get(url: URL) -> "Container":
        with login() as kive:
            raw = kive.get(url.value).json()
        return Container._from_json(raw)

    @staticmethod
    def _from_json(raw: Mapping[str, object]) -> "Container":
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

    def fetch_apps(self) -> Iterator["App"]:
        """Fetch all apps from this container's app_list URL."""

        with login() as kive:
            try:
                logger.debug(
                    "Fetching apps from container %s (family: %s, tag: %s)",
                    self.id,
                    escape(self.family_name),
                    escape(self.tag),
                )

                apps_data = kive.get(self.app_list_url.value).json()

                for app_raw in apps_data:
                    yield App._from_json(app_raw)

            except kiveapi.KiveServerException as err:
                logger.error(
                    "Failed to retrieve apps from container %s: %s", self.id, err
                )
            except kiveapi.KiveClientException as err:
                logger.error(
                    "Failed to retrieve apps from container %s: %s", self.id, err
                )

    def get_apps_list(self) -> List["App"]:
        """Get all apps from this container as a list."""
        return list(self.fetch_apps())

    def dump(self, out: TextIO, expand_apps: bool = False) -> None:
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
