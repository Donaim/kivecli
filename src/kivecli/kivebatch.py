
from dataclasses import dataclass
from typing import Mapping, TextIO, Sequence, AbstractSet, Iterator, Optional
from functools import cached_property
import json

from .batchid import BatchId
from .url import URL
from .kiverun import KiveRun
from .login import login
from .logger import logger
from .escape import escape
import kiveapi


@dataclass(frozen=True)
class KiveBatch:
    # The original JSON object received from Kive.
    _original_raw: Mapping[str, object]

    # The `id` field of this run.
    id: BatchId

    # Name given to this run
    name: str

    # The URL for this run.
    url: URL

    # The URL for this run.
    groups_allowed: AbstractSet[str]

    # The batch that this KiveBatch was performed at, if any.
    runs: Sequence[KiveRun]

    @staticmethod
    def get_by_id(batch_id: int) -> "KiveBatch":
        """Get a specific batch by ID from the Kive server.
        
        Args:
            batch_id: The numeric ID of the batch
            
        Returns:
            KiveBatch object verified to exist on server
        """
        with login() as kive:
            raw = kive.endpoints.batches.get(batch_id)
            return KiveBatch.__from_json(raw)

    @staticmethod
    def find_or_create(name: str, groups_allowed: Sequence[str]) -> "KiveBatch":
        """Find an existing batch by name, or create a new one.
        
        Args:
            name: The name of the batch
            groups_allowed: List of group names allowed to access the batch
            
        Returns:
            KiveBatch object (either found or newly created)
        """
        with login() as kive:
            # Search for existing batch
            data = kive.endpoints.batches.get(params={"name": name})
            
            for raw in data.get("results", []):
                batch = KiveBatch.__from_json(raw)
                # Check if groups match
                if set(batch.groups_allowed) == set(groups_allowed):
                    logger.debug("Found existing batch named %s.", escape(name))
                    return batch
            
            # Create new batch
            description = ''
            raw = kive.endpoints.batches.post(json=dict(
                name=name,
                description=description,
                groups_allowed=groups_allowed))
            logger.debug("Created new batch named %s.", escape(name))
            return KiveBatch.__from_json(raw)

    @staticmethod
    def search(name: Optional[str] = None) -> Iterator["KiveBatch"]:
        """Search for batches matching the given criteria.
        
        Args:
            name: Filter by batch name
            
        Yields:
            KiveBatch objects that match the search criteria
        """
        with login() as kive:
            query = {}
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
                        data = kive.endpoints.batches.get(params=query)

                    for raw in data["results"]:
                        yield KiveBatch.__from_json(raw)

                    url = data.get("next")
                    if not url:
                        break
                except (KeyError, kiveapi.KiveServerException, kiveapi.KiveClientException) as err:
                    logger.error("Failed to retrieve batches: %s", err)
                    break

    @staticmethod
    def __from_json(raw: Mapping[str, object]) -> 'KiveBatch':
        """Internal method to construct KiveBatch from JSON. Do not use directly."""
        id_obj = raw['id']
        assert isinstance(id_obj, int)
        id = BatchId(id_obj)
        name = str(raw["name"])
        url = URL(str(raw["url"]))
        runs_obj = raw["runs"]
        assert isinstance(runs_obj, list)

        def parse_run(raw: object) -> KiveRun:
            assert isinstance(raw, dict)
            return KiveRun.from_json(raw)

        groups_allowed_obj = raw["groups_allowed"]
        assert isinstance(groups_allowed_obj, list)
        groups_allowed = frozenset(map(str, groups_allowed_obj))
        runs = tuple(parse_run(run) for run in runs_obj)

        return KiveBatch(_original_raw=raw,
                         id=id,
                         name=name,
                         url=url,
                         groups_allowed=groups_allowed,
                         runs=runs,
                         )

    @cached_property
    def raw(self) -> Mapping[str, object]:
        ret = {k: v for k, v in self._original_raw.items()}
        ret["id"] = self.id.value
        ret["name"] = self.name
        ret["url"] = self.url.value
        ret["groups_allowed"] = list(self.groups_allowed)
        ret["runs"] = list(run.raw for run in self.runs)
        return ret

    def dump(self, out: TextIO) -> None:
        json.dump(self.raw, out, indent='\t')
