
from dataclasses import dataclass
from typing import Mapping, Iterator, Optional

from .url import URL
from .login import login
from .md5checksum import MD5Checksum
from .dirpath import DirPath
from .logger import logger
from .escape import escape


@dataclass(frozen=True)
class Dataset:
    raw: Mapping[str, object]
    name: str
    url: URL
    download_url: URL
    md5checksum: MD5Checksum
    is_purged: bool

    @staticmethod
    def get(url: URL) -> 'Dataset':
        with login() as kive:
            raw = kive.get(url.value).json()
        return Dataset._from_json(raw)

    @staticmethod
    def _from_json(raw: Mapping[str, object]) -> 'Dataset':
        md5checksum = MD5Checksum(str(raw['MD5_checksum']))
        url = URL(str(raw['url']))
        name = str(raw['name'])
        download_url = URL(str(raw['download_url']))
        is_purged = bool(raw['is_purged'])
        return Dataset(raw=raw,
                       name=name,
                       url=url,
                       download_url=download_url,
                       md5checksum=md5checksum,
                       is_purged=is_purged,
                       )

    def update(self) -> Optional['Dataset']:
        """
        Returns an isomorphic dataset that is not purged.
        """

        datasets = self.iterate_isomorphic()
        for dataset in datasets:
            if not dataset.is_purged:
                return dataset

        return None

    def iterate_isomorphic(self) -> Iterator['Dataset']:
        """
        Finds all datasets that have the same MD5 hash as self.
        """

        md5 = self.md5checksum

        with login() as kive:
            raw_datasets = kive.endpoints.datasets.filter('md5', md5)
            for raw in raw_datasets:
                yield Dataset._from_json(raw)

    def download(self, output: DirPath) -> None:
        with login() as kive:
            filepath = output / self.name
            logger.debug("Downloading %s to %s.",
                         escape(self.name), escape(filepath))
            with open(filepath, "wb") as outf:
                kive.download_file(outf, self.download_url)
