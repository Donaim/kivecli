
from dataclasses import dataclass
from typing import Dict

from .url import URL
from .login import login
from .md5checksum import MD5Checksum
from .dirpath import DirPath
from .logger import logger
from .escape import escape


@dataclass(frozen=True)
class Dataset:
    raw: Dict[str, object]
    name: str
    url: URL
    download_url: URL
    md5checksum: MD5Checksum

    @staticmethod
    def get(url: URL) -> 'Dataset':
        with login() as kive:
            raw = kive.get(url.value).json()
            md5checksum = MD5Checksum(raw['MD5_checksum'])
            name = raw['name']
            download_url = raw['download_url']
            return Dataset(raw=raw,
                           name=name,
                           url=url,
                           download_url=download_url,
                           md5checksum=md5checksum,
                           )

    def download(self, output: DirPath) -> None:
        with login() as kive:
            filepath = output / self.name
            logger.debug("Downloading %s to %s.",
                         escape(self.name), escape(filepath))
            with open(filepath, "wb") as outf:
                kive.download_file(outf, self.download_url)
