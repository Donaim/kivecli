import hashlib
from typing import cast, BinaryIO, Mapping, Iterable, Optional

import kiveapi


ALLOWED_GROUPS = ['Everyone']


def find_name_and_permissions_match(items: Iterable[Mapping[str, object]],
                                    type_name: str) \
                                -> Optional[Mapping[str, object]]:
    needed_groups = set(ALLOWED_GROUPS)
    for item in items:
        groups = cast(Iterable[str], item['groups_allowed'])
        missing_groups = needed_groups - set(groups)
        if not missing_groups:
            return item

    return None


def calculate_md5_hash(source_file: BinaryIO) -> str:
    chunk_size = 4096
    digest = hashlib.md5()
    for chunk in iter(lambda: source_file.read(chunk_size), b""):
        digest.update(chunk)
    return digest.hexdigest()


def find_kive_dataset(self: kiveapi.KiveAPI,
                      source_file: BinaryIO) \
                      -> Optional[Mapping[str, object]]:
    """
    Search for a dataset in Kive by name and checksum.

    :param source_file: open file object to read from
    :return: the dataset object from the Kive API wrapper, or None
    """

    checksum = calculate_md5_hash(source_file)
    datasets = self.endpoints.datasets.filter(
        'md5', checksum,
        'uploaded', True)

    return find_name_and_permissions_match(datasets, type_name='dataset')
