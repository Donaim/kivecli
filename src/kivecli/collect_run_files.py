
from typing import Mapping, Iterator, Callable

import kiveapi

from .logger import logger
from .url import URL
from .escape import escape
from .dataset import Dataset


def collect_run_files(kive: kiveapi.KiveAPI,
                      matches: Callable[[Mapping[str, object]], bool],
                      containerrun: Mapping[str, object]) \
                      -> Iterator[Dataset]:

    run_datasets = kive.get(containerrun["dataset_list"]).json()
    for run_dataset in run_datasets:
        if matches(run_dataset):
            url = URL(str(run_dataset["dataset"]))
            dataset = Dataset.get(url)
            checksum = dataset.md5checksum
            filename = dataset.name
            name = str(run_dataset['argument_name'])

            logger.debug("Found dataset at %s for %s.",
                         escape(dataset.url),
                         escape(dataset.name))
            logger.debug("File %s corresponds to Kive argument name %s.",
                         escape(filename), escape(name))
            logger.debug("Argument %s has MD5 hash %s.",
                         escape(name), checksum)

            yield dataset
