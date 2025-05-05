
from typing import Iterator

from .logger import logger
from .escape import escape
from .dataset import Dataset
from .datasetinfo import DatasetInfo
from .runfilesfilter import RunFilesFilter
from .login import login
from .kiverun import KiveRun


def collect_run_files(containerrun: KiveRun,
                      filefilter: RunFilesFilter,
                      ) -> Iterator[Dataset]:

    dataset_list = containerrun.dataset_list
    with login():
        for run_dataset in DatasetInfo.from_run(dataset_list):
            if filefilter.matches(run_dataset):
                dataset = Dataset.get(run_dataset.url)
                checksum = dataset.md5checksum
                filename = dataset.name

                logger.debug("Found dataset at %s for %s.",
                             escape(dataset.url),
                             escape(dataset.name))
                logger.debug("File %s corresponds to Kive argument name %s.",
                             escape(filename),
                             escape(run_dataset.argument_name))
                logger.debug("Argument %s has MD5 hash %s.",
                             escape(run_dataset.argument_name), checksum)

                yield dataset
