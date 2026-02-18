
from typing import Iterator

from .dataset import Dataset
from .datasetinfo import DatasetInfo
from .escape import escape
from .kiverun import KiveRun
from .logger import logger
from .login import login
from .runfilesfilter import RunFilesFilter
from .usererror import UserError


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
                             escape(filename))
                logger.debug("File %s corresponds to Kive argument name %s.",
                             escape(filename),
                             escape(run_dataset.argument_name))
                logger.debug("Argument %s has MD5 hash %s.",
                             escape(run_dataset.argument_name), checksum)

                if dataset.is_purged:
                    logger.debug("File %s is purged. "
                                 "Trying to find an alternative.",
                                 escape(filename))

                    new = dataset.update()
                    if new is None:
                        raise UserError(
                            "File %s is purged "
                            "and alternative cannot be found.",
                            escape(filename))

                    logger.debug("Found alternative for file %s"
                                 " - it is file %s.",
                                 escape(filename), escape(new.name))
                    dataset = new

                yield dataset
