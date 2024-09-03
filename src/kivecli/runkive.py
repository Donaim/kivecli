#! /usr/bin/env python3

import argparse
import sys
import os
import logging
import hashlib
from typing import cast, Sequence, BinaryIO, Dict, Iterable, Optional
from pathlib import Path
import time

import kiveapi
from kiveapi.dataset import Dataset


# Set up the logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class UserError(RuntimeError):
    def __init__(self, fmt: str, *fmt_args: object):
        self.fmt = fmt
        self.fmt_args = fmt_args
        self.code = 1


def dir_path(string: str) -> Path:
    if (not os.path.exists(string)) or os.path.isdir(string):
        return Path(string)
    else:
        raise UserError("Path %r is not a directory.", string)


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a script on Kive.")

    parser.add_argument("--output", type=dir_path,
                        help="Output folder where results are downloaded."
                        " Not downloading by default.")

    parser.add_argument("--batch", help="Unique name for the batch.")
    parser.add_argument("--stdout",
                        default=sys.stdout.buffer,
                        type=argparse.FileType('wb'),
                        help="Redirected stdout to file.")
    parser.add_argument("--stderr",
                        default=sys.stderr.buffer,
                        type=argparse.FileType('wb'),
                        help="Redirected stderr to file.")
    parser.add_argument("--app_id", type=int, required=True,
                        help="App id of the target pipeline.")

    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument('--verbose', action='store_true',
                                 help='Increase output verbosity.')
    verbosity_group.add_argument('--no-verbose', action='store_true',
                                 help='Normal output verbosity.', default=True)
    verbosity_group.add_argument('--debug', action='store_true',
                                 help='Maximum output verbosity.')
    verbosity_group.add_argument('--quiet', action='store_true',
                                 help='Minimize output verbosity.')

    parser.add_argument("script", type=argparse.FileType('r'),
                        help="Path to the script to be run.")

    parser.add_argument("inputs",
                        nargs="*",
                        type=argparse.FileType('r'),
                        help="Path to the input files"
                        " which are passed as arguments to script.")

    return parser


ALLOWED_GROUPS = ['Everyone']


def download_results(kive: kiveapi.KiveAPI,
                     containerrun: Dict[str, object],
                     output: Path) -> None:
    # Retrieve outputs and save to files.

    run_datasets = kive.get(containerrun["dataset_list"]).json()
    for run_dataset in run_datasets:
        if run_dataset.get("argument_type") == "O":
            dataset = kive.get(run_dataset["dataset"]).json()
            filename = dataset["name"]
            filepath = output / filename
            logger.debug("Downloading %r to %r.", filename, str(filepath))
            with open(filepath, "wb") as outf:
                kive.download_file(outf, dataset["download_url"])


def find_name_and_permissions_match(items: Iterable[Dict[str, object]],
                                    name: Optional[str],
                                    type_name: str) \
                                -> Optional[Dict[str, object]]:
    needed_groups = set(ALLOWED_GROUPS)
    for item in items:
        groups = cast(Iterable[str], item['groups_allowed'])
        missing_groups = needed_groups - set(groups)
        if (name is None or item['name'] == name) and not missing_groups:
            return item

    return None


def create_batch(kive: kiveapi.KiveAPI, name: str) -> Dict[str, object]:
    description = ''
    old_batches = kive.endpoints.batches.filter('name', name)
    batch = find_name_and_permissions_match(old_batches, name, 'batch')

    if batch is None:
        batch = kive.endpoints.batches.post(json=dict(
            name=name,
            description=description,
            groups_allowed=ALLOWED_GROUPS))
        logger.debug("Created new batch named %r.", name)
    else:
        logger.debug("Found existing batch named %r.", name)

    return batch


def calculate_md5_hash(source_file: BinaryIO) -> str:
    chunk_size = 4096
    digest = hashlib.md5()
    for chunk in iter(lambda: source_file.read(chunk_size), b""):
        digest.update(chunk)
    return digest.hexdigest()


def find_kive_dataset(self: kiveapi.KiveAPI,
                      source_file: BinaryIO,
                      dataset_name: str) \
                      -> Optional[Dict[str, object]]:
    """
    Search for a dataset in Kive by name and checksum.

    :param source_file: open file object to read from
    :param str dataset_name: dataset name to search for
    :return: the dataset object from the Kive API wrapper, or None
    """

    checksum = calculate_md5_hash(source_file)
    datasets = self.endpoints.datasets.filter(
        # 'name', dataset_name, # check the name match.
        'md5', checksum,
        'uploaded', True)

    return find_name_and_permissions_match(
        datasets,
        name=None,
        type_name='dataset')


def find_kive_containerapp(kive: kiveapi.KiveAPI,
                           app_id: Optional[str],
                           ) \
                           -> Dict[str, object]:

    if app_id is not None:
        ret: Dict[str, object] = kive.endpoints.containerapps.get(app_id)
        return ret

    raise UserError("Value for app id must be provided.")


def upload_or_retrieve_dataset(session: kiveapi.KiveAPI,
                               name: str,
                               inputpath: Path,
                               users: Optional[Sequence[str]] = None,
                               groups: Optional[Sequence[str]] = None) \
                               -> Optional[Dataset]:
    """Create a dataset by uploading a file to Kive."""

    if users is None and groups is None:
        raise ValueError("A list of users or a list of groups is required.")

    with open(inputpath, "rb") as inputfile:
        found = find_kive_dataset(session, inputfile, name)
        if found:
            logger.debug("Found existing dataset for %r.", name)
            return Dataset(found, session)

    try:
        with open(inputpath, "rb") as inputfile:
            dataset = session.add_dataset(name, 'None',
                                          inputfile, None,
                                          users, groups)
            logger.debug("Uploaded new dataset for %r.", name)
            return dataset

    except kiveapi.KiveMalformedDataException:
        dataset = session.find_dataset(name=name)[0]
        if dataset is not None:
            logger.debug("Found existing dataset for %r.", name)
            return dataset

    return None


def await_containerrun(session: kiveapi.KiveAPI,
                       containerrun: Dict[str, object]) \
        -> Dict[str, object]:
    """
    Given a `KiveAPI instance and a container run, monitor the run
    for completion and return the completed run.
    """

    ACTIVE_STATES = ["N", "S", "L", "R"]
    FAIL_STATES = ["X", "F"]
    INTERVAL = 1.0
    MAX_WAIT = float("inf")

    starttime = time.time()
    elapsed = 0.0

    runid = containerrun["id"]
    logger.debug("Waiting for run %r to finish.", runid)

    last_state: str = ""
    while elapsed < MAX_WAIT:
        containerrun = session.endpoints.containerruns.get(runid)

        state_obj = containerrun["state"]
        assert isinstance(state_obj, str)
        state: str = state_obj

        elapsed = round(time.time() - starttime, 2)

        if state != last_state:
            last_state = state
            logger.debug("Run %r in state %s after %s seconds elapsed.",
                         runid, state, elapsed)

        if state in ACTIVE_STATES:
            time.sleep(INTERVAL)
        elif state == "C":
            logger.debug("Run finished after %s seconds.", elapsed)
            break
        elif state in FAIL_STATES:
            logger.warning("Run failed after %s seconds.", elapsed)
        else:
            logger.warning("Run failed catastrophically after %s seconds.",
                           elapsed)
            break
    else:
        logger.warning("Run timed out after %s seconds.", elapsed)
        return containerrun

    return containerrun


def get_input_datasets(kive: kiveapi.KiveAPI,
                       script: Path,
                       inputs: Iterable[Path]) \
        -> Iterable[str]:

    for arg in ([script] + list(inputs)):
        dataset = upload_or_retrieve_dataset(
            kive,
            os.path.basename(arg),
            arg,
            groups=ALLOWED_GROUPS,
        )

        assert dataset is not None, "Expected a dataset."
        yield dataset.raw["url"]


def main(argv: Sequence[str]) -> int:
    parser = cli_parser()
    args = parser.parse_args(argv)
    if args.quiet:
        logger.setLevel(logging.ERROR)
    elif args.verbose:
        logger.setLevel(logging.INFO)
    elif args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARN)

    logger.debug("Start.")

    server = os.environ.get("MICALL_KIVE_SERVER")
    user = os.environ.get("MICALL_KIVE_USER")
    password = os.environ.get("MICALL_KIVE_PASSWORD")

    if server is None:
        raise UserError("Must set $MICALL_KIVE_SERVER environment variable.")
    if user is None:
        raise UserError("Must set $MICALL_KIVE_USER environment variable.")
    if password is None:
        raise UserError("Must set $MICALL_KIVE_PASSWORD environment variable.")

    inputs_args = args.inputs or []
    inputs = [arg.name for arg in inputs_args]

    kive = kiveapi.KiveAPI(server)
    try:
        kive.login(user, password)
    except kiveapi.KiveAuthException as e:
        raise UserError("Login failed: %s", str(e))

    logger.debug("Logged in successfully.")

    if args.output is not None:
        logger.debug("Making output directory at %r.", str(args.output))
        os.makedirs(args.output, exist_ok=True)

    inputpath = Path(args.script.name)

    # Get the app from a container family.
    app = find_kive_containerapp(kive, str(args.app_id))
    app_link = kive.server_url + app["absolute_url"]
    app_name = app["name"]
    app_container = app["container_name"]
    logger.debug("Using app %r.", app_link)
    logger.debug("App name is %r.", app_name)
    logger.debug("App container is %r.", app_container)

    appid = app['id']
    appargs = kive.endpoints.containerapps.get(f"{appid}/argument_list")
    input_appargs = [x for x in appargs if x["type"] == "I"]
    if len(inputs) > len(input_appargs):
        raise UserError("At most %r inputs supported, but got %r.",
                        len(input_appargs), len(inputs))

    appargs_urls = [x["url"] for x in input_appargs]
    input_datasets = get_input_datasets(kive, inputpath, inputs)
    dataset_list = [
        {
            "argument": x,
            "dataset": y,
        } for (x, y) in zip(appargs_urls, input_datasets)
    ]

    runspec = {
        "name": f"Free {inputpath.name}",
        "app": app["url"],
        "groups_allowed": ALLOWED_GROUPS,
        "datasets": dataset_list,
    }

    if args.batch:
        batch = create_batch(kive, args.batch)
        runspec["batch"] = batch["url"]

    logger.debug("Starting the run.")
    containerrun = kive.endpoints.containerruns.post(json=runspec)
    url = kive.server_url + containerrun["absolute_url"]
    logger.debug("Started at %r.", url)

    containerrun = await_containerrun(kive, containerrun)
    if args.output is not None:
        download_results(kive, containerrun, args.output)

    log_list = kive.get(containerrun["log_list"]).json()
    for log in log_list:
        if log["size"] == 0:
            continue

        if log["type"] == "O":
            kive.download_file(args.stdout, log["download_url"])
            args.stdout.flush()

        if log["type"] == "E":
            kive.download_file(args.stderr, log["download_url"])
            args.stderr.flush()

    if containerrun["state"] == "C":
        return 0
    else:
        return 1


if __name__ == '__main__':
    try:
        rc = main(sys.argv[1:])
        logger.debug("Done.")
    except BrokenPipeError:
        logger.debug("Broken pipe.")
        rc = 1
    except KeyboardInterrupt:
        logger.debug("Interrupted.")
        rc = 1
    except UserError as e:
        logger.fatal(e.fmt, *e.fmt_args)
        rc = e.code

    sys.exit(rc)
