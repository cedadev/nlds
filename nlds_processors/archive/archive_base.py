# encoding: utf-8
"""
archive_base.py
"""
__author__ = "Jack Leland and Neil Massey"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Any
import os

from nlds_processors.transfer.base_transfer import BaseTransferConsumer
from nlds_processors.utils.aggregations import bin_files

from nlds.details import PathDetails
from nlds.rabbit.consumer import State
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG

from nlds.nlds_setup import USE_DISKTAPE, DISKTAPE_LOC

if USE_DISKTAPE:
    from nlds_processors.archive.s3_to_tarfile_disk import S3ToTarfileDisk
else:
    from nlds_processors.archive.s3_to_tarfile_tape import S3ToTarfileTape


class ArchiveError(Exception):
    pass


class BaseArchiveConsumer(BaseTransferConsumer, ABC):
    DEFAULT_QUEUE_NAME = "archive_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}.{RK.ARCHIVE}.{RK.WILD}"
    DEFAULT_REROUTING_INFO = f"->{DEFAULT_QUEUE_NAME.upper()}"

    _TAPE_POOL = "tape_pool"
    _TAPE_URL = "tape_url"
    _CHUNK_SIZE = "chunk_size"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    ARCHIVE_CONSUMER_CONFIG = {
        _TAPE_POOL: None,
        _TAPE_URL: None,
        _CHUNK_SIZE: 5 * (1024**2),  # Default to 5 MiB
        _PRINT_TRACEBACKS: False,
    }
    DEFAULT_CONSUMER_CONFIG = (
        BaseTransferConsumer.DEFAULT_CONSUMER_CONFIG | ARCHIVE_CONSUMER_CONFIG
    )

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.tape_pool = self.load_config_value(self._TAPE_POOL)
        self.tape_url = self.load_config_value(self._TAPE_URL)
        self.chunk_size = int(self.load_config_value(self._CHUNK_SIZE))
        self.reset()

    def _create_streamer(
        self,
        tenancy: str,
        access_key: str,
        secret_key: str,
        tape_url: str,
    ):
        """Helper function to create a streamer based on status of USE_DISK_TAPE"""
        if USE_DISKTAPE:
            disk_loc = os.path.expanduser(DISKTAPE_LOC)
            self.log(
                f"Starting connection between {disk_loc} and object store "
                f"{tenancy}",
                RK.LOG_INFO,
            )
            streamer = S3ToTarfileDisk(
                s3_tenancy=tenancy,
                s3_access_key=access_key,
                s3_secret_key=secret_key,
                disk_location=disk_loc,
                secure_fl=self.require_secure_fl,
                logger=self.log,
            )
        else:
            self.log(
                f"Starting connecting between {tape_url} and object store "
                f"{tenancy}",
                RK.LOG_INFO,
            )
            streamer = S3ToTarfileTape(
                s3_tenancy=tenancy,
                s3_access_key=access_key,
                s3_secret_key=secret_key,
                tape_url=tape_url,
                secure_fl=self.require_secure_fl,
                logger=self.log,
            )
        return streamer

    def callback(self, ch, method, properties, body, connection):
        """Callback for the base archive consumer. Takes the message contents
        in body and runs some standard objectstore verification (reused from the
        BaseTransferConsumer) as well as some more tape-specific config
        scraping, then runs the appropriate transfer function.
        """
        if not self._callback_common(ch, method, properties, body, connection):
            # fail all files if callback common fails
            rk_transfer_failed = ".".join(
                [self.rk_parts[0], self.rk_parts[1], RK.FAILED]
            )
            for file in self.filelist:
                file.failure_reason = 'Failed in archive transfer init'
                
            self.send_pathlist(
                self.filelist, rk_transfer_failed, self.body_json, state=State.FAILED
            )
            return
        # get tape_url for those routes that need it
        if self.rk_parts[2] in [RK.START, RK.PREPARE, RK.PREPARE_CHECK]:
            try:
                tape_url = self.get_tape_config(self.body_json)
            except ArchiveError as e:
                self.log(
                    "Tape config unobtainable or invalid, exiting callback.",
                    RK.LOG_ERROR,
                )
                self.log(str(e), RK.LOG_DEBUG)
                raise e

        # create aggregates
        if self.rk_parts[2] == RK.INITIATE:
            self.log(
                "Aggregating filelist into appropriately sized sub-lists for each "
                "Aggregation",
                RK.LOG_INFO,
            )
            # Make a new routing key which returns message to this queue
            rk_transfer_start = ".".join([self.rk_parts[0], self.rk_parts[1], RK.START])
            # Aggregate files into bins of approximately equal size and split
            # the transaction into subtransactions to allow parallel transfers
            sub_lists = bin_files(self.filelist)
            # assign ARCHIVE_GETTING or ARCHIVE_PUTTING to make it more obvious to the
            # user what is actually happening
            if self.rk_parts[1] == RK.ARCHIVE_GET:
                new_state = State.ARCHIVE_GETTING
            elif self.rk_parts[1] == RK.ARCHIVE_PUT:
                new_state = State.ARCHIVE_PUTTING
            else:
                new_state = State.ARCHIVE_INIT

            for sub_list in sub_lists:
                self.send_pathlist(
                    sub_list,
                    rk_transfer_start,
                    self.body_json,
                    state=new_state,
                )
        elif self.rk_parts[2] == RK.START:
            self.transfer(
                self.transaction_id,
                self.tenancy,
                self.access_key,
                self.secret_key,
                tape_url,
                self.filelist,
                self.rk_parts[0],
                self.body_json,
            )
        elif self.rk_parts[2] == RK.PREPARE:
            self.prepare(
                self.transaction_id,
                self.tenancy,
                self.access_key,
                self.secret_key,
                tape_url,
                self.filelist,
                self.rk_parts[0],
                self.body_json,
            )
        elif self.rk_parts[2] == RK.PREPARE_CHECK:
            self.prepare_check(
                self.transaction_id,
                self.tenancy,
                self.access_key,
                self.secret_key,
                tape_url,
                self.filelist,
                self.rk_parts[0],
                self.body_json,
            )
        else:
            raise ArchiveError(f"Unknown routing key {self.rk_parts[2]}")

    def get_tape_config(self, body_dict) -> Tuple:
        """Convenience function to extract tape relevant config from the message
        details section. Currently this is just the tape
        """
        tape_url = None
        if (
            MSG.TAPE_URL in body_dict[MSG.DETAILS]
            and body_dict[MSG.DETAILS][MSG.TAPE_URL] is not None
        ):
            tape_url = body_dict[MSG.DETAILS][MSG.TAPE_URL]
        else:
            tape_url = self.tape_url

        # Check to see whether tape_url has been specified in either the message
        # or the server_config - exit if not.
        if tape_url is None:
            reason = (
                "No tape_url specified at server- or request-level, exiting callback."
            )
            self.log(reason, RK.LOG_ERROR)
            raise ArchiveError(reason)

        return tape_url

    @classmethod
    def get_holding_prefix(cls, body: Dict[str, Any], holding_id: int = -1) -> str:
        """Get the uneditable holding information from the message body to
        reproduce the holding prefix made in the catalog"""
        try:
            if holding_id == -1:
                holding_id = body[MSG.META][MSG.HOLDING_ID]
            user = body[MSG.DETAILS][MSG.USER]
            group = body[MSG.DETAILS][MSG.GROUP]
        except KeyError as e:
            raise ArchiveError(f"Could not make holding prefix, original error: {e}")

        return f"nlds.{holding_id}.{user}.{group}"

    @abstractmethod
    def transfer(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        tape_url: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_dict: Dict[str, str],
    ):
        raise NotImplementedError

    @abstractmethod
    def prepare(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        tape_url: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, str],
    ):
        raise NotImplementedError

    @abstractmethod
    def prepare_check(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        tape_url: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, str],
    ):
        raise NotImplementedError
