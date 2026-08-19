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


class ArchiveError(Exception):
    pass


class BaseArchiveConsumer(BaseTransferConsumer, ABC):
    DEFAULT_QUEUE_NAME = "archive_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}.{RK.ARCHIVE}.{RK.WILD}"
    DEFAULT_REROUTING_INFO = f"->{DEFAULT_QUEUE_NAME.upper()}"

    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    ARCHIVE_CONSUMER_CONFIG = {
        _PRINT_TRACEBACKS: False,
    }
    DEFAULT_CONSUMER_CONFIG = (
        BaseTransferConsumer.DEFAULT_CONSUMER_CONFIG | ARCHIVE_CONSUMER_CONFIG
    )

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
        self.reset()

    def _parse_tape_url(self, body: Dict) -> str:
        # Get the tape_url from message, if none found then use the configured default
        if (
            MSG.TAPE_URL in body[MSG.DETAILS]
            and body[MSG.DETAILS][MSG.TAPE_URL] is not None
        ):
            tape_url = body[MSG.DETAILS][MSG.TAPE_URL]
        else:
            raise ArchiveError("tape_url not found in message details.")
        return tape_url

    def _create_streamer(
        self,
        tenancy: str,
        access_key: str,
        secret_key: str,
        tape_url: str = None,
    ):
        """Helper function to create a streamer based on the contents of tape_url.
        If the tape_url first character is "/" then it is a disk location.
        If it is "root" then it is a tape location.
        """
        if tape_url[0] == "/":
            from nlds_processors.archive.s3_to_tarfile_disk import S3ToTarfileDisk

            disk_loc = os.path.expanduser(self.disktape_loc)
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
                http_timeout=self.http_timeout,
                logger=self.log,
            )
        elif tape_url[0:7] == "root://":
            from nlds_processors.archive.s3_to_tarfile_tape import S3ToTarfileTape

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
                http_timeout=self.http_timeout,
                logger=self.log,
            )
        else:
            raise ArchiveError(
                f"Unknown tape_url format {tape_url} passed into in function call. "
                f"Format should be '/path/to/directory' for disktape and "
                f"'root://' for XrootD tape location."
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
                file.failure_reason = "Failed in archive transfer init"

            self.send_pathlist(
                self.filelist, rk_transfer_failed, self.body_json, state=State.FAILED
            )
            return

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
            # the transaction into sub-transactions to allow parallel transfers
            sub_lists = bin_files(
                self.filelist,
                target_bin_count=self.filelist_max_len,
                target_bin_size=self.filelist_max_size,
            )
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
        # transfer files (PUT or GET)
        elif self.rk_parts[2] == RK.START:
            self.transfer(
                self.transaction_id,
                self.tenancy,
                self.access_key,
                self.secret_key,
                self.filelist,
                self.rk_parts[0],
                self.body_json,
            )
        # prepare the files for transfer (GET) - i.e. fetch them from tape
        elif self.rk_parts[2] == RK.PREPARE:
            self.prepare(
                self.transaction_id,
                self.tenancy,
                self.access_key,
                self.secret_key,
                self.filelist,
                self.rk_parts[0],
                self.body_json,
            )
        # check whether the files have been fetched from tape by the prepare phase for
        # a transfer (GET)
        elif self.rk_parts[2] == RK.PREPARE_CHECK:
            self.prepare_check(
                self.transaction_id,
                self.tenancy,
                self.access_key,
                self.secret_key,
                self.filelist,
                self.rk_parts[0],
                self.body_json,
            )
        else:
            raise ArchiveError(f"Unknown routing key {self.rk_parts[2]}")

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
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, str],
    ):
        raise NotImplementedError
