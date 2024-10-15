# encoding: utf-8
"""
archive_base.py
NOTE: This module is imported into a revision, and so should be very defensive 
with how it imports external modules (like xrootd). 
"""
__author__ = "Jack Leland and Neil Massey"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from abc import ABC, abstractmethod
import json
from typing import List, Dict, Tuple
from enum import Enum

from nlds_processors.transferers.base_transfer import BaseTransferConsumer
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

    _TAPE_POOL = "tape_pool"
    _TAPE_URL = "tape_url"
    _CHUNK_SIZE = "chunk_size"
    _QUERY_CHECKSUM = "query_checksum_fl"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    ARCHIVE_CONSUMER_CONFIG = {
        _TAPE_POOL: None,
        _TAPE_URL: None,
        _CHUNK_SIZE: 5 * (1024**2),  # Default to 5 MiB
        _QUERY_CHECKSUM: True,
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
        self.query_checksum_fl = self.load_config_value(self._QUERY_CHECKSUM)
        self.reset()

    def callback(self, ch, method, properties, body, connection):
        """Callback for the base archiver consumer. Takes the message contents
        in body and runs some standard objectstore verification (reused from the
        BaseTransferConsumer) as well as some more tape-specific config
        scraping, then runs the appropriate transfer function.
        """
        if not self._callback_common(ch, method, properties, body, connection):
            return

        # create aggregates
        if self.rk_parts[2] == RK.INITIATE:
            self.log(
                "Aggregating filelist into appropriately sized sub-lists for each "
                "Aggregation",
                RK.LOG_INFO
            )
            # Make a new routing key which returns message to this queue
            rk_transfer_start = ".".join([self.rk_parts[0], self.rk_parts[1], RK.START])
            # Aggregate files into bins of approximately equal size and split
            # the transaction into subtransactions to allow parallel transfers
            sub_lists = bin_files(self.filelist)
            for sub_list in sub_lists:
                self.send_pathlist(
                    sub_list,
                    rk_transfer_start,
                    self.body_json,
                    state=State.INITIALISING,
                )
        elif self.rk_parts[2] == RK.START:
            try:
                tape_url = self.get_tape_config(self.body_json)
            except ArchiveError as e:
                self.log(
                    "Tape config unobtainable or invalid, exiting callback.",
                    RK.LOG_ERROR,
                )
                self.log(str(e), RK.LOG_DEBUG)
                raise e

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
