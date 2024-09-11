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
from typing import List, Dict
from enum import Enum

from nlds_processors.transferers.base_transfer import (
    BaseTransferConsumer,
    TransferError,
)
from nlds.errors import CallbackError
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
    _MAX_RETRIES = "max_retries"
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
        self.reset()

        # Convert body from bytes to string for ease of manipulation and then to
        # a dict
        body = body.decode("utf-8")
        body_dict = json.loads(body)

        if self._is_system_status_check(body_json=body_dict, properties=properties):
            return

        # Verify routing key is appropriate
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError as e:
            self.log(
                "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
            )
            return

        filelist = self.parse_filelist(body_dict)

        ###
        # Verify and load message contents

        try:
            transaction_id = body_dict[MSG.DETAILS][MSG.TRANSACT_ID]
        except KeyError:
            self.log("Transaction id unobtainable, exiting callback.", RK.LOG_ERROR)
            return

        # Set uid and gid from message contents if configured to check
        # permissions
        if self.check_permissions_fl:
            self.log("Check permissions flag is set, setting uid and gids now.")
            self.set_ids(body_dict)

        try:
            access_key, secret_key, tenancy = self.get_objectstore_config(body_dict)
        except TransferError:
            self.log("Objectstore config unobtainable, exiting callback.", RK.LOG_ERROR)
            raise

        try:
            tape_url = self.get_tape_config(body_dict)
        except ArchiveError as e:
            self.log(
                "Tape config unobtainable or invalid, exiting callback.",
                RK.LOG_ERROR,
            )
            self.log(str(e), RK.LOG_DEBUG)
            raise

        self.log(
            f"Starting tape transfer between {tape_url} and object store " f"{tenancy}",
            RK.LOG_INFO,
        )

        # Append route info to message to track the route of the message
        body_dict = self.append_route_info(body_dict)

        self.transfer(
            transaction_id,
            tenancy,
            access_key,
            secret_key,
            tape_url,
            filelist,
            rk_parts[0],
            body_dict,
        )

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
                "No tenancy specified at server- or request-level, exiting " "callback."
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
        raise NotImplementedError()
