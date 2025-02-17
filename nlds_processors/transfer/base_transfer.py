# encoding: utf-8
"""
base_transfer.py
"""
__author__ = "Jack Leland and Neil Massey"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from abc import ABC, abstractmethod
import json
import os
from typing import List, NamedTuple, Dict, Tuple
import pathlib as pth

from nlds.rabbit.statting_consumer import StattingConsumer
from nlds.details import PathDetails
from nlds.rabbit.consumer import State
from nlds_processors.utils.aggregations import bin_files
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG
from nlds_processors.transfer.transfer_error import TransferError


class BaseTransferConsumer(StattingConsumer, ABC):
    DEFAULT_QUEUE_NAME = "transfer_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}.{RK.TRANSFER}.{RK.WILD}"
    DEFAULT_REROUTING_INFO = f"->{DEFAULT_QUEUE_NAME.upper()}"

    _TENANCY = "tenancy"
    _REQUIRE_SECURE = "require_secure_fl"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    _FILELIST_MAX_LENGTH = "filelist_max_length"
    DEFAULT_CONSUMER_CONFIG = {
        _TENANCY: None,
        _REQUIRE_SECURE: True,
        _PRINT_TRACEBACKS: False,
        _FILELIST_MAX_LENGTH: 1000,
        StattingConsumer._FILELIST_MAX_SIZE: 16 * 1000 * 1000,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.tenancy = self.load_config_value(self._TENANCY)
        self.require_secure_fl = self.load_config_value(self._REQUIRE_SECURE)
        self.print_tracebacks_fl = self.load_config_value(self._PRINT_TRACEBACKS)
        self.filelist_max_len = self.load_config_value(self._FILELIST_MAX_LENGTH)
        self.filelist_max_size = self.load_config_value(self._FILELIST_MAX_SIZE)
        self.reset()

    def _callback_common(self, cm, method, properties, body, connection):
        self.reset()

        # Convert body from bytes to string for ease of manipulation
        self.body = body.decode("utf-8")
        self.body_json = json.loads(self.body)

        if self._is_system_status_check(
            body_json=self.body_json, properties=properties
        ):
            return False

        self.log(
            f"Received from {self.queues[0].name} ({method.routing_key})",
            RK.LOG_DEBUG,
            body_json=self.body_json,
        )

        # Verify routing key is appropriate
        try:
            self.rk_parts = self.split_routing_key(method.routing_key)
        except ValueError as e:
            self.log(
                "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
            )
            return

        ###
        # Verify and load message contents
        try:
            self.transaction_id = self.body_json[MSG.DETAILS][MSG.TRANSACT_ID]
        except KeyError:
            self.log("Transaction id unobtainable, exiting callback.", RK.LOG_ERROR)
            return False

        try:
            self.filelist = self.parse_filelist(self.body_json)
        except TypeError as e:
            self.log("Filelist not parseable, exiting callback", RK.LOG_ERROR)
            return False

        try:
            (self.access_key, self.secret_key, self.tenancy) = (
                self.get_objectstore_config(self.body_json)
            )
        except TransferError:
            self.log("Objectstore config unobtainable, exiting callback.", RK.LOG_ERROR)
            return False

        # Set uid and gid from message contents
        self.log("Setting uid and gids now.", RK.LOG_INFO)
        try:
            self.set_ids(self.body_json)
        except KeyError as e:
            self.log("Problem running set_ids, exiting callback", RK.LOG_ERROR)
            return False

        # Append route info to message to track the route of the message
        self.body_json = self.append_route_info(self.body_json)
        return True

    def callback(self, ch, method, properties, body, connection):

        if not self._callback_common(ch, method, properties, body, connection):
            # fail all files if callback common fails
            rk_transfer_failed = ".".join(
                [self.rk_parts[0], self.rk_parts[1], RK.FAILED]
            )
            for file in self.filelist:
                file.failure_reason = 'Failed in transfer init'
                
            self.send_pathlist(
                self.filelist, rk_transfer_failed, self.body_json, state=State.FAILED
            )
            return

        # API-methods that have an INITIATE phase will split the files across
        # sub-messages to parallelise upload and download.
        # These methods are:
        #       transfer-put  : to parallelise upload to the object storage
        #       transfer-get  : to parallelise download from the object storage
        #       archive-put   : to form the aggregates on the tape
        # Note: archive-get does not have an INITIATE phase.  This is because the
        # aggregates need to be prepared (staged to cache) and, for efficiency they
        # should be prepared (staged) all at once. Once they are staged, the files are
        # split by aggregate into separate messages.

        if self.rk_parts[2] == RK.INITIATE:
            self.log(
                "Aggregating list into more appropriately sized sub-lists for "
                "parallelised uploads.",
                RK.LOG_INFO,
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
                    state=State.TRANSFER_INIT,
                )
        elif self.rk_parts[2] == RK.START:
            # Start transfer - this is implementation specific and handled by
            # child classes
            self.log(f"Starting object store transfer with {self.tenancy}", RK.LOG_INFO)
            self.transfer(
                self.transaction_id,
                self.tenancy,
                self.access_key,
                self.secret_key,
                self.filelist,
                self.rk_parts[0],
                self.body_json,
            )
        else:
            raise TransferError(f"Unknown routing key {self.rk_parts[2]}")

    def get_objectstore_config(self, body_dict) -> Tuple:
        try:
            access_key = body_dict[MSG.DETAILS][MSG.ACCESS_KEY]
            secret_key = body_dict[MSG.DETAILS][MSG.SECRET_KEY]
        except KeyError:
            reason = "Secret key or access key unobtainable"
            self.log(f"{reason}, exiting callback", RK.LOG_ERROR)
            raise TransferError(reason)

        tenancy = None
        # If tenancy specified in message details then override the server-
        # config value
        if (
            MSG.TENANCY in body_dict[MSG.DETAILS]
            and body_dict[MSG.DETAILS][MSG.TENANCY] is not None
        ):
            tenancy = body_dict[MSG.DETAILS][MSG.TENANCY]
        else:
            tenancy = self.tenancy

        # Check to see whether tenancy has been specified in either the message
        # or the server_config - exit if not.
        if tenancy is None:
            reason = "No tenancy specified at server- or request-level"
            self.log(f"{reason}, exiting callback.", RK.LOG_ERROR)
            raise TransferError(reason)

        return access_key, secret_key, tenancy

    def check_path_access(
        self, path: pth.Path, stat_result: NamedTuple = None, access: int = os.R_OK
    ) -> bool:
        return super().check_path_access(path, stat_result, access)

    @abstractmethod
    def transfer(
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
