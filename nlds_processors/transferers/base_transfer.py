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
from nlds_processors.transferers.transfer_error import TransferError


class BaseTransferConsumer(StattingConsumer, ABC):
    DEFAULT_QUEUE_NAME = "transfer_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}.{RK.TRANSFER}.{RK.WILD}"
    DEFAULT_REROUTING_INFO = f"->{DEFAULT_QUEUE_NAME.upper()}"

    _TENANCY = "tenancy"
    _REQUIRE_SECURE = "require_secure_fl"
    _CHECK_PERMISSIONS = "check_permissions_fl"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    _MAX_RETRIES = "max_retries"
    _FILELIST_MAX_LENGTH = "filelist_max_length"
    _REMOVE_ROOT_SLASH = "remove_root_slash_fl"
    DEFAULT_CONSUMER_CONFIG = {
        _TENANCY: None,
        _REQUIRE_SECURE: True,
        _CHECK_PERMISSIONS: True,
        _PRINT_TRACEBACKS: False,
        _MAX_RETRIES: 5,
        _FILELIST_MAX_LENGTH: 1000,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.tenancy = self.load_config_value(self._TENANCY)
        self.require_secure_fl = self.load_config_value(self._REQUIRE_SECURE)
        self.check_permissions_fl = self.load_config_value(self._CHECK_PERMISSIONS)
        self.print_tracebacks_fl = self.load_config_value(self._PRINT_TRACEBACKS)
        self.max_retries = self.load_config_value(self._MAX_RETRIES)
        self.filelist_max_len = self.load_config_value(self._FILELIST_MAX_LENGTH)

        self.reset()

    def callback(self, ch, method, properties, body, connection):
        self.reset()

        # Convert body from bytes to string for ease of manipulation
        body = body.decode("utf-8")
        body_json = json.loads(body)

        if self._is_system_status_check(body_json=body_json, properties=properties):
            return

        self.log(
            f"Received {json.dumps(body_json, indent=4)} from "
            f"{self.queues[0].name} ({method.routing_key})",
            RK.LOG_DEBUG,
        )

        # Verify routing key is appropriate
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError as e:
            self.log(
                "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
            )
            return

        ###
        # Verify and load message contents
        try:
            transaction_id = body_json[MSG.DETAILS][MSG.TRANSACT_ID]
        except KeyError:
            self.log("Transaction id unobtainable, exiting callback.", RK.LOG_ERROR)
            return

        try:
            filelist = self.parse_filelist(body_json)
        except TypeError as e:
            self.log("Filelist not parseable, exiting callback", RK.LOG_ERROR)
            return

        try:
            access_key, secret_key, tenancy = self.get_objectstore_config(body_json)
        except TransferError:
            self.log("Objectstore config unobtainable, exiting callback.", RK.LOG_ERROR)
            return

        # Set uid and gid from message contents if configured to check
        # permissions
        if self.check_permissions_fl:
            self.set_ids(body_json)

        # Append route info to message to track the route of the message
        body_json = self.append_route_info(body_json)

        if rk_parts[2] == RK.INITIATE:
            self.log(
                "Aggregating list into more appropriately sized sub-lists for "
                "parallelised uploads.",
                RK.LOG_INFO,
            )

            # Make a new routing key which returns message to this queue
            rk_transfer_start = ".".join([rk_parts[0], rk_parts[1], RK.START])
            # Aggregate files into bins of approximately equal size and split
            # the transaction into subtransactions to allow parallel transfers
            sub_lists = bin_files(filelist)
            for sub_list in sub_lists:
                self.send_pathlist(
                    sub_list, rk_transfer_start, body_json, state=State.INITIALISING
                )
        elif rk_parts[2] == RK.START:
            # Start transfer - this is implementation specific and handled by
            # child classes
            self.log(f"Starting object store transfer with {tenancy}", RK.LOG_INFO)
            self.transfer(
                transaction_id,
                tenancy,
                access_key,
                secret_key,
                filelist,
                rk_parts[0],
                body_json,
            )
        else:
            raise TransferError(f"Unknown routing key {rk_parts[2]}")

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
        return super().check_path_access(
            path, stat_result, access, self.check_permissions_fl
        )

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
