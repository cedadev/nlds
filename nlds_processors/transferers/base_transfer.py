# encoding: utf-8
"""

"""
__author__ = 'Jack Leland and Neil Massey'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from abc import ABC, abstractmethod
import json
import os
from typing import List, NamedTuple, Dict, Tuple
import pathlib as pth

from nlds.rabbit.statting_consumer import StattingConsumer
from nlds.rabbit.publisher import RabbitMQPublisher as RMQP
from nlds.details import PathDetails
from nlds.rabbit.consumer import FilelistType, State


class TransferError(Exception):
    pass


class BaseTransferConsumer(StattingConsumer, ABC):
    DEFAULT_QUEUE_NAME = "transfer_q"
    DEFAULT_ROUTING_KEY = (f"{RMQP.RK_ROOT}.{RMQP.RK_TRANSFER}.{RMQP.RK_WILD}")
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
        RMQP.RETRY_DELAYS: RMQP.DEFAULT_RETRY_DELAYS,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.tenancy = self.load_config_value(
            self._TENANCY)
        self.require_secure_fl = self.load_config_value(
            self._REQUIRE_SECURE)
        self.check_permissions_fl = self.load_config_value(
            self._CHECK_PERMISSIONS)
        self.print_tracebacks_fl = self.load_config_value(
            self._PRINT_TRACEBACKS)
        self.max_retries = self.load_config_value(
            self._MAX_RETRIES)
        self.filelist_max_len = self.load_config_value(
            self._FILELIST_MAX_LENGTH)
        self.retry_delays = self.load_config_value(
            self.RETRY_DELAYS)

        self.reset()


    def callback(self, ch, method, properties, body, connection):
        self.reset()
        
        # Convert body from bytes to string for ease of manipulation
        body = body.decode("utf-8")
        body_json = json.loads(body)
        
        
        # This checks if the message was for a system status check
        try:
            api_method = body_json[self.MSG_DETAILS][self.MSG_API_ACTION]
        except KeyError:
            self.log(f"Message did not contain api_method", self.RK_LOG_INFO)
            api_method = None
        
        
        # If received system test message, reply to it (this is for system status check)
        if api_method == "system_stat":
            if properties.correlation_id is not None and properties.correlation_id != self.channel.consumer_tags[0]:
                return False
            if (body_json["details"]["ignore_message"]) == True:
                return
            else:
                self.publish_message(
                    properties.reply_to,
                    msg_dict=body_json,
                    exchange={'name': ''},
                    correlation_id=properties.correlation_id
                )
            return
        

        self.log(f"Received {json.dumps(body_json, indent=4)} from "
                 f"{self.queues[0].name} ({method.routing_key})", 
                 self.RK_LOG_DEBUG)

        # Verify routing key is appropriate
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError as e:
            self.log(
                "Routing key inappropriate length, exiting callback.", 
                self.RK_LOG_ERROR
            )
            return

        ### 
        # Verify and load message contents 

        try: 
            transaction_id = body_json[self.MSG_DETAILS][self.MSG_TRANSACT_ID]
        except KeyError:
            self.log(
                "Transaction id unobtainable, exiting callback.", 
                self.RK_LOG_ERROR
            )
            return

        filelist = self.parse_filelist(body_json)

        try:
            access_key, secret_key, tenancy = self.get_objectstore_config(
                body_json
            )
        except TransferError: 
            self.log("Objectstore config unobtainable, exiting callback.", 
                     self.RK_LOG_ERROR)
            return
        
        # First check for transaction-level message failure and boot back to 
        # catalog if necessary. We do this here so that errors in set_ids() can
        # still be caught at the transaction-level
        retries = self.get_retries(body_json)
        if retries is not None and retries.count > self.max_retries:
            rk_failed = ".".join([rk_parts[0], rk_parts[1], self.RK_FAILED])
            if rk_parts[1] == self.RK_TRANSFER_PUT:
                # For transfer-put, mark the message as 'processed' so it can be
                # failed more safely.
                mode = FilelistType.processed
                state = State.CATALOG_ROLLBACK
                save_reasons_fl = True
            else:
                # Otherwise fail it regularly
                mode = FilelistType.failed
                state = None
                save_reasons_fl = False
            self.send_pathlist(filelist, rk_failed, body_json, mode=mode, 
                               state=state, save_reasons_fl=save_reasons_fl)
            return

        # Set uid and gid from message contents if configured to check 
        # permissions
        if self.check_permissions_fl:
            self.set_ids(body_json)

        self.log(f"Starting object store transfer with {tenancy}", 
                 self.RK_LOG_INFO)

        # Append route info to message to track the route of the message
        body_json = self.append_route_info(body_json)

        # Start transfer - this is implementation specific and handled by child 
        # classes 
        self.transfer(transaction_id, tenancy, access_key, secret_key, 
                      filelist, rk_parts[0], body_json)
        

    def get_objectstore_config(self, body_dict) -> Tuple:
        try:
            access_key = body_dict[self.MSG_DETAILS][self.MSG_ACCESS_KEY]
            secret_key = body_dict[self.MSG_DETAILS][self.MSG_SECRET_KEY]
        except KeyError:
            reason = "Secret key or access key unobtainable"
            self.log(f"{reason}, exiting callback", self.RK_LOG_ERROR)
            raise TransferError(reason)

        tenancy = None
        # If tenancy specified in message details then override the server-
        # config value
        if (self.MSG_TENANCY in body_dict[self.MSG_DETAILS] 
                and body_dict[self.MSG_DETAILS][self.MSG_TENANCY] is not None):
            tenancy = body_dict[self.MSG_DETAILS][self.MSG_TENANCY]
        else:
            tenancy = self.tenancy
        
        # Check to see whether tenancy has been specified in either the message 
        # or the server_config - exit if not. 
        if tenancy is None:
            reason = "No tenancy specified at server- or request-level"
            self.log(f"{reason}, exiting callback.", self.RK_LOG_ERROR)
            raise TransferError(reason) 
        
        return access_key, secret_key, tenancy
      

    def check_path_access(self, path: pth.Path, stat_result: NamedTuple = None, 
                          access: int = os.R_OK) -> bool:
        return super().check_path_access(
            path, 
            stat_result, 
            access, 
            self.check_permissions_fl
        )


    @abstractmethod
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, filelist: List[PathDetails], rk_origin: str,
                 body_json: Dict[str, str]):
        pass
