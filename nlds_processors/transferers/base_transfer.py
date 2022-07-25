from abc import ABC, abstractmethod
import json
import traceback
from typing import List, NamedTuple

from nlds.rabbit.consumer import RabbitMQConsumer

class BaseTransferConsumer(RabbitMQConsumer, ABC):
    DEFAULT_QUEUE_NAME = "transfer_q"
    DEFAULT_ROUTING_KEY = (f"{RabbitMQConsumer.RK_ROOT}."
                           f"{RabbitMQConsumer.RK_TRANSFER}."
                           f"{RabbitMQConsumer.RK_WILD}")
    DEFAULT_REROUTING_INFO = f"->{DEFAULT_QUEUE_NAME.upper()}"

    _TENANCY = "tenancy"
    _REQUIRE_SECURE = "require_secure_fl"
    _CHECK_PERMISSIONS = "check_permissions_fl"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    _REMOVE_ROOT_SLASH = "remove_root_slash_fl"
    DEFAULT_CONSUMER_CONFIG = {
        _TENANCY: None,
        _REQUIRE_SECURE: True,
        _CHECK_PERMISSIONS: True,
        _PRINT_TRACEBACKS: False,
        _REMOVE_ROOT_SLASH: True,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.tenancy = self.load_config_value(self._TENANCY)
        self.require_secure_fl = self.load_config_value(self._REQUIRE_SECURE)
        self.check_permissions_fl = self.load_config_value(
            self._CHECK_PERMISSIONS)
        self.print_tracebacks_fl = self.load_config_value(
            self._PRINT_TRACEBACKS)
        self.remove_root_slash_fl = self.load_config_value(
            self._REMOVE_ROOT_SLASH)
        
    def callback(self, ch, method, properties, body, connection):
        try:
            # Convert body from bytes to string for ease of manipulation
            body = body.decode("utf-8")
            body_json = json.loads(body)

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
                access_key = body_json[self.MSG_DETAILS][self.MSG_ACCESS_KEY]
                secret_key = body_json[self.MSG_DETAILS][self.MSG_SECRET_KEY]
            except KeyError:
                self.log(
                    "Secret key or access key unobtainable, exiting callback.", 
                    self.RK_LOG_ERROR
                )
                return

            tenancy = None
            # If tenancy specified in message details then override the server-
            # config value
            if (self.MSG_TENANCY in body_json[self.MSG_DETAILS] 
                and body_json[self.MSG_DETAILS][self.MSG_TENANCY] is not None):
                tenancy = body_json[self.MSG_DETAILS][self.MSG_TENANCY]
            else:
                tenancy = self.tenancy
            
            # Check to see whether tenancy has been specified in either the 
            # message or the server_config - exit if not. 
            if tenancy is None:
                self.log(
                    "No tenancy specified at server- or request-level, exiting "
                    "callback.", 
                    self.RK_LOG_ERROR
                )
                return 

            self.log(f"Starting transfer to object store at {tenancy}", 
                     self.RK_LOG_INFO)

            # Append route info to message to track the route of the message
            body_json = self.append_route_info(body_json)

            # Start transfer - this is implementation specific and handled by 
            # child classes 
            self.transfer(transaction_id, tenancy, access_key, secret_key, 
                          filelist)

            self.log("Transfer complete, passing list back to worker for "
                     f"re-routing and cataloguing.", self.RK_LOG_INFO)
            
            new_routing_key = ".".join([
                rk_parts[0], 
                rk_parts[1], 
                self.RK_COMPLETE,
            ])
            self.publish_message(new_routing_key, json.dumps(body_json))
        
        except (ValueError, TypeError, KeyError, PermissionError) as e:
            if self.print_tracebacks_fl:
                tb = traceback.format_exc()
                self.log(tb, self.RK_LOG_DEBUG)
            self.log(
                f"Encountered error ({e}), sending to logger.", 
                self.RK_LOG_ERROR, exc_info=e
            )
            self.log(
                f"Failed message content: {json.dumps(body_json, index=4)}",
                self.RK_LOG_DEBUG
            )

    @abstractmethod
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, filelist: List[NamedTuple]):
        pass