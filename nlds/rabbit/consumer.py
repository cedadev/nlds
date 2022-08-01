# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '07 Dec 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

import functools
from abc import ABC, abstractmethod
import logging
from typing import Dict, List, NamedTuple
import pathlib as pth
import grp
import pwd
import os

from pika.exceptions import StreamLostError, AMQPConnectionError
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method, Header
from pika.amqp_object import Method
from pika.spec import Channel
from pydantic import BaseModel

from .publisher import RabbitMQPublisher
from ..server_config import (LOGGING_CONFIG_ENABLE, LOGGING_CONFIG_FILES, 
                             LOGGING_CONFIG_FORMAT, LOGGING_CONFIG_LEVEL,    
                             LOGGING_CONFIG_SECTION, LOGGING_CONFIG_STDOUT, 
                             RABBIT_CONFIG_QUEUES, LOGGING_CONFIG_STDOUT_LEVEL, 
                             RABBIT_CONFIG_QUEUE_NAME, LOGGING_CONFIG_ROLLOVER)
from ..utils.permissions import check_permissions

logger = logging.getLogger("nlds.root")


class RabbitQEBinding(BaseModel):
    exchange: str
    routing_key: str

class RabbitQueue(BaseModel):
    name: str
    bindings: List[RabbitQEBinding]

    @classmethod
    def from_defaults(cls, queue_name, exchange, routing_key):
        return cls(
            name=queue_name, 
            bindings=[RabbitQEBinding(exchange=exchange, 
                                      routing_key=routing_key)]
        )


class RabbitMQConsumer(ABC, RabbitMQPublisher):
    DEFAULT_QUEUE_NAME = "test_q"
    DEFAULT_ROUTING_KEY = "test"
    DEFAULT_EXCHANGE_NAME = "test_exchange"
    DEFAULT_REROUTING_INFO = "->"

    DEFAULT_CONSUMER_CONFIG = dict()

    def __init__(self, queue: str = None, setup_logging_fl=False):
        super().__init__(setup_logging_fl=False)

        # TODO: (2021-12-21) Only one queue can be specified at the moment, 
        # should be able to specify multiple queues to subscribe to but this 
        # isn't a priority.
        self.queue = queue
        try:
            if queue is not None:
                # If queue specified then select only that configuration
                if RABBIT_CONFIG_QUEUES in self.config:
                    # Load queue config if it exists in .server_config file.
                    self.queues = [RabbitQueue(**q) 
                                   for q in self.config[RABBIT_CONFIG_QUEUES] 
                                   if q[RABBIT_CONFIG_QUEUE_NAME] == queue]
                else: 
                    raise ValueError("No rabbit queues found in config.")
                
                if queue not in [q.name for q in self.queues]:
                    raise ValueError("Requested queue not in configuration.")
            
            else:
                raise ValueError("No queue specified, switching to default "
                                 "config.")
                
        except ValueError as e:
            self.log("Using default queue config - only fit for testing "
                     "purposes.", self.RK_LOG_WARNING)
            self.queue = self.DEFAULT_QUEUE_NAME
            self.queues = [RabbitQueue.from_defaults(
                self.DEFAULT_QUEUE_NAME,
                self.DEFAULT_EXCHANGE_NAME, 
                self.DEFAULT_ROUTING_KEY
            )]  

        # Load consumer-specific config
        if self.queue in self.whole_config:
            self.consumer_config = self.whole_config[self.queue]
        else: 
            self.consumer_config = self.DEFAULT_CONSUMER_CONFIG

        self.gid = None
        self.uid = None
        
        self.setup_logging(enable=setup_logging_fl)

    def reset(self):
        self.gid = None
        self.uid = None
    
    def load_config_value(self, config_option: str,
                          path_listify_fl: bool = False):
        """
        Function for verification and loading of options from the consumer-
        specific section of the .server_config file. Attempts to load from the 
        config section and reverts to hardcoded default value if an error is 
        encountered. Will not attempt to load an option if no default value is 
        available. 

        :param config_option:   (str) The option in the indexer section of the 
                                .server_config file to be verified and loaded.
        :param path_listify:    (boolean) Optional argument to control whether 
                                value should be treated as a list and each item 
                                converted to a pathlib.Path() object. 
        :returns:   The value at config_option, otherwise the default value as 
                    defined in Consumer.DEFAULT_CONSUMER_CONFIG. This is 
                    overloadable by 

        """
        # Check if the given config option is valid (i.e. whether there is an 
        # available default option)
        if config_option not in self.DEFAULT_CONSUMER_CONFIG:
            raise ValueError(
                f"Configuration option {config_option} not valid.\n"
                f"Must be one of {list(self.DEFAULT_CONSUMER_CONFIG.keys())}"
            )
        else:
            return_val = self.DEFAULT_CONSUMER_CONFIG[config_option]

        if config_option in self.consumer_config:
            try:
                return_val = self.consumer_config[config_option]
                if path_listify_fl:
                    # TODO: (2022-02-17) This is very specific to the use-case 
                    # of the indexer, could potentially be divided up into 
                    # listify and convert functions, but that's probably only 
                    # necessary if we refactor this into Consumer – which is 
                    # probably a good idea when we start fleshing out other 
                    # consumers
                    return_val_list = self.consumer_config[config_option]
                    # Make sure returned value is a list and not a string
                    # Note: it can't be any other iterable because it's loaded 
                    # from a json
                    assert isinstance(return_val_list, list)
                    return_val = [pth.Path(item) for item in return_val_list] 
            except KeyError:
                self.log(f"Invalid value for {config_option} in config file. "
                         f"Using default value instead.", self.RK_LOG_WARNING) 

        return return_val
    
    def parse_filelist(self, body_json):
        # Convert flat list into list of named tuples and the check it is, 
        # in fact, a list
        try:
            filelist = [self.IndexItem(i, r) for i, r in 
                        list(body_json[self.MSG_DATA][self.MSG_FILELIST])]
        except TypeError as e:
            self.log(
                "Failed to reformat list into indexitems. Filelist in "
                "message does not appear to be in the correct format.", 
                self.RK_LOG_ERROR
            )
            raise e

        return filelist

    def set_ids(self, body_json: Dict[str, str], use_pwd_gid_fl: bool = True):
        """Changes the real user- and group-ids stored in the class to that 
        specified in the incoming message details section so that permissions 
        on each file in a filelist can be checked.

        """
        # Attempt to get uid from, given username, in password db
        try:
            username = body_json[self.MSG_DETAILS][self.MSG_USER]
            pwddata = pwd.getpwnam(username)
            pwd_uid = pwddata.pw_uid
            pwd_gid = pwddata.pw_gid
        except KeyError as e:
            self.log(
                f"Problem fetching uid using username {username}", 
                self.RK_LOG_ERROR
            )
            raise e

        # Attempt to get gid from group name using grp module
        try:
            group_name = body_json[self.MSG_DETAILS][self.MSG_GROUP]
            grp_data = grp.getgrnam(group_name)
            grp_gid = grp_data.gr_gid
        except KeyError as e:         
            # If consumer setting is configured to allow the use of the gid 
            # gained from the pwd call, use that instead
            if use_pwd_gid_fl:
                self.log(
                    f"Problem fetching gid using grp, group name was "
                    f"{group_name}. Continuing with pwd_gid ({pwd_gid}).", 
                    self.RK_LOG_WARNING
                )
                grp_gid = pwd_gid
            else:
                self.log(
                    f"Problem fetching gid using grp, group name was "
                    f"{group_name}.", self.RK_LOG_ERROR
                )
                raise e

        self.uid = pwd_uid
        self.gid = grp_gid

    def check_path_access(self, path: pth.Path, stat_result: NamedTuple = None, 
                          access: int = os.R_OK, 
                          check_permissions_fl: bool = True) -> bool:
        """Checks that the given path is accessible, either by checking for its 
        existence or, if the check_permissions_fl is set, by doing a permissions
        check on the file's bitmask. This requires a stat of the file, so one 
        must be provided via stat_result else one is performed. The uid and gid 
        of the user must be set in the object as well, usually by having 
        performed RabbitMQConsumer.set_ids() beforehand.

        """
        if self.uid is None or self.gid is None:
            raise ValueError("uid and gid not set properly.")
        
        if not isinstance(path, pth.Path):
            raise ValueError("No valid path object was given.")

        if not path.exists():
            # Can't access or stat something that doesn't exist
            return False
        elif check_permissions_fl:
            # If no stat result is passed through then get our own
            if stat_result is None:
                stat_result = path.stat()
            return check_permissions(self.uid, self.gid, access=access, 
                                     stat_result=stat_result)
        else:
            return True

    def setup_logging(self, enable=False, log_level: str = None, 
                      log_format: str = None, add_stdout_fl: bool = False, 
                      stdout_log_level: str = None, log_files: List[str]=None,
                      log_rollover: str = None) -> None:
        """
        Override of the publisher method which allows consumer-specific logging 
        to take precedence over the general logging configuration.

        """
        # TODO: (2022-03-01) This is quite verbose and annoying to extend. 
        if LOGGING_CONFIG_SECTION in self.consumer_config:
            consumer_logging_conf = self.consumer_config[LOGGING_CONFIG_SECTION]
            if LOGGING_CONFIG_ENABLE in consumer_logging_conf: 
                enable = consumer_logging_conf[LOGGING_CONFIG_ENABLE]
            if LOGGING_CONFIG_LEVEL in consumer_logging_conf:
                log_level = consumer_logging_conf[LOGGING_CONFIG_LEVEL]
            if LOGGING_CONFIG_FORMAT in consumer_logging_conf:
                log_format = consumer_logging_conf[LOGGING_CONFIG_FORMAT]
            if LOGGING_CONFIG_STDOUT in consumer_logging_conf:
                add_stdout_fl = consumer_logging_conf[LOGGING_CONFIG_STDOUT]
            if LOGGING_CONFIG_STDOUT_LEVEL in consumer_logging_conf:
                stdout_log_level = consumer_logging_conf[LOGGING_CONFIG_STDOUT_LEVEL]
            if LOGGING_CONFIG_FILES in consumer_logging_conf:
                log_files = consumer_logging_conf[LOGGING_CONFIG_FILES]
            if LOGGING_CONFIG_ROLLOVER in consumer_logging_conf:
                log_rollover = consumer_logging_conf[LOGGING_CONFIG_ROLLOVER]

        # Allow the hard-coded default deactivation of logging to be overridden 
        # by the consumer-specific logging config
        if not enable:
            return

        return super().setup_logging(enable, log_level, log_format, 
                                     add_stdout_fl, stdout_log_level, log_files,
                                     log_rollover)

    @abstractmethod
    def callback(self, ch: Channel, method: Method, properties: Header, 
                 body: bytes, connection: Connection):
        pass

    def declare_bindings(self) -> None:
        """
        Overridden method from Publisher, additionally declares the queues and 
        queue-exchange bindings outlined in the config file. If no queues were 
        set then the default - generated within __init__ - is used instead. 

        """
        super().declare_bindings()
        for queue in self.queues:
            self.channel.queue_declare(queue=queue.name)
            for binding in queue.bindings:
                self.channel.queue_bind(exchange=binding.exchange, 
                                        queue=queue.name, 
                                        routing_key=binding.routing_key)
            # Apply callback to all queues
            wrapped_callback = functools.partial(self.callback, 
                                                 connection=self.connection)
            self.channel.basic_consume(queue=queue.name, 
                                       on_message_callback=wrapped_callback, 
                                       auto_ack=True)
    
    @staticmethod
    def split_routing_key(routing_key: str) -> None:
        """
        Method to simply verify and split the routing key into parts. 

        :return: 3-tuple of routing key parts
        """
        rk_parts = routing_key.split('.')
        if len(rk_parts) != 3:
            raise ValueError(f"Routing key ({routing_key}) malformed, should "
                             "consist of 3 parts.")
        return rk_parts

    @classmethod
    def append_route_info(cls, body: Dict, route_info: str = None):
        if route_info is None: 
            route_info = cls.DEFAULT_REROUTING_INFO
        if cls.MSG_ROUTE in body[cls.MSG_DETAILS]:
            body[cls.MSG_DETAILS][cls.MSG_ROUTE] += route_info
        else:
            body[cls.MSG_DETAILS][cls.MSG_ROUTE] = route_info
        return body
    
    def log(self, log_message: str, log_level: str, target: str = None, 
            **kwargs) -> None:
        if not target:
            target = self.queue
        super().log(log_message, log_level, target, **kwargs)

    def run(self):
        """
        Method to run when thread is started. Creates an AMQP connection
        and sets some exception handling.

        A common exception which occurs is StreamLostError.
        The connection should get reset if that happens.

        :return:
        """

        while True:
            self.get_connection()

            try:
                startup_message = f"{self.DEFAULT_QUEUE_NAME} - READY"
                logger.info(startup_message)
                self.channel.start_consuming()

            except KeyboardInterrupt:
                self.channel.stop_consuming()
                break

            except (StreamLostError, AMQPConnectionError) as e:
                # Log problem
                logger.error('Connection lost, reconnecting', exc_info=e)
                continue

            except Exception as e:
                logger.critical(e)

                self.channel.stop_consuming()
                break
