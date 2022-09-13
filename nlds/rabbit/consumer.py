# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '07 Dec 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from enum import Enum
import functools
from abc import ABC, abstractmethod
import logging
import traceback
from typing import Dict, List, NamedTuple, Union
import pathlib as pth
import grp
import pwd
import os
import json
from datetime import datetime, timedelta

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
from ..details import PathDetails

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

class FilelistType(Enum):
    raw = 0
    processed = 1
    retry = 2
    failed = 3
    indexed = 1
    transferred = 1

class RabbitMQConsumer(ABC, RabbitMQPublisher):
    DEFAULT_QUEUE_NAME = "test_q"
    DEFAULT_ROUTING_KEY = "test"
    DEFAULT_EXCHANGE_NAME = "test_exchange"
    DEFAULT_REROUTING_INFO = "->"

    DEFAULT_CONSUMER_CONFIG = dict()

    def __init__(self, queue: str = None, setup_logging_fl=False):
        super().__init__(name=queue, setup_logging_fl=False)

        # TODO: (2021-12-21) Only one queue can be specified at the moment, 
        # should be able to specify multiple queues to subscribe to but this 
        # isn't a priority.
        self.name = queue
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
            print("Using default queue config - only fit for testing purposes.")
            self.name = self.DEFAULT_QUEUE_NAME
            self.queues = [RabbitQueue.from_defaults(
                self.DEFAULT_QUEUE_NAME,
                self.DEFAULT_EXCHANGE_NAME, 
                self.DEFAULT_ROUTING_KEY
            )]  

        # Load consumer-specific config
        if self.name in self.whole_config:
            self.consumer_config = self.whole_config[self.name]
        else: 
            self.consumer_config = self.DEFAULT_CONSUMER_CONFIG

        # Memeber variables to temporarily hold user- and group-id of a message
        self.gid = None
        self.uid = None
        
        # Controls default behaviour of logging when certain exceptions are 
        # caught in the callback. 
        self.print_tracebacks_fl = True

        # Set up the logging and pass through constructor parameter
        self.setup_logging(enable=setup_logging_fl)

    def reset(self) -> None:
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
    
    def parse_filelist(self, body_json: dict) -> List[PathDetails]:
        # Convert flat list into list of PathDetails objects and the check it 
        # is, in fact, a list
        try:
            filelist = [PathDetails.from_dict(pd_dict) for pd_dict in 
                        list(body_json[self.MSG_DATA][self.MSG_FILELIST])]
        except TypeError as e:
            self.log(
                "Failed to reformat list into PathDetails objects. Filelist in "
                "message does not appear to be in the correct format.", 
                self.RK_LOG_ERROR
            )
            raise e

        return filelist

    def _choose_list(self, list_type: FilelistType = FilelistType.processed
                    ) -> List[PathDetails]:
        """ Choose the correct pathlist for a given mode of operation. This 
        requires that the appropriate member variable be instantiated in the 
        consumer class.
        """
        if list_type == FilelistType.processed:
            return self.completelist
        elif list_type == FilelistType.retry:
            return self.retrylist
        elif list_type == FilelistType.failed:
            return self.failedlist
        else: 
            raise ValueError(f"Invalid list type provided ({list_type})")

    def append_and_send(self, 
            path_details: PathDetails, 
            routing_key: str, 
            body_json: Dict[str, str], 
            filesize: int = None,
            list_type: Union[FilelistType, str] = FilelistType.processed,
        ) -> None:
        """Append a path details item to an existing PathDetails list and then 
        determine if said list requires sending to the exchange due to size 
        limits (can be either file size or list length). Which pathlist is to 
        be used is determined by the list_type passed, which can be either a 
        FilelistType enum or an appropriate string that can be cast into one. 

        The list will by default be capped by list-length, with the maximum 
        determined by a filelist_max_length config variable (defaults to 1000). 
        This behaviour is overriden by specifying the filesize kwarg, which is 
        in kilobytes and must be an integer, at consumption time.

        The message body and destination should be specified through body_json 
        and routing_key respectively. 

        NOTE: This was refactored here from the index/transfer processors. 
        Might make more sense to put it somewhere else given there are specific 
        config variables required (filelist_max_length, message_max_size) which 
        could fail with an AttributeError?

        """
        # If list_type given as a string then attempt to cast it into an 
        # appropriate enum
        if not isinstance(list_type, FilelistType):
            try:
                list_type = FilelistType[list_type]
            except KeyError:
                raise ValueError("list_type value invalid, must be a "
                                 "FilelistType enum or a string capabale of "
                                 f"being cast to such (list_type={list_type})")

        # Select correct pathlist and append the given PathDetails object
        pathlist = self._choose_list(list_type)
        pathlist.append(path_details)

        # If filesize has been passed then use total list size as message cap
        if filesize:
            # NOTE: This references a general pathlist but a specific list size, 
            # perhaps these two should be combined together into a single 
            # pathlist object? Might not be necessary for just this small code 
            # snippet. 
            self.completelist_size += filesize
            
            # Send directly to exchange and reset filelist
            if self.completelist_size >= self.message_max_size:
                self.send_pathlist(
                    pathlist, routing_key, body_json, mode=list_type
                )
                pathlist.clear()
                self.completelist_size = 0

        # The default message cap is the length of the pathlist. This applies
        # to failed or problem lists by default
        elif len(pathlist) >= self.filelist_max_len:
            # Send directly to exchange and reset filelist
            self.send_pathlist(
                pathlist, routing_key, body_json, mode=list_type
            )
            pathlist.clear()

    def send_pathlist(self, pathlist: List[PathDetails], routing_key: str, 
                       body_json: Dict[str, str], 
                       mode: FilelistType = FilelistType.processed
                       ) -> None:
        """ Convenience function which sends the given list of PathDetails 
        objects to the exchange with the given routing key and message body. 
        Mode specifies what to put into the log message, as well as determining 
        whether the list should be retry-reset and whether the message should be 
        delayed.

        """
        self.log(f"Sending list back to exchange (routing_key = {routing_key})",
                 self.RK_LOG_INFO)

        delay = 0
        if mode == FilelistType.processed:
            # Reset the retries upon successful indexing. 
            for path_details in pathlist:
                path_details.reset_retries()
        elif mode == FilelistType.retry:
            # Delay the retry message depending on how many retries have been 
            # accumulated. All retries in a retry list _should_ be the same so 
            # base it off of the first one.
            delay = self.get_retry_delay(pathlist[0].retries)
            self.log(f"Adding {delay / 1000}s delay to retry. Should be sent at"
                     f" {datetime.now() + timedelta(milliseconds=delay)}", 
                     self.RK_LOG_DEBUG)
        
        body_json[self.MSG_DATA][self.MSG_FILELIST] = pathlist
        self.publish_message(routing_key, json.dumps(body_json), delay=delay)

    def set_ids(
            self, 
            body_json: Dict[str, str], 
            use_pwd_gid_fl: bool = True
        ) -> None:
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
                stdout_log_level = (
                    consumer_logging_conf[LOGGING_CONFIG_STDOUT_LEVEL]
                )
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

    @staticmethod
    def _acknowledge_message(channel: Channel, delivery_tag: str) -> None:
        """Acknowledge a message with a basic ack. This is the bare minimum 
        requirement for an acknowledgement according to rabbit protocols.

        :param channel:         Channel which message came from
        :param delivery_tag:    Message id
        """

        logger.debug(f'Acknowledging message: {delivery_tag}')
        if channel.is_open:
            channel.basic_ack(delivery_tag)

    def acknowledge_message(self, channel: Channel, delivery_tag: str, 
                            connection: Connection) -> None:
        """Method for acknowledging a message so the next can be fetched. This 
        should be called at the end of a consumer callback, and - in order to do 
        so thread-safely - from within connection object.  All of the required 
        params come from the standard callback params.

        :param channel:         Callback channel param
        :param delivery_tag:    From the callback method param. eg. 
                                method.delivery_tag
        :param connection:      Connection object from the callback param
        """
        cb = functools.partial(self._acknowledge_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

    @abstractmethod
    def callback(self, ch: Channel, method: Method, properties: Header, 
                 body: bytes, connection: Connection) -> None:
        """Standard consumer callback function as defined by rabbitMQ, with the 
        standard callback parameters of Channel, Method, Header, Body (in bytes)
        and Connection.

        This is the working method of a consumer, i.e. it does the job the 
        consumer has been designed to do, and so must be implemented and 
        overridden by any child consumer classes. 
        """
        NotImplementedError
    
    def _wrapped_callback(self, ch: Channel, method: Method, properties: Header, 
                          body: bytes, connection: Connection) -> None:
        """Wrapper around standard callback function which adds error handling 
        and manual message acknowledgement. All arguments are the same as those 
        in self.callback, i.e. the standard rabbitMQ consumer callback 
        parameters.

        This should be performed on all consumers and should be left untouched 
        in child implementations.
        """
        # Wrap callback with a try-except catching a selection of common 
        # errors which can be caught without stopping consumption. 
        try:
            self.callback(ch, method, properties, body, connection)
        except (ValueError, TypeError, KeyError, PermissionError) as e:
            if self.print_tracebacks_fl:
                tb = traceback.format_exc()
                self.log(tb, self.RK_LOG_DEBUG)
            self.log(
                f"Encountered error ({e}), sending to logger.", 
                self.RK_LOG_ERROR, exc_info=e
            )
            self.log(
                f"Failed message content: {body}",
                self.RK_LOG_DEBUG
            )
        else:
            # Ack message only if it has failed in the limited number of ways 
            # above
            self.acknowledge_message(ch, method.delivery_tag, connection)

            
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
            wrapped_callback = functools.partial(self._wrapped_callback, 
                                                 connection=self.connection)
            self.channel.basic_consume(queue=queue.name, 
                                       on_message_callback=wrapped_callback)
    
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
    def append_route_info(cls, body: Dict, route_info: str = None) -> Dict:
        if route_info is None: 
            route_info = cls.DEFAULT_REROUTING_INFO
        if cls.MSG_ROUTE in body[cls.MSG_DETAILS]:
            body[cls.MSG_DETAILS][cls.MSG_ROUTE] += route_info
        else:
            body[cls.MSG_DETAILS][cls.MSG_ROUTE] = route_info
        return body

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
                # Catch all other exceptions and log them as critical. 
                tb = traceback.format_exc()
                self.log(tb, self.RK_LOG_CRITICAL, exc_info=e)

                self.channel.stop_consuming()
                break
