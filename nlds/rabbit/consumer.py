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
from typing import Dict, List, Any
import pathlib as pth
from datetime import datetime, timedelta
import uuid
import json
from json.decoder import JSONDecodeError
from urllib3.exceptions import HTTPError
import signal
import threading as thr

from pika.exceptions import StreamLostError, AMQPConnectionError
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method, Header
from pika.amqp_object import Method
from pika.spec import Channel
from pydantic import BaseModel
from minio.error import S3Error

from .publisher import RabbitMQPublisher
from ..server_config import (
    LOGGING_CONFIG_ENABLE, 
    LOGGING_CONFIG_FILES, 
    LOGGING_CONFIG_FORMAT, 
    LOGGING_CONFIG_LEVEL,    
    LOGGING_CONFIG_SECTION, 
    LOGGING_CONFIG_STDOUT, 
    LOGGING_CONFIG_STDOUT_LEVEL, 
    LOGGING_CONFIG_BACKUP_COUNT,
    LOGGING_CONFIG_MAX_BYTES,
    RABBIT_CONFIG_QUEUE_NAME, 
    RABBIT_CONFIG_QUEUES, 
)
from ..details import PathDetails, Retries
from ..errors import RabbitRetryError, CallbackError

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
    catalogued = 1
    archived = 1

class State(Enum):
    # Generic states
    INITIALISING = -1
    ROUTING = 0
    COMPLETE = 100
    FAILED = 101
    # PUT workflow states
    SPLITTING = 1
    INDEXING = 2
    CATALOG_PUTTING = 3
    TRANSFER_PUTTING = 4
    CATALOG_ROLLBACK = 5
    # GET workflow states
    CATALOG_GETTING = 10
    ARCHIVE_GETTING = 11
    TRANSFER_GETTING = 12
    # ARCHIVE_PUT workflow states
    ARCHIVE_INIT = 20
    CATALOG_ARCHIVE_AGGREGATING = 21
    ARCHIVE_PUTTING = 22
    CATALOG_ARCHIVE_UPDATING = 23
    # Shared ARCHIVE states
    CATALOG_ARCHIVE_ROLLBACK = 40

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_
    
    @classmethod
    def has_name(cls, name):
        return name in cls._member_names_

    @classmethod
    def get_final_states(cls):
        final_states = (
            cls.TRANSFER_GETTING,
            cls.TRANSFER_PUTTING,
            cls.CATALOG_ARCHIVE_UPDATING,
            cls.CATALOG_ROLLBACK,
            cls.CATALOG_ARCHIVE_ROLLBACK,
            cls.FAILED,
        )
        return final_states
    
    @classmethod
    def get_failed_states(cls):
        return (
            cls.CATALOG_ROLLBACK,
            cls.CATALOG_ARCHIVE_ROLLBACK,
            cls.FAILED
        )

    def to_json(self):
        return self.value
    

class SigTermError(Exception):
    pass


class RabbitMQConsumer(ABC, RabbitMQPublisher):
    DEFAULT_QUEUE_NAME = "test_q"
    DEFAULT_ROUTING_KEY = "test"
    DEFAULT_EXCHANGE_NAME = "test_exchange"
    DEFAULT_REROUTING_INFO = "->"

    DEFAULT_CONSUMER_CONFIG : Dict[str, Any] = dict()

    # The state associated with finishing the consumer, must be set but can be 
    # overridden
    DEFAULT_STATE = State.ROUTING

    def __init__(self, queue: str = None, setup_logging_fl=False):
        super().__init__(name=queue, setup_logging_fl=False)
        self.loop = True

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

        # Member variable to keep track of the number of messages spawned during 
        # a callback, for monitoring sub_record creation
        self.sent_message_count = 0

        # (re)Declare the pathlists here to make them available without having 
        # to pass them through every function call. 
        self.completelist : List[PathDetails] = []
        self.retrylist: List[PathDetails] = []
        self.failedlist: List[PathDetails] = []
        self.max_retries = 5
        
        # Controls default behaviour of logging when certain exceptions are 
        # caught in the callback. 
        self.print_tracebacks_fl = True

        # List of active threads created by consumption process
        self.threads = []

        # Set up the logging and pass through constructor parameter
        self.setup_logging(enable=setup_logging_fl)


    def reset(self) -> None:
        self.sent_message_count = 0
        self.completelist = []
        self.retrylist = []
        self.failedlist = []
    

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
        """Convert flat list from message json into list of PathDetails objects 
        and the check it is, in fact, a list
        """
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
        # de-duplicate the filelist
        filelist = self.dedup_filelist(filelist)
        return filelist
    

    def dedup_filelist(self, filelist: List[PathDetails]) -> List[PathDetails]:
        """De-duplicate filelist 
        """
        new_filelist = []
        pathlist = []
        for pd in filelist:
            if not pd.original_path in pathlist:
                new_filelist.append(pd)
                pathlist.append(pd.original_path)

        return new_filelist


    def send_pathlist(
            self, 
            pathlist: List[PathDetails], 
            routing_key: str, 
            body_json: Dict[str, Any], 
            state: State = None,
            mode: FilelistType = FilelistType.processed, 
            warning: List[str] = None, 
            delay: int = None,
            save_reasons_fl: bool = False,
        ) -> None:
        """Convenience function which sends the given list of PathDetails 
        objects to the exchange with the given routing key and message body. 
        Mode specifies what to put into the log message, as well as determining 
        whether the list should be retry-reset and whether the message should be 
        delayed. Optionally, when resetting the retries the retry-reasons can be 
        saved for later perusal by setting the save_reasons_fl.

        Additionally forwards transaction state info on to the monitor. As part 
        of this it keeps track of the number of messages sent and reassigns 
        message sub_ids appropriately so that monitoring can keep track of the 
        transaction's state more easily. 

        """
        # If list_type given as a string then attempt to cast it into an 
        # appropriate enum
        if not isinstance(mode, FilelistType):
            try:
                mode = FilelistType[mode]
            except KeyError:
                raise ValueError("mode value invalid, must be a FilelistType "
                                 "enum or a string capabale of being cast to "
                                 f"such (mode={mode})")

        self.log(f"Sending list back to exchange (routing_key = {routing_key})",
                 self.RK_LOG_INFO)
        
        # Get the current state to use in case of retrying with no state set
        try: 
            current_state = State(body_json[self.MSG_DETAILS][self.MSG_STATE])
        except (ValueError, KeyError) as e:
            self.log("Couldn't retrieve current state from message, continuing "
                     "with default state.", self.RK_LOG_WARNING)
            self.log(f"Exception raised during state retrieval: {e}", 
                     self.RK_LOG_DEBUG)
            current_state = self.DEFAULT_STATE

        body_json[self.MSG_DETAILS][self.MSG_RETRY] = False
        if mode == FilelistType.processed:
            # Reset the retries (both transaction-level and file-level) upon 
            # successful completion of processing. 
            trans_retries = Retries.from_dict(body_json)
            trans_retries.reset(save_reasons_fl=save_reasons_fl)
            body_json.update(trans_retries.to_dict())
            for path_details in pathlist:
                path_details.retries.reset(save_reasons_fl=save_reasons_fl)
            if state is None: 
                state = self.DEFAULT_STATE
        elif mode == FilelistType.retry:
            # Delay the retry message depending on how many retries have been 
            # accumulated. All retries in a retry list _should_ be the same so 
            # base it off of the first one.
            if delay is None:
                delay = self.get_retry_delay(pathlist[0].retries.count)
                arrival_time = datetime.now() + timedelta(milliseconds=delay)
                self.log(f"Adding {delay / 1000}s delay to retry. Should be "
                         f"sent at {arrival_time}", self.RK_LOG_DEBUG)
            body_json[self.MSG_DETAILS][self.MSG_RETRY] = True
            if state is None:
                state = current_state
        elif mode == FilelistType.failed:
            state = State.FAILED

        # If necessary values not set at this point then use default values
        if state is None:
            state = self.DEFAULT_STATE
        if delay is None:
            delay = 0

        # Create new sub_id for each extra subrecord created.
        if self.sent_message_count >= 1:
            body_json[self.MSG_DETAILS][self.MSG_SUB_ID] = str(uuid.uuid4())
        
        # Send message to next part of workflow
        body_json[self.MSG_DATA][self.MSG_FILELIST] = pathlist
        body_json[self.MSG_DETAILS][self.MSG_STATE] = state.value
        
        self.publish_message(routing_key, body_json, delay=delay)

        # Send message to monitoring to keep track of state
        # add any warning
        if warning and len(warning) > 0:
            body_json[self.MSG_DETAILS][self.MSG_WARNING] = warning

        monitoring_rk = ".".join([routing_key.split('.')[0], 
                                  self.RK_MONITOR_PUT, 
                                  self.RK_START])
        self.publish_message(monitoring_rk, body_json)
        self.sent_message_count += 1


    def get_retries(self, body: Dict) -> Retries:
        """Retrieve the retries from a message. If no present or inaccessible 
        then add new ones. """
        try: 
            retries = Retries.from_dict(body)
        except KeyError as e:
            self.log(f"Could not retrieve message retry information ({e}). "
                     f"Inserting a new Retries dict back into the message.", 
                     self.RK_LOG_INFO)
            retries = Retries()
            body |= retries.to_dict()
        return retries
    

    def setup_logging(
            self, 
            enable=False, 
            log_level: str = None, 
            log_format: str = None, 
            add_stdout_fl: bool = False, 
            stdout_log_level: str = None, 
            log_files: List[str]=None,
            log_max_bytes: int = None, 
            log_backup_count: int = None,
        ) -> None:
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
            if LOGGING_CONFIG_MAX_BYTES in consumer_logging_conf:
                log_max_bytes = consumer_logging_conf[LOGGING_CONFIG_MAX_BYTES]
            if LOGGING_CONFIG_BACKUP_COUNT in consumer_logging_conf:
                log_backup_count = consumer_logging_conf[LOGGING_CONFIG_BACKUP_COUNT]

        # Allow the hard-coded default deactivation of logging to be overridden 
        # by the consumer-specific logging config
        if not enable:
            return

        return super().setup_logging(
            enable=enable, 
            log_level=log_level, 
            log_format=log_format, 
            add_stdout_fl=add_stdout_fl, 
            stdout_log_level=stdout_log_level, 
            log_files=log_files,
            log_max_bytes=log_max_bytes,
            log_backup_count=log_backup_count,
        )
    
    #######
    # Callback wrappers
    
    def _log_errored_transaction(self, body, exception):
        """Log message which has failed at some point in its callback"""
        if self.print_tracebacks_fl:
            tb = traceback.format_exc()
            self.log("Printing traceback of error: ", self.RK_LOG_DEBUG)
            self.log(tb, self.RK_LOG_DEBUG)
        self.log(
            f"Encountered error in message callback ({exception}), sending"
            " to logger.", self.RK_LOG_ERROR, exc_info=exception
        )
        self.log(
            f"Failed message content: {body}",
            self.RK_LOG_DEBUG
        )

    def _handle_expected_error(self, body: bytes, routing_key: str, 
                               original_error: Any):
        """Handle the workflow of an expected error - attempt to retry the 
        transaction or fail it as apparopriate. Given we don't know exactly what 
        is wrong with the message we need to be quite defensive with error 
        handling here so as to avoid breaking the consumption loop.
        
        """
        # First we log the expected error 
        self._log_errored_transaction(body, original_error)

        # Then we try to parse its source and retry information from the message 
        # body.
        retries = None
        try: 
            # First try to parse message body for retry information
            body_json = json.loads(body)
            retries = Retries.from_dict(body_json)
        except (JSONDecodeError, KeyError) as e:
            self.log("Could not retrieve failed message retry information "
                     f"{e}, will now attempt to fail message in monitoring.", 
                     self.RK_LOG_INFO)            
        try: 
            # Get message source from routing key, if possible
            rk_parts = self.split_routing_key(routing_key)
            rk_source = rk_parts[0]
        except Exception as e:
            self.log("Could not retrieve routing key source, reverting to "
                     "default NLDS value.", self.RK_LOG_WARNING)
            rk_source = self.RK_ROOT

        monitoring_rk = ".".join([rk_source, 
                                  self.RK_MONITOR_PUT,  
                                  self.RK_START])

        if retries is not None and retries.count <= self.max_retries:
            # Retry the job
            self.log(f"Retrying errored job with routing key {routing_key}", 
                     self.RK_LOG_INFO)
            try:
                self._retry_transaction(body_json, retries, routing_key, 
                                        monitoring_rk, original_error)
            except Exception as e:
                self.log(f"Failed attempt to retry transaction that failed "
                         f"during callback. Error: {e}.", self.RK_LOG_WARNING)
                # Fail the job if at any point the attempt to retry fails. 
                self._fail_transaction(body_json, monitoring_rk)
        else:
            # Fail the job
            self._fail_transaction(body_json, monitoring_rk)

    def _fail_transaction(self, body_json: Dict[str, Any], monitoring_rk: str):
        """Attempt to mark transaction as failed in monitoring db"""
        try:                
            # Send message to monitoring to keep track of state
            body_json[self.MSG_DETAILS][self.MSG_STATE] = State.FAILED
            body_json[self.MSG_DETAILS][self.MSG_RETRY] = False
            self.publish_message(monitoring_rk, body_json)
        except Exception as e:
            # If this fails there's not much we can do at this stage...
            # TODO: might be worth figuring out a way of just extracting the 
            # transaction id and failing the job from that? If it's gotten to 
            # this point and failed then 
            self.log("Failed attempt to mark transaction as failed in "
                     "monitoring.", self.RK_LOG_WARNING)
            self.log(f"Exception that arose during attempt to mark job as "
                     f"failed: {e}", self.RK_LOG_DEBUG)
            self.log(f"Message that couldn't be failed: "
                     f"{json.dumps(body_json)}", self.RK_LOG_DEBUG)

    def _retry_transaction(
            self, 
            body_json: Dict[str, Any], 
            retries: Retries, 
            original_rk: str, 
            monitoring_rk: str, 
            error: Exception
        ) -> None:
        """Attempt to retry the message with a retry delay, back to the original 
        routing_key"""              
        # Delay the retry message depending on how many retries have been 
        # accumulated - using simply the transaction-level retries
        retries.increment(reason=f"Exception during callback: {error}")
        body_json.update(retries.to_dict())
        delay = self.get_retry_delay(retries.count)
        self.log(f"Adding {delay / 1000}s delay to retry. Should be sent at"
                 f" {datetime.now() + timedelta(milliseconds=delay)}", 
                 self.RK_LOG_DEBUG)
        body_json[self.MSG_DETAILS][self.MSG_RETRY] = True

        # Send to original routing key (i.e. retry it) with the requisite delay 
        # and also update the monitoring db. 
        self.publish_message(original_rk, body_json, delay=delay)
        self.publish_message(monitoring_rk, body_json)

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
    
    @staticmethod
    def _nacknowledge_message(channel: Channel, delivery_tag: str) -> None:
        """Nacknowledge a message with a basic nack.

        :param channel:         Channel which message came from
        :param delivery_tag:    Message id
        """

        logger.debug(f'Nacking message: {delivery_tag}')
        if channel.is_open:
            channel.basic_nack(delivery_tag=delivery_tag)

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

    def nack_message(self, channel: Channel, delivery_tag: str, 
                     connection: Connection) -> None:
        """Method for nacknowledging a message so that it can be requeued and 
        the next can be fetched. This is called after a consumer callback if, 
        and only if, it returns a False. As in the case of acking, in order to 
        do this thread-safely it is done from within a connection object.  All 
        of the required params come from the standard callback params.

        :param channel:         Callback channel param
        :param delivery_tag:    From the callback method param. eg. 
                                method.delivery_tag
        :param connection:      Connection object from the callback param
        """
        cb = functools.partial(self._nacknowledge_message, channel, delivery_tag)
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
    
    EXPECTED_EXCEPTIONS = (
        ValueError, 
        TypeError, 
        KeyError, 
        PermissionError,
        RabbitRetryError,
        CallbackError,
        JSONDecodeError,
        HTTPError,
        S3Error,
    )

    def _wrapped_callback(self, ch: Channel, method: Method, properties: Header, 
                          body: bytes, connection: Connection) -> None:
        """Wrapper around standard callback function which adds error handling 
        and manual message acknowledgement. All arguments are the same as those 
        in self.callback, i.e. the standard rabbitMQ consumer callback 
        parameters.

        This should be performed on all consumers and should be left untouched 
        in child implementations.
        """
        ack_fl = None 
        # Begin the 
        self.keepalive.start_polling()

        # Wrap callback with a try-except catching a selection of common 
        # errors which can be caught without stopping consumption.
        try:
            ack_fl = self.callback(ch, method, properties, body, connection)
        except self.EXPECTED_EXCEPTIONS as original_error:
            self._handle_expected_error(body, method.routing_key, original_error)
        except Exception as e:
            # NOTE: why are we making this distinction? We can attempt to retry 
            # any message that fails and the worst that can happen is what 
            # already happens now
            self.log(
                f"Unexpected exception encountered! The current list of "
                f"expected exceptions is: {self.EXPECTED_EXCEPTIONS}. Consider "
                f"adding this exception type ({type(e).__name__}) to the list "
                f"and accounting for it more specifically. Now attempting to "
                f"handle message with standard retry/fail mechanism.", 
                self.RK_LOG_WARNING
            )
            self._handle_expected_error(body, method.routing_key, e)
        finally:
            # Only ack message if the message has not been flagged for nacking 
            # in the callback with a `return False`
            if ack_fl is False:
                self.nack_message(ch, method.delivery_tag, connection)
            else:
                self.acknowledge_message(ch, method.delivery_tag, connection)
        
        # Clear the consuming event so the keepalive stops polling the connection
        self.keepalive.stop_polling()


    def _start_callback_threaded(
            self, 
            ch: Channel, 
            method: Method, 
            properties: Header, 
            body: bytes, 
            connection: Connection
        ) -> None:
        """Consumption method which starts the _wrapped_callback() in a new 
        thread so the connection can be kept alive in the main thread. Allows 
        for both long-running tasks and multithreading, though in the case of 
        the latter this may not be as efficient as just scaling out to another 
        consumer. 

        TODO: (2024-02-08) This currently doesn't work but I'll leave it here in 
        case we return to it further down the line. 
        """
        t = thr.Thread(target=self._wrapped_callback, args=(ch, method, 
                                                            properties, 
                                                            body, connection))
        t.start()
        self.threads.append()

            
    def declare_bindings(self) -> None:
        """
        Overridden method from Publisher, additionally declares the queues and 
        queue-exchange bindings outlined in the config file. If no queues were 
        set then the default - generated within __init__ - is used instead. 

        """
        super().declare_bindings()
        for queue in self.queues:
            self.channel.queue_declare(queue=queue.name, durable=True)
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
    def append_route_info(
            cls, 
            body: Dict[str, Any], 
            route_info: str = None
        ) -> Dict:
        if route_info is None: 
            route_info = cls.DEFAULT_REROUTING_INFO
        if cls.MSG_ROUTE in body[cls.MSG_DETAILS]:
            body[cls.MSG_DETAILS][cls.MSG_ROUTE] += route_info
        else:
            body[cls.MSG_DETAILS][cls.MSG_ROUTE] = route_info
        return body
    
    def exit(self, *args):
        raise SigTermError

    def run(self):
        """
        Method to run when thread is started. Creates an AMQP connection
        and sets some exception handling.

        A common exception which occurs is StreamLostError.
        The connection should get reset if that happens.

        :return:
        """

        # set up SigTerm handler
        signal.signal(signal.SIGTERM, self.exit)

        while self.loop:
            self.get_connection()

            try:
                startup_message = f"{self.DEFAULT_QUEUE_NAME} - READY"
                logger.info(startup_message)
                self.channel.start_consuming()

            except KeyboardInterrupt:
                self.loop = False
                break
            
            except SigTermError:
                self.loop = False
                break

            except (StreamLostError, AMQPConnectionError) as e:
                # Log problem
                logger.error('Connection lost, reconnecting', exc_info=e)
                continue

            except Exception as e:
                # Catch all other exceptions and log them as critical. 
                tb = traceback.format_exc()
                self.log(tb, self.RK_LOG_CRITICAL, exc_info=e)
                self.loop = False
                break

        self.channel.stop_consuming()

        # Wait for all threads to complete
        # TODO: what happens if we try to sigterm?
        for t in self.threads:
            t.join()
        
        self.connection.close()

