# encoding: utf-8
"""
consumer.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "07 Dec 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import functools
from abc import ABC, abstractmethod
import logging
import traceback
from typing import Dict, List, Any
import pathlib as pth
from hashlib import md5
from uuid import UUID, uuid4
import signal
import threading as thr
import json
import zlib
import base64

from pika.exceptions import StreamLostError, AMQPConnectionError
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method, Header
from pika.amqp_object import Method
from pika.spec import Channel
from pydantic import BaseModel

import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG
from nlds.rabbit.state import State
from nlds.rabbit.publisher import RabbitMQPublisher as RMQP
import nlds.server_config as CFG
from nlds.details import PathDetails
from nlds.errors import MessageError

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
            bindings=[RabbitQEBinding(exchange=exchange, routing_key=routing_key)],
        )


class SigTermError(Exception):
    pass


def deserialize(body: str) -> dict:
    """Deserialize the message body by calling JSON loads and decompressing the
    message if necessary."""
    body_dict = json.loads(body)
    # check whether the DATA section is serialized
    if MSG.COMPRESS in body_dict[MSG.DETAILS] and body_dict[MSG.DETAILS][MSG.COMPRESS]:
        # data is in a b64 encoded ascii string - need to convert to bytes (in
        # ascii format before decompressing and loading into json
        try:
            byte_string = body_dict[MSG.DATA].encode("ascii")
        except AttributeError:
            logger.error(
                "DATA part of message was not compressed, despite compressed flag being"
                " set in message"
            )
        else:
            decompressed_string = zlib.decompress(base64.b64decode(byte_string))
            body_dict[MSG.DATA] = json.loads(decompressed_string)
            logger.debug(
                f"Decompressing message, compressed size {len(byte_string)}, "
                f" actual size {len(decompressed_string)}"
            )
        # specify that the message is now decompressed, in case it gets passed through
        # deserialize again
        body_dict[MSG.DETAILS][MSG.COMPRESS] = False
    return body_dict


class RabbitMQConsumer(ABC, RMQP):
    DEFAULT_QUEUE_NAME = "test_q"
    DEFAULT_ROUTING_KEY = "test"
    DEFAULT_EXCHANGE_NAME = "test_exchange"
    DEFAULT_REROUTING_INFO = "->"

    DEFAULT_CONSUMER_CONFIG: Dict[str, Any] = dict()

    # The state associated with finishing the consumer, must be set but can be
    # overridden
    DEFAULT_STATE = State.ROUTING

    def __setup_queues(self, queue: str = None):
        # TODO: (2021-12-21) Only one queue can be specified at the moment,
        # should be able to specify multiple queues to subscribe to but this
        # isn't a priority.
        self.name = queue
        try:
            if queue is not None:
                # If queue specified then select only that configuration
                if CFG.RABBIT_CONFIG_QUEUES in self.config:
                    # Load queue config if it exists in .server_config file.
                    self.queues = [
                        RabbitQueue(**q)
                        for q in self.config[CFG.RABBIT_CONFIG_QUEUES]
                        if q[CFG.RABBIT_CONFIG_QUEUE_NAME] == queue
                    ]
                else:
                    raise ValueError("No rabbit queues found in config.")

                if queue not in [q.name for q in self.queues]:
                    raise ValueError(f"Requested queue {queue} not in configuration.")

            else:
                raise ValueError("No queue specified, switching to default " "config.")

        except ValueError as e:
            raise Exception(e)

    def __init__(self, queue: str = None, setup_logging_fl=False):
        super().__init__(name=queue, setup_logging_fl=False)
        self.loop = True

        self.__setup_queues(queue)

        # Load consumer-specific config
        if self.name in self.whole_config:
            self.consumer_config = self.whole_config[self.name]
        else:
            self.consumer_config = self.DEFAULT_CONSUMER_CONFIG

        # (re)Declare the pathlists here to make them available without having
        # to pass them through every function call.
        self.completelist: List[PathDetails] = []
        self.failedlist: List[PathDetails] = []

        # Controls default behaviour of logging when certain exceptions are
        # caught in the callback.
        self.print_tracebacks_fl = True

        # List of active threads created by consumption process
        self.threads = []

        # Set up the logging and pass through constructor parameter
        self.setup_logging(enable=setup_logging_fl)

    def _is_system_status_check(self, body_json: Dict[str, Any], properties) -> bool:
        """Check whether the body_json contains a message to check for system status"""
        # This checks if the message was for a system status check
        try:
            api_method = body_json[MSG.DETAILS][MSG.API_ACTION]
        except KeyError:
            api_method = None

        # If received system test message, reply to it (this is for system status check)
        if api_method == RK.SYSTEM_STAT:
            # if (
            #     properties.correlation_id is not None
            #     and properties.correlation_id != self.channel.consumer_tags[0]
            # ):
            if (
                properties.correlation_id
                and self.channel.consumer_tags[0] not in properties.correlation_id
            ):
                return False

            if (body_json["details"]["ignore_message"]) == True:
                return False
            else:
                self.publish_message(
                    properties.reply_to,
                    msg_dict=body_json,
                    exchange={"name": ""},
                    correlation_id=properties.correlation_id,
                )
            return True
        return False

    def reset(self) -> None:
        self.completelist.clear()
        self.failedlist.clear()

    def load_config_value(self, config_option: str, path_listify_fl: bool = False):
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
                    return_val_list = self.consumer_config[config_option]
                    # Make sure returned value is a list and not a string
                    # Note: it can't be any other iterable because it's loaded
                    # from a json
                    if not isinstance(return_val_list, list):
                        raise MessageError("Return value is not a valid list")
                    return_val = [pth.Path(item) for item in return_val_list]
            except KeyError:
                self.log(
                    f"Invalid value for {config_option} in config file. "
                    f"Using default value instead.",
                    RK.LOG_WARNING,
                )

        return return_val

    def parse_filelist(self, body_json: dict) -> List[PathDetails]:
        """Convert flat list from message json into list of PathDetails objects
        and then check it is, in fact, a list
        """
        try:
            filelist = [
                PathDetails.from_dict(pd_dict)
                for pd_dict in list(body_json[MSG.DATA][MSG.FILELIST])
            ]
        except TypeError as e:
            self.log(
                "Failed to reformat list into PathDetails objects. Filelist in "
                "message does not appear to be in the correct format.",
                RK.LOG_ERROR,
            )
            raise e
        # de-duplicate the filelist
        filelist = self.dedup_filelist(filelist)
        return filelist

    def dedup_filelist(self, filelist: List[PathDetails]) -> List[PathDetails]:
        """De-duplicate filelist"""
        new_filelist = []
        pathlist = []
        for pd in filelist:
            if not pd.original_path in pathlist:
                new_filelist.append(pd)
                pathlist.append(pd.original_path)

        return new_filelist

    def create_sub_id(self, filelist: List[PathDetails]) -> List[PathDetails]:
        """Sub id is now created by hashing the paths from the filelist"""
        if filelist != []:
            filenames = [f.original_path for f in filelist]
            filelist_hash = md5("".join(filenames).encode()).hexdigest()
            sub_id = UUID(filelist_hash)
        else:
            sub_id = uuid4()
        return str(sub_id)

    def send_pathlist(
        self,
        pathlist: List[PathDetails],
        routing_key: str,
        body_json: Dict[str, Any],
        state: State = None,
        warning: List[str] = None,
        delay=0,
    ) -> None:
        """Convenience function which sends the given list of PathDetails
        objects to the exchange with the given routing key and message body.
        Mode specifies what to put into the log message.

        Additionally forwards transaction state info on to the monitor. As part
        of this it keeps track of the number of messages sent and reassigns
        message sub_ids appropriately so that monitoring can keep track of the
        transaction's state more easily.

        """
        # monitoring routing
        monitoring_rk = ".".join([routing_key.split(".")[0], RK.MONITOR_PUT, RK.START])
        # shouldn't send empty pathlist
        if len(pathlist) == 0:
            raise MessageError("Pathlist is empty")
        # If necessary values not set at this point then use default values
        if state is None:
            state = self.DEFAULT_STATE

        # NRM 03/09/2025 - the sub_id is now the hash of the pathlist
        c_sub_id = body_json[MSG.DETAILS][MSG.SUB_ID]
        sub_id = self.create_sub_id(pathlist)
        if sub_id != c_sub_id:
            self.log(
                f"Changing sub id from {c_sub_id} to {sub_id} with pathlist {pathlist}",
                RK.LOG_DEBUG,
            )
            # send a splitting message for the old sub id, as it has been split into
            # sub messagews
            body_json[MSG.DETAILS][MSG.STATE] = state.SPLIT.value
            self.publish_message(monitoring_rk, body_json, delay=delay)
            # reassign the sub_id
            body_json[MSG.DETAILS][MSG.SUB_ID] = sub_id

        body_json[MSG.DATA][MSG.FILELIST] = pathlist
        body_json[MSG.DETAILS][MSG.STATE] = state.value

        self.publish_message(routing_key, body_json, delay=delay)

        # Send message to monitoring to keep track of state
        # add any warning
        if warning and len(warning) > 0:
            body_json[MSG.DETAILS][MSG.WARNING] = warning

        # added the delay back in for the PREPARE method, but now works differently
        self.publish_message(monitoring_rk, body_json, delay=delay)

    def send_complete(
        self,
        routing_key: str,
        body_json: Dict[str, Any],
    ):
        body_json[MSG.DETAILS][MSG.STATE] = State.COMPLETE
        monitoring_rk = ".".join([routing_key.split(".")[0], RK.MONITOR_PUT, RK.START])
        self.publish_message(monitoring_rk, body_json)

    def setup_logging(
        self,
        enable=False,
        log_level: str = None,
        log_format: str = None,
        add_stdout_fl: bool = False,
        stdout_log_level: str = None,
        log_files: List[str] = None,
        log_max_bytes: int = None,
        log_backup_count: int = None,
    ) -> None:
        """
        Override of the publisher method which allows consumer-specific logging
        to take precedence over the general logging configuration.
        """
        # TODO: (2022-03-01) This is quite verbose and annoying to extend.
        if CFG.LOGGING_CONFIG_SECTION in self.consumer_config:
            consumer_logging_conf = self.consumer_config[CFG.LOGGING_CONFIG_SECTION]
            if CFG.LOGGING_CONFIG_ENABLE in consumer_logging_conf:
                enable = consumer_logging_conf[CFG.LOGGING_CONFIG_ENABLE]
            if CFG.LOGGING_CONFIG_LEVEL in consumer_logging_conf:
                log_level = consumer_logging_conf[CFG.LOGGING_CONFIG_LEVEL]
            if CFG.LOGGING_CONFIG_FORMAT in consumer_logging_conf:
                log_format = consumer_logging_conf[CFG.LOGGING_CONFIG_FORMAT]
            if CFG.LOGGING_CONFIG_STDOUT in consumer_logging_conf:
                add_stdout_fl = consumer_logging_conf[CFG.LOGGING_CONFIG_STDOUT]
            if CFG.LOGGING_CONFIG_STDOUT_LEVEL in consumer_logging_conf:
                stdout_log_level = consumer_logging_conf[
                    CFG.LOGGING_CONFIG_STDOUT_LEVEL
                ]
            if CFG.LOGGING_CONFIG_FILES in consumer_logging_conf:
                log_files = consumer_logging_conf[CFG.LOGGING_CONFIG_FILES]
            if CFG.LOGGING_CONFIG_MAX_BYTES in consumer_logging_conf:
                log_max_bytes = consumer_logging_conf[CFG.LOGGING_CONFIG_MAX_BYTES]
            if CFG.LOGGING_CONFIG_BACKUP_COUNT in consumer_logging_conf:
                log_backup_count = consumer_logging_conf[
                    CFG.LOGGING_CONFIG_BACKUP_COUNT
                ]

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
            self.log("Printing traceback of error: ", RK.LOG_DEBUG)
            self.log(tb, RK.LOG_DEBUG)
        self.log(
            f"Encountered error in message callback ({exception}), sending"
            " to logger.",
            RK.LOG_ERROR,
            exc_info=exception,
        )
        self.log(f"Failed message content: {body}", RK.LOG_DEBUG)

    @staticmethod
    def _acknowledge_message(channel: Channel, delivery_tag: str) -> None:
        """Acknowledge a message with a basic ack. This is the bare minimum
        requirement for an acknowledgement according to rabbit protocols.

        :param channel:         Channel which message came from
        :param delivery_tag:    Message id
        """

        logger.debug(f"Acknowledging message: {delivery_tag}")
        if channel.is_open:
            channel.basic_ack(delivery_tag)

    @staticmethod
    def _nacknowledge_message(channel: Channel, delivery_tag: str) -> None:
        """Nacknowledge a message with a basic nack.

        :param channel:         Channel which message came from
        :param delivery_tag:    Message id
        """

        logger.debug(f"Nacking message: {delivery_tag}")
        if channel.is_open:
            channel.basic_nack(delivery_tag=delivery_tag)

    def acknowledge_message(
        self, channel: Channel, delivery_tag: str, connection: Connection
    ) -> None:
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

    def nack_message(
        self, channel: Channel, delivery_tag: str, connection: Connection
    ) -> None:
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

    def _deserialize(self, body: bytes) -> dict[str, str]:
        """Deserialize the message body by calling JSON loads and decompressing the
        message if necessary."""
        return deserialize(body)

    @abstractmethod
    def callback(
        self,
        ch: Channel,
        method: Method,
        properties: Header,
        body: bytes,
        connection: Connection,
    ) -> None:
        """Standard consumer callback function as defined by rabbitMQ, with the
        standard callback parameters of Channel, Method, Header, Body (in bytes)
        and Connection.

        This is the working method of a consumer, i.e. it does the job the
        consumer has been designed to do, and so must be implemented and
        overridden by any child consumer classes.
        """
        NotImplementedError

    def _wrapped_callback(
        self,
        ch: Channel,
        method: Method,
        properties: Header,
        body: bytes,
        connection: Connection,
    ) -> None:
        """Wrapper around standard callback function which adds error handling
        and manual message acknowledgement. All arguments are the same as those
        in self.callback, i.e. the standard rabbitMQ consumer callback
        parameters.

        This should be performed on all consumers and should be left untouched
        in child implementations.
        """
        self.keepalive.start_polling()

        # Wrap callback with a try-except catching a selection of common
        # errors which can be caught without stopping consumption.
        # NRM - this has changed to stop on all exceptions!
        # NRM - change to acknowledge the message straight away.  If it fails with an
        # exception then resend the message
        try:
            self.acknowledge_message(ch, method.delivery_tag, connection)
            self.log(
                f"Acknowledged message with routing key {method.routing_key}.",
                RK.LOG_INFO,
            )
            self.callback(ch, method, properties, body, connection)
        except Exception as e:
            self.log(
                f"Unhandled exception occurred in callback.  Requeuing message.",
                RK.LOG_INFO,
            )
            tb = traceback.format_exc()
            self.log(tb, RK.LOG_INFO, exc_info=e)
            self.channel.basic_publish(
                exchange=method.exchange,
                routing_key=method.routing_key,
                properties=properties,
                body=body,
                mandatory=True,
            )

        # Clear the consuming event so the keepalive stops polling the connection
        self.keepalive.stop_polling()

    def _start_callback_threaded(
        self,
        ch: Channel,
        method: Method,
        properties: Header,
        body: bytes,
        connection: Connection,
    ) -> None:
        """Consumption method which starts the _wrapped_callback() in a new
        thread so the connection can be kept alive in the main thread. Allows
        for both long-running tasks and multithreading, though in the case of
        the latter this may not be as efficient as just scaling out to another
        consumer.

        TODO: (2024-02-08) This currently doesn't work but I'll leave it here in
        case we return to it further down the line.
        """
        t = thr.Thread(
            target=self._wrapped_callback,
            args=(ch, method, properties, body, connection),
        )
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
                self.channel.queue_bind(
                    exchange=binding.exchange,
                    queue=queue.name,
                    routing_key=binding.routing_key,
                )
            # Apply callback to all queues
            wrapped_callback = functools.partial(
                self._wrapped_callback, connection=self.connection
            )
            self.channel.basic_consume(
                queue=queue.name, on_message_callback=wrapped_callback
            )

    @staticmethod
    def split_routing_key(routing_key: str) -> None:
        """
        Method to simply verify and split the routing key into parts.

        :return: 3-tuple of routing key parts
        """
        rk_parts = routing_key.split(".")
        if len(rk_parts) != 3:
            raise ValueError(
                f"Routing key ({routing_key}) malformed, should " "consist of 3 parts."
            )
        return rk_parts

    @classmethod
    def append_route_info(cls, body: Dict[str, Any], route_info: str = None) -> Dict:
        if route_info is None:
            route_info = cls.DEFAULT_REROUTING_INFO
        if MSG.ROUTE in body[MSG.DETAILS]:
            body[MSG.DETAILS][MSG.ROUTE] += route_info
        else:
            body[MSG.DETAILS][MSG.ROUTE] = route_info
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
        # set up SigHup handler
        signal.signal(signal.SIGHUP, self.exit)

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
                logger.error("Connection lost, reconnecting", exc_info=e)
                continue

            except Exception as e:
                # Catch all other exceptions and log them as critical.
                tb = traceback.format_exc()
                self.log(tb, RK.LOG_CRITICAL, exc_info=e)
                self.loop = False
                break

        self.channel.stop_consuming()

        # Wait for all threads to complete
        # TODO: what happens if we try to sigterm?
        for t in self.threads:
            t.join()

        self.connection.close()
