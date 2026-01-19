# encoding: utf-8
"""
publisher.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import sys
from datetime import datetime
import json
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Any
import pathlib
import zlib
import base64
from copy import copy
from retry import retry
from threading import Timer

import pika
from pika.exceptions import AMQPConnectionError, UnroutableError, ChannelWrongStateError

import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG
import nlds.server_config as CFG

from nlds.rabbit.keepalive import KeepaliveDaemon
from nlds.errors import RabbitRetryError

logger = logging.getLogger("nlds.root")


class RabbitMQPublisher:

    def __init__(self, name="publisher", setup_logging_fl=False):
        # Get rabbit-specific section of config file
        self.whole_config = CFG.load_config()
        self.config = self.whole_config[CFG.RABBIT_CONFIG_SECTION]
        if CFG.GENERAL_CONFIG_SECTION in self.whole_config:
            self.general_config = self.whole_config[CFG.GENERAL_CONFIG_SECTION]
        else:
            self.general_config = dict()

        if (
            CFG.RABBIT_CONFIG_COMPRESS in self.config
            and self.config[CFG.RABBIT_CONFIG_COMPRESS]
        ):
            self.compress = True
        else:
            self.compress = False

        # Set name for logging purposes
        self.name = name

        # Load exchange section of config as this is the only required part for
        # sending messages
        self.exchanges = self.config["exchange"]

        # If multiple exchanges given then verify each and assign the first as a
        # default exchange.
        if not isinstance(self.exchanges, list):
            self.exchanges = [self.exchanges]
        for exchange in self.exchanges:
            self.verify_exchange(exchange)
        self.default_exchange = self.exchanges[0]

        self.connection = None
        self.channel = None
        self.heartbeat = self.config.get(CFG.RABBIT_CONFIG_HEARTBEAT) or 300
        self.timeout = self.config.get(CFG.RABBIT_CONFIG_TIMEOUT) or 1800
        # 30 mins in s
        self.keepalive = None

        # setup the logger
        if setup_logging_fl:
            self.setup_logging()

    @retry(RabbitRetryError, tries=-1, delay=1, backoff=2, max_delay=60, logger=logger)
    def get_connection(self):
        try:
            if not self.channel or not self.channel.is_open:
                # Get the username and password for rabbit
                rabbit_user = self.config["user"]
                rabbit_password = self.config["password"]

                # Kill any daemon threads before we make a new one for the new
                # connection
                if self.keepalive:
                    self.keepalive.kill()

                # Start the rabbitMQ connection
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        self.config["server"],
                        credentials=pika.PlainCredentials(rabbit_user, rabbit_password),
                        virtual_host=self.config["vhost"],
                        heartbeat=self.heartbeat,
                        blocked_connection_timeout=self.timeout,
                    )
                )
                self.keepalive = KeepaliveDaemon(connection, self.heartbeat)
                self.keepalive.start()

                # Create a new channel with basic qos
                channel = connection.channel()
                channel.basic_qos(prefetch_count=1)
                channel.confirm_delivery()

                self.connection = connection
                self.channel = channel

                # Declare the exchange config. Also provides a hook for other
                # bindings (e.g. queues) to be declared in child classes.
                self.declare_bindings()
        except (AMQPConnectionError, ChannelWrongStateError) as e:
            logger.error(
                "AMQPConnectionError encountered on attempting to "
                "establish a connection. Retrying..."
            )
            logger.debug(f"{type(e).__name__}: {e}")
            raise RabbitRetryError(str(e), ampq_exception=e)

    def declare_bindings(self) -> None:
        """Go through list of exchanges from config file and declare each."""
        for exchange in self.exchanges:
            self.channel.exchange_declare(
                exchange=exchange["name"], exchange_type=exchange["type"]
            )

    @staticmethod
    def verify_exchange(exchange):
        """Verify that an exchange dict defined in the config file is valid.
        Throws a ValueError if not.

        """
        if "name" not in exchange or "type" not in exchange:
            raise ValueError("Exchange in config file incomplete, cannot be declared.")

    def _get_default_properties(self) -> pika.BasicProperties:
        return pika.BasicProperties(
            content_encoding="application/json",
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        )

    def _serialize(self, msg_dict: dict[str, str], indent: int | None = None) -> str:
        """Serialize the message payload by dumping it to JSON"""
        # copy the message as we are altering it and want to keep the original unchanged
        msg_dict_out = copy(msg_dict)
        # get whether we should compress the message or not
        if self.compress:
            # if we should then we compress the DATA part of the message, but leave the
            # DETAILS part uncompressed
            # we also put a flag in the DETAILS part to say the message is compressed
            # need to copy the msg_dict otherwise the input dictionary will be
            # compressed as well
            msg_dict_out[MSG.DETAILS][MSG.COMPRESS] = True
            # dump DATA part of dictionary to string, convert to bytes (in ascii
            # format), then compress the string and reassign to DATA
            # using level 1 for speed and we see most of the advantage by just using
            # any compression level
            byte_string = json.dumps(msg_dict_out[MSG.DATA]).encode("ascii")
            msg_dict_out[MSG.DATA] = base64.b64encode(
                zlib.compress(byte_string, level=1)
            ).decode("ascii")
            logger.debug(
                f"Compressing message, original size: {len(byte_string)}, "
                f" compressed size: {len(msg_dict_out[MSG.DATA])}"
            )
        else:
            logger.debug("Not compressing message!!!")

        return json.dumps(msg_dict_out, indent=indent)

    @retry(RabbitRetryError, tries=-1, delay=1, backoff=2, max_delay=60, logger=logger)
    def _publish_in_thread(
        self,
        routing_key: str,
        body: str,
        exchange: str = None,
        properties: pika.BasicProperties = None,
        mandatory_fl: bool = True,
    ):
        """
        Publish to the queue in the thread.
        Opens its own connection and channel.
        Replicates a very shortened and specialized part of "get_connection"
        """
        try:
            # Start the rabbitMQ connection
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    self.config["server"],
                    credentials=pika.PlainCredentials(
                        self.config["user"], self.config["password"]
                    ),
                    virtual_host=self.config["vhost"],
                    heartbeat=self.heartbeat,
                    blocked_connection_timeout=self.timeout,
                )
            )

            # Create a new channel with basic qos
            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)
            channel.confirm_delivery()
            # add the exchanges from the config
            for e in self.exchanges:
                channel.exchange_declare(exchange=e["name"], exchange_type=e["type"])
            channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                properties=properties,
                body=body,
                mandatory=mandatory_fl,
            )
            channel.close()
            logger.debug(f"Sending message with key: {routing_key}")
        except (AMQPConnectionError, ChannelWrongStateError) as e:
            logger.error(
                "AMQPConnectionError encountered on attempting to "
                "establish a connection. Retrying..."
            )
            logger.debug(f"{type(e).__name__}: {e}")
            raise RabbitRetryError(str(e), ampq_exception=e)

    @retry(RabbitRetryError, tries=-1, delay=1, backoff=2, max_delay=60, logger=logger)
    def publish_message(
        self,
        routing_key: str,
        msg_dict: Dict,
        exchange: Dict = None,
        delay: int = 0,
        properties: pika.BasicProperties = None,
        mandatory_fl: bool = True,
        correlation_id: str = None,
    ) -> None:
        """Sends a message with the specified routing key to an exchange for
        routing. If no exchange is provided it will default to the first
        exchange declared in the server_config.

        This is in essence a light wrapper around the basic_publish method in
        pika.
        """
        # add the time stamp to the message here
        msg_dict[MSG.TIMESTAMP] = datetime.now().isoformat(sep="-")
        # JSON the message
        msg = self._serialize(msg_dict)

        if not exchange:
            exchange = self.default_exchange
        if not properties:
            properties = self._get_default_properties()

        if correlation_id:
            properties.correlation_id = correlation_id

        try:
            if delay > 0:
                kwargs = {
                    "exchange": exchange["name"],
                    "routing_key": routing_key,
                    "properties": properties,
                    "body": msg,
                    "mandatory_fl": mandatory_fl,
                }
                t = Timer(delay, self._publish_in_thread, args=None, kwargs=kwargs)
                t.start()
            else:
                self.channel.basic_publish(
                    exchange=exchange["name"],
                    routing_key=routing_key,
                    properties=properties,
                    body=msg,
                    mandatory=mandatory_fl,
                )
                logger.debug(f"Sending message with key: {routing_key}")

        except (AMQPConnectionError, ChannelWrongStateError) as e:
            # For any connection error then reset the connection and try again
            logger.error(
                "AMQPConnectionError encountered on attempting to "
                "publish a message. Manually resetting and retrying."
            )
            logger.debug(f"{e}")
            self.connection = None
            self.get_connection()
            raise RabbitRetryError(str(e), ampq_exception=e)
        except UnroutableError as e:
            # For any Undelivered messages attempt to send again
            logger.error(
                "Message delivery was not confirmed, wasn't delivered "
                f"properly (rk = {routing_key})."
            )
            logger.debug(f"{type(e).__name__}: {e}")
            # NOTE: don't reraise in this case, can cause an infinite loop as
            # the message will never be sent.
            # raise RabbitRetryError(str(e), ampq_exception=e)

    def close_connection(self) -> None:
        self.connection.close()

    _default_logging_conf = {
        CFG.LOGGING_CONFIG_ENABLE: True,
        CFG.LOGGING_CONFIG_LEVEL: RK.LOG_INFO,
        CFG.LOGGING_CONFIG_FORMAT: (
            "%(asctime)s - %(name)s - " "%(levelname)s - %(message)s"
        ),
        CFG.LOGGING_CONFIG_STDOUT: False,
        CFG.LOGGING_CONFIG_STDOUT_LEVEL: RK.LOG_WARNING,
        CFG.LOGGING_CONFIG_MAX_BYTES: 16 * 1024 * 1024,
        CFG.LOGGING_CONFIG_BACKUP_COUNT: 0,
    }

    def setup_logging(
        self,
        enable: bool = True,
        log_level: str = None,
        log_format: str = None,
        add_stdout_fl: bool = False,
        stdout_log_level: str = None,
        log_files: List[str] = None,
        log_max_bytes: int = None,
        log_backup_count: int = None,
    ) -> None:
        """
        Sets up logging for a publisher (i.e. the nlds-api server) using a set
        number of configuration options from the logging interface. Each of
        the configuration options are able to be overridden by kwargs, allowing
        for child classes (i.e. consumers) to implement their own settings.

        Allows the creation of stderr and stdout handlers under the generic
        global logger, and file specific handlers/loggers under the name of each
        output file - intended to be used for tracking each consumer's output on
        the logging consumer.

        :param bool enable:             Whether to activate the logging
                                        functionality.
        :param str log_format:          The format string of the logging output,
                                        as per instructions in logging docs.
                                        Controls all logging outputs (stderr,
                                        stdout, files).
        :param str log_level:           The logging level of the global logger
                                        (applies to only the stderr stream)
        :param bool add_stdout_fl:      Boolean flag for controlling whether the
                                        global logger also prints to stdout.
        :param str stdout_log_level:    Logging level of the stdout logging
                                        stream
        :param list[str] log_files:     List of files to write logging output
                                        to, with each made in its own logger
                                        object referencable by the file name.

        """
        # Do not configure logging if not enabled at the internal level (note
        # this can be overridden by consumer-specific config)
        if not enable:
            return

        try:
            # Attempt to load config from 'logging' section of .server_config
            # file.
            global_logging_config = self.whole_config[CFG.LOGGING_CONFIG_SECTION]

            # Merge with default config dict to ensure all options have a value
            global_logging_config = self._default_logging_conf | global_logging_config
        except KeyError as e:
            logger.info(
                "Failed to find logging configuration in .server_config"
                " file, using defaults instead."
            )
            global_logging_config = self._default_logging_conf

        # Skip rest of config if logging not enabled at the global level
        if not global_logging_config[CFG.LOGGING_CONFIG_ENABLE]:
            return

        # Set logging level, using the kwarg as a priority
        if log_level is None:
            log_level = global_logging_config[CFG.LOGGING_CONFIG_LEVEL]
        logger.setLevel(getattr(logging, log_level.upper()))

        # Add formatting, using the kwarg as a priority
        if log_format is None:
            log_format = global_logging_config[CFG.LOGGING_CONFIG_FORMAT]
        sh = logging.StreamHandler()
        sh.setLevel(getattr(logging, log_level.upper()))
        formatter = logging.Formatter(log_format)
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        logger.info(f"Standard-error logger set up at {log_level}")

        # Optionally add stdout printing in addition to default stderr
        if add_stdout_fl or (
            CFG.LOGGING_CONFIG_STDOUT in global_logging_config
            and global_logging_config[CFG.LOGGING_CONFIG_STDOUT]
        ):
            if stdout_log_level is None:
                stdout_log_level = global_logging_config[
                    CFG.LOGGING_CONFIG_STDOUT_LEVEL
                ]
            sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(getattr(logging, stdout_log_level.upper()))
            sh.setFormatter(formatter)

            logger.addHandler(sh)
            logger.info(f"Standard-out logger set up at {stdout_log_level}")

        # If something has been specified in log_files attempt to load it
        if log_files is not None or CFG.LOGGING_CONFIG_FILES in global_logging_config:
            try:
                # Load log_files from server_config if not specified from kwargs
                if log_files is None:
                    log_files = global_logging_config[CFG.LOGGING_CONFIG_FILES]
            except KeyError as e:
                logger.warning(f"Failed to load log files from config: " f"{str(e)}")
                return

            # If log files set then see what the rotation bytes should be
            if (
                log_max_bytes is not None
                or CFG.LOGGING_CONFIG_MAX_BYTES in global_logging_config
            ):
                try:
                    if log_max_bytes is None:
                        log_max_bytes = global_logging_config[
                            CFG.LOGGING_CONFIG_MAX_BYTES
                        ]
                except KeyError as e:
                    logger.warning(
                        f"Failed to load log max bytes from config: " f"{str(e)}"
                    )
                    return

            # If log files set then see what the number of backups should be
            if (
                log_backup_count is not None
                or CFG.LOGGING_CONFIG_BACKUP_COUNT in global_logging_config
            ):
                try:
                    if log_backup_count is None:
                        log_backup_count = global_logging_config[
                            CFG.LOGGING_CONFIG_BACKUP_COUNT
                        ]
                except KeyError as e:
                    logger.warning(
                        f"Failed to load log backup count from " f"config: {str(e)}"
                    )
                    return

            # For each log file specified make and attach a filehandler with
            # the same log_level and log_format as specified globally.
            if isinstance(log_files, list):
                for log_file in log_files:
                    try:
                        # Make log file in separate logger
                        fh = RotatingFileHandler(
                            log_file,
                            maxBytes=log_max_bytes,
                            backupCount=log_backup_count,
                        )
                        # Use the same log_level and formatter as the base
                        fh.setLevel(getattr(logging, log_level.upper()))
                        fh.setFormatter(formatter)

                        # Get a name with which to reference the logger by
                        # taking the filename and removing any file extension
                        filestem = pathlib.Path(log_file).stem
                        fh_logger = logging.getLogger(f"nlds.{filestem}")
                        fh_logger.addHandler(fh)
                        fh_logger.setLevel(getattr(logging, log_level.upper()))

                        # Write out a startup message
                        fh_logger.info(f"{filestem} file logger set up at {log_level}")

                    except (FileNotFoundError, OSError) as e:
                        # TODO: Should probably do something more robustly with
                        # this error message, but sending a message to the queue
                        # at startup seems excessive?
                        logger.warning(
                            f"Failed to create log file for " f"{log_file}: {str(e)}"
                        )

    def _log(
        self,
        log_message: str,
        log_level: str,
        target: str,
        remote: bool = True,
        **kwargs,
    ) -> None:
        """
        Catch-all function to log a message, both sending it to the local logger
        and sending a message to the exchange en-route to the logger
        microservice.

        :param str log_message:     The message for the log event (i.e. what to
                                    feed into logger.info() etc.)
        :param str log_level:       The log level for the new log event. Must be
                                    one of the standard logging levels (see
                                    python logging docs).
        :param str target:          The intended target log on the logging
                                    microservice. Must be one of the configured
                                    logging handlers
        :param bool remote:         Log with the remote logger via message
        :param kwargs:              Optional. Keyword args to pass into the call
                                    to logging.log()
        """
        # Check that given log level is appropriate
        if log_level.lower() not in RK.LOG_RKS:
            logger.error(
                f"Given log level ({log_level}) not in approved list "
                f"of logging levels. \n"
                f"One of {RK.LOG_RKS} must be used instead."
            )
            return

        # Check format of given target
        if not (target[:5] == RK.LOGGER_PREFIX):
            target = f"{RK.LOGGER_PREFIX}{target}"

        # First log message with local logger
        log_level_int = getattr(logging, log_level.upper())
        logger.log(log_level_int, log_message, **kwargs)

        # Then assemble a message to send to the logging consumer, if remote set
        if remote:
            routing_key = ".".join([RK.ROOT, RK.LOG, log_level.lower()])
            message = self.create_log_message(log_message, target)
            self.publish_message(routing_key, message)

    def log(
        self,
        log_message: str,
        log_level: str,
        target: str = None,
        body_json: str = None,
        remote: bool = True,
        **kwargs,
    ) -> None:
        # Attempt to log to publisher's name
        if not target:
            target = self.name
        # convert string json to nice formatted json and append to message
        if body_json:
            log_message += f"\n{json.dumps(body_json, indent=4)}\n"
        # set remote to false for LOG_INFO and LOG_WARNING
        if log_level == RK.LOG_INFO or log_level == RK.LOG_WARNING:
            remote = False
        self._log(log_message, log_level, target, remote, **kwargs)

    @classmethod
    def create_log_message(
        cls, message: str, target: str, route: str = None
    ) -> Dict[str, Any]:
        """
        Create logging message to send to rabbit exchange. Message is, as with
        the standard message, in json format with metadata described in DETAILS
        and data, i.e. the log message, under DATA.

        :param str message:     ID of transaction as provided by fast-api
        :param str target:      The target log file to be written to, must be
                                set to the name of a particular microservice,
                                usually the one which instigated the log event
        :param str route:       Route that the message has taken. Optional, will
                                be set to target if not specifed.

        :return:    JSON encoded string in the proper format for message passing

        """
        if route is None:
            route = target.upper()
        timestamp = datetime.now().isoformat(sep="-")
        message_dict = {
            MSG.DETAILS: {
                MSG.TIMESTAMP: timestamp,
                MSG.LOG_TARGET: target,
                MSG.ROUTE: route,
            },
            MSG.DATA: {
                MSG.LOG_MESSAGE: message,
            },
            MSG.TYPE: MSG.TYPE_LOG,
        }

        return message_dict
