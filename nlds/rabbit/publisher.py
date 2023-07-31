# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

import sys
from datetime import datetime, timedelta
import json
import logging
from logging.handlers import TimedRotatingFileHandler
from typing import Dict, List, Any
import pathlib
from collections.abc import Sequence

import pika
from pika.exceptions import AMQPConnectionError, UnroutableError, ChannelWrongStateError
from retry import retry

from ..server_config import (
    load_config, 
    GENERAL_CONFIG_SECTION,
    RABBIT_CONFIG_SECTION, 
    LOGGING_CONFIG_SECTION, 
    LOGGING_CONFIG_ROLLOVER, 
    LOGGING_CONFIG_FILES, 
    LOGGING_CONFIG_STDOUT, 
    LOGGING_CONFIG_LEVEL, 
    LOGGING_CONFIG_STDOUT_LEVEL, 
    LOGGING_CONFIG_FORMAT, 
    LOGGING_CONFIG_ENABLE,
)
from ..errors import RabbitRetryError

logger = logging.getLogger("nlds.root")

class RabbitMQPublisher():
    # Routing key constants
    RK_PUT = "put"
    RK_GET = "get"
    RK_DEL = "del"
    RK_PUTLIST = "putlist"
    RK_GETLIST = "getlist"
    RK_DELLIST = "dellist"
    RK_LIST = "list"
    RK_STAT = "stat"
    RK_FIND = "find"
    RK_META = "meta"

    # Exchange routing key parts – root
    RK_ROOT = "nlds-api"
    RK_WILD = "*"

    # Exchange routing key parts – queues
    RK_INDEX = "index"
    RK_CATALOG = "catalog"
    RK_CATALOG_PUT = "catalog-put"
    RK_CATALOG_GET = "catalog-get"
    RK_CATALOG_DEL = "catalog-del"
    RK_MONITOR = "monitor"
    RK_MONITOR_PUT = "monitor-put"
    RK_MONITOR_GET = "monitor-get"
    RK_TRANSFER = "transfer"
    RK_TRANSFER_PUT = "transfer-put"
    RK_TRANSFER_GET = "transfer-get"
    RK_ARCHIVE = "archive"
    RK_ARCHIVE_PUT = "archive-put"
    RK_ARCHIVE_GET = "archive-get"
    RK_ROUTE = "route"
    RK_LOG = "log"

    # Exchange routing key parts – actions
    RK_INITIATE = "init"
    RK_START = "start"
    RK_COMPLETE = "complete"
    RK_FAILED = "failed"
    RK_REROUTE = "reroute"

    # Exchange routing key parts – monitoring levels
    RK_LOG_NONE = "none"
    RK_LOG_DEBUG = "debug"
    RK_LOG_INFO = "info"
    RK_LOG_WARNING = "warning"
    RK_LOG_ERROR = "error"
    RK_LOG_CRITICAL = "critical"
    LOG_RKS = (
        RK_LOG_NONE, 
        RK_LOG_DEBUG, 
        RK_LOG_INFO, 
        RK_LOG_WARNING, 
        RK_LOG_ERROR, 
        RK_LOG_CRITICAL
    )
    LOGGER_PREFIX = "nlds."

    # Message json sections
    MSG_DETAILS = "details"
    MSG_ID = "id"
    MSG_TRANSACT_ID = "transaction_id"
    MSG_TIMESTAMP = "timestamp"
    MSG_USER = "user"
    MSG_GROUP = "group"
    MSG_TARGET = "target"
    MSG_ROUTE = "route"
    MSG_ERROR = "error"
    MSG_TENANCY = "tenancy"
    MSG_ACCESS_KEY = "access_key"
    MSG_SECRET_KEY = "secret_key"
    MSG_API_ACTION = "api_action"
    MSG_JOB_LABEL = "job_label"
    MSG_DATA = "data"
    MSG_FILELIST = "filelist"
    MSG_FILELIST_ITEMS = "fl_items"
    MSG_FILELIST_RETRIES = "fl_retries"
    MSG_TRANSACTIONS = "transactions"
    MSG_LOG_TARGET = "log_target"
    MSG_LOG_MESSAGE = "log_message"
    MSG_META = "meta"
    MSG_NEW_META = "new_meta"
    MSG_LABEL = "label"
    MSG_TAG = "tag"
    MSG_DEL_TAG = "del_tag"
    MSG_PATH = "path"
    MSG_HOLDING_ID = "holding_id"
    MSG_HOLDING_LIST = "holdings"
    MSG_STATE = "state"
    MSG_SUB_ID = "sub_id"
    MSG_RETRY = "retry"
    MSG_FAILURE = "failure"
    MSG_USER_QUERY = "user_query"
    MSG_GROUP_QUERY = "group_query"
    MSG_RETRY_COUNT_QUERY = "retry_count"
    MSG_RECORD_LIST = "records"
    MSG_WARNING = "warning"
    MSG_TAPE_URL = "tape_url"
    MSG_TAPE_POOL = "tape_pool"
    MSG_TAPE_TAR_PATH = "tape_tar_path"

    MSG_RETRIES = "retries"
    MSG_RETRIES_COUNT = "count"
    MSG_RETRIES_REASONS = "reasons"

    MSG_TYPE = "type"
    MSG_TYPE_STANDARD = "standard"
    MSG_TYPE_LOG = "log"

    # In ascending order: 0 seconds, 30 seconds, 1 minute, 1 hour, 1 day, 5 days
    # All must be in milliseconds.
    RETRY_DELAYS = "retry_delays"
    DEFAULT_RETRY_DELAYS = [
        timedelta(seconds=0).total_seconds() * 1000,          
        timedelta(seconds=30).total_seconds() * 1000,          
        timedelta(minutes=1).total_seconds() * 1000, 
        timedelta(hours=1).total_seconds() * 1000, 
        timedelta(days=1).total_seconds() * 1000, 
        timedelta(days=5).total_seconds() * 1000, 
    ]

    def __init__(self, name="publisher", setup_logging_fl=False):
        # Get rabbit-specific section of config file
        self.whole_config = load_config()
        self.config = self.whole_config[RABBIT_CONFIG_SECTION]
        if GENERAL_CONFIG_SECTION in self.whole_config:
            self.general_config = self.whole_config[GENERAL_CONFIG_SECTION]
        else:
            self.general_config = dict()

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
        try:
            # Do some basic verification of the general retry delays. 
            self.retry_delays = self.general_config[self.RETRY_DELAYS]
            assert (isinstance(self.retry_delays, Sequence) 
                    and not isinstance(self.retry_delays, str))
            assert len(self.retry_delays) > 0
            assert isinstance(self.retry_delays[0], int)
        except (KeyError, TypeError, AssertionError):
            self.retry_delays = self.DEFAULT_RETRY_DELAYS
        
        if setup_logging_fl:
            self.setup_logging()
    
    @retry(RabbitRetryError, tries=5, delay=1, backoff=2, logger=logger)
    def get_connection(self):
        try:
            if (not self.channel or not self.channel.is_open):
                # Get the username and password for rabbit
                rabbit_user = self.config["user"]
                rabbit_password = self.config["password"]

                # Start the rabbitMQ connection
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        self.config["server"],
                        credentials=pika.PlainCredentials(rabbit_user, 
                                                          rabbit_password),
                        virtual_host=self.config["vhost"],
                        heartbeat=60
                    )
                )

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
            logger.error("AMQPConnectionError encountered on attempting to "
                         "establish a connection. Retrying...")
            logger.debug(f"{e}")
            raise RabbitRetryError(str(e), ampq_exception=e)

    def declare_bindings(self) -> None:
        """Go through list of exchanges from config file and declare each. Will 
        also declare delayed exchanges for use in scheduled messaging if the 
        delayed flag is present and activated for a given exchange.
        
        """
        for exchange in self.exchanges:
            if exchange['delayed']:
                args = {"x-delayed-type": exchange["type"]}
                self.channel.exchange_declare(exchange=exchange["name"], 
                                              exchange_type="x-delayed-message",
                                              arguments=args)
            else:
                self.channel.exchange_declare(exchange=exchange["name"], 
                                              exchange_type=exchange["type"])

    @staticmethod
    def verify_exchange(exchange):
        """Verify that an exchange dict defined in the config file is valid. 
        Throws a ValueError if not. 
        
        """
        if ("name" not in exchange or "type" not in exchange 
                or "delayed" not in exchange):
            raise ValueError("Exchange in config file incomplete, cannot "
                             "be declared.")

    def _get_default_properties(self, delay: int = 0) -> pika.BasicProperties:
        return pika.BasicProperties(
            content_encoding='application/json',
            headers={
                "x-delay": delay
            },
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        )

    @retry(RabbitRetryError, tries=2, delay=0, backoff=1, logger=logger)
    def publish_message(self, 
                        routing_key: str, 
                        msg_dict: Dict, 
                        exchange: Dict = None,
                        delay: int = 0, 
                        properties: pika.BasicProperties = None, 
                        mandatory_fl: bool = True,
                        correlation_id: str = None) -> None:
        """Sends a message with the specified routing key to an exchange for 
        routing. If no exchange is provided it will default to the first 
        exchange declared in the server_config. 
        
        An optional delay can be added which will force the message to sit for 
        the specified number of seconds at the exchange before being routed. 
        Note that this only happens if the given (or default if not specified) 
        exchange is declared as a x-delayed-message exchange at start up with 
        the 'delayed' flag. 
        
        This is in essence a light wrapper around the basic_publish method in 
        pika. 
        """
        # add the time stamp to the message here
        msg_dict[self.MSG_TIMESTAMP] = datetime.now().isoformat(sep='-')
        # JSON the message
        msg = json.dumps(msg_dict)

        if not exchange:
            exchange = self.default_exchange
        if not properties:
            properties = self._get_default_properties(delay=delay)
        if delay > 0: 
            # Delayed messages and mandatory acknowledgements are unfortunately 
            # incompatible. For now prioritising delay over the mandatory flag.
            mandatory_fl = False

        if correlation_id:
            properties.correlation_id = correlation_id

        try:
            self.channel.basic_publish(
                exchange=exchange["name"],
                routing_key=routing_key,
                properties=properties,
                body=msg,
                mandatory=mandatory_fl,
            )
        except (AMQPConnectionError, ChannelWrongStateError) as e:
            # For any connection error then reset the connection and try again
            logger.error("AMQPConnectionError encountered on attempting to "
                         "publish a message. Manually resetting and retrying.")
            logger.debug(f"{e}")
            self.connection = None
            self.get_connection()
            raise RabbitRetryError(str(e), ampq_exception=e)
        except UnroutableError as e:
            # For any Undelivered messages attempt to send again
            logger.error("Message delivery was not confirmed, wasn't delivered "
                         f"properly (rk = {routing_key}). Attempting retry...")
            logger.debug(f"{e}")
            raise RabbitRetryError(str(e), ampq_exception=e)

    def get_retry_delay(self, retries: int):
        """Simple convenience function for getting the delay (in seconds) for an 
        indexlist with a given number of retries. Works off of the member 
        variable self.retry_delays, maxing out at its final value i.e. if there 
        are 5 elements in self.retry_delays, and 7 retries requested, then the 
        5th element is returned. 
        """
        retries = min(retries, len(self.retry_delays) - 1)
        return int(self.retry_delays[retries])

    def close_connection(self) -> None:
        self.connection.close()

    _default_logging_conf = {
        LOGGING_CONFIG_ENABLE: True,
        LOGGING_CONFIG_LEVEL: RK_LOG_INFO,
        LOGGING_CONFIG_FORMAT: ("%(asctime)s - %(name)s - "
                                "%(levelname)s - %(message)s"),
        LOGGING_CONFIG_STDOUT: False,
        LOGGING_CONFIG_STDOUT_LEVEL: RK_LOG_WARNING,
        LOGGING_CONFIG_ROLLOVER: "W0"
    }
    def setup_logging(self, enable: bool = True, log_level: str = None, 
                      log_format: str = None, add_stdout_fl: bool = False, 
                      stdout_log_level: str = None, log_files: List[str]=None,
                      log_rollover: str = None) -> None:
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
        # TODO: (2022-03-14) This has gotten a bit unwieldy, might be best to 
        # strip back and keep it simpler.
        # TODO: (2022-03-21) Or move this all to a mixin? 

        # Do not configure logging if not enabled at the internal level (note 
        # this can be overridden by consumer-specific config)
        if not enable:
            return

        try:
            # Attempt to load config from 'logging' section of .server_config 
            # file.
            global_logging_config = self.whole_config[LOGGING_CONFIG_SECTION]

            # Merge with default config dict to ensure all options have a value
            global_logging_config = (self._default_logging_conf 
                                     | global_logging_config)
        except KeyError as e:
            logger.info('Failed to find logging configuration in .server_config'
                        ' file, using defaults instead.')
            global_logging_config = self._default_logging_conf

        # Skip rest of config if logging not enabled at the global level
        if not global_logging_config[LOGGING_CONFIG_ENABLE]:
            return

        # Set logging level, using the kwarg as a priority 
        if log_level is None:
            log_level = global_logging_config[LOGGING_CONFIG_LEVEL]
        logger.setLevel(getattr(logging, log_level.upper()))

        # Add formatting, using the kwarg as a priority
        if log_format is None:
            log_format = global_logging_config[LOGGING_CONFIG_FORMAT]
        sh = logging.StreamHandler()
        sh.setLevel(getattr(logging, log_level.upper()))
        formatter = logging.Formatter(log_format)
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        logger.info(f"Standard-error logger set up at {log_level}")

        # Optionally add stdout printing in addition to default stderr
        if add_stdout_fl or (LOGGING_CONFIG_STDOUT in global_logging_config 
                             and global_logging_config[LOGGING_CONFIG_STDOUT]):
            if stdout_log_level is None:
                stdout_log_level = global_logging_config[
                    LOGGING_CONFIG_STDOUT_LEVEL
                ]
            sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(getattr(logging, stdout_log_level.upper()))
            sh.setFormatter(formatter)

            logger.addHandler(sh)
            logger.info(f"Standard-out logger set up at {stdout_log_level}")
        
        # If something has been specified in log_files attempt to load it
        if (log_files is not None 
                or LOGGING_CONFIG_FILES in global_logging_config):
            try: 
                # Load log_files from server_config if not specified from kwargs
                if log_files is None:
                    log_files = global_logging_config[LOGGING_CONFIG_FILES]
            except KeyError as e:
                logger.warning(f"Failed to load log files from config: "
                               f"{str(e)}")
                return

            # If log files set then see what the rotation time should be
            if (log_rollover is not None 
                or LOGGING_CONFIG_ROLLOVER in global_logging_config):
                try:
                    if log_rollover is None: 
                        log_rollover = global_logging_config[LOGGING_CONFIG_ROLLOVER]
                except KeyError as e:
                    logger.warning(f"Failed to load log rollover from config: "
                                   f"{str(e)}")
                    return
            
            # For each log file specified make and attach a filehandler with 
            # the same log_level and log_format as specified globally.
            if isinstance(log_files, list):
                for log_file in log_files:
                    try:
                        # Make log file in separate logger
                        fh = TimedRotatingFileHandler(
                            log_file,
                            when=log_rollover,
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
                        fh_logger.info(f"{filestem} file logger set up at "
                                       f"{log_level}")

                    except (FileNotFoundError, OSError) as e:
                        # TODO: Should probably do something more robustly with 
                        # this error message, but sending a message to the queue 
                        # at startup seems excessive? 
                        logger.warning(f"Failed to create log file for "
                                       f"{log_file}: {str(e)}")

    def _log(self, log_message: str, log_level: str, target: str, 
            **kwargs) -> None:
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
        :param kwargs:              Optional. Keyword args to pass into the call 
                                    to logging.log() 
        """
        # Check that given log level is appropriate 
        if log_level.lower() not in self.LOG_RKS:
            logger.error(f"Given log level ({log_level}) not in approved list "
                         f"of logging levels. \n"
                         f"One of {self.LOG_RKS} must be used instead.")
            return

        # Check format of given target
        if not (target[:5] == self.LOGGER_PREFIX):
            target = f"{self.LOGGER_PREFIX}{target}"

        # First log message with local logger
        log_level_int = getattr(logging, log_level.upper())
        logger.log(log_level_int, log_message, **kwargs)

        # Then assemble a message to send to the logging consumer
        routing_key = ".".join([self.RK_ROOT, self.RK_LOG, log_level.lower()])
        message = self.create_log_message(log_message, target)
        self.publish_message(routing_key, message)
    
    def log(self, log_message: str, log_level: str, target: str = None, 
            **kwargs) -> None:
        # Attempt to log to publisher's name
        if not target:
            target = self.name
        self._log(log_message, log_level, target, **kwargs)

    @classmethod
    def create_log_message(cls, message: str, target: str, 
                           route: str = None) -> Dict[str, Any]:
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
        timestamp = datetime.now().isoformat(sep='-')
        message_dict = {
            cls.MSG_DETAILS: {
                cls.MSG_TIMESTAMP: timestamp,
                cls.MSG_LOG_TARGET: target,
                cls.MSG_ROUTE: route,
            }, 
            cls.MSG_DATA: {
                cls.MSG_LOG_MESSAGE: message,
            },
            cls.MSG_TYPE: cls.MSG_TYPE_LOG
        }

        return message_dict