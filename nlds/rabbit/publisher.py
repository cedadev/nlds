# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

import sys
from datetime import datetime
from uuid import UUID
import json
import logging
from typing import List
import pathlib
from collections import namedtuple

import pika
from pika.exceptions import AMQPConnectionError, AMQPHeartbeatTimeout, \
                            AMQPChannelError, AMQPError
from retry import retry

from ..server_config import LOGGING_CONFIG_FILES, LOGGING_CONFIG_STDOUT, load_config, RABBIT_CONFIG_SECTION, \
                            LOGGING_CONFIG_SECTION, LOGGING_CONFIG_LEVEL, \
                            LOGGING_CONFIG_STDOUT_LEVEL, LOGGING_CONFIG_FORMAT, \
                            LOGGING_CONFIG_ENABLE
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

    # Exchange routing key parts – root
    RK_ROOT = "nlds-api"
    RK_WILD = "*"

    # Exchange routing key parts – queues
    RK_INDEX = "index"
    RK_CATALOGUE = "cat"
    RK_MONITOR = "mon"
    RK_TRANSFER = "tran"
    RK_ROUTE = "route"
    RK_LOG = "log"

    # Exchange routing key parts – actions
    RK_INITIATE = "init"
    RK_COMPLETE = "complete"
    RK_FAILED = "failed"

    # Exchange routing key parts – monitoring levels
    RK_LOG_NONE = "none"
    RK_LOG_DEBUG = "debug"
    RK_LOG_INFO = "info"
    RK_LOG_WARNING = "warning"
    RK_LOG_ERROR = "error"
    RK_LOG_CRITICAL = "critical"
    LOG_RKS = (
        RK_LOG_NONE, RK_LOG_DEBUG, RK_LOG_INFO, RK_LOG_WARNING, RK_LOG_ERROR, RK_LOG_CRITICAL
    )
    LOGGER_PREFIX = "nlds."

    # Message json sections
    MSG_DETAILS = "details"
    MSG_TRANSACT_ID = "transaction_id"
    MSG_TIMESTAMP = "timestamp"
    MSG_USER = "user"
    MSG_GROUP = "group"
    MSG_TARGET = "target"
    MSG_ROUTE = "route"
    MSG_ERROR = "error"
    MSG_DATA = "data"
    MSG_FILELIST = "filelist"
    MSG_FILELIST_RETRIES = "retries"
    MSG_LOG_TARGET = "log_target"
    MSG_LOG_MESSAGE = "log_message"

    MSG_TYPE = "type"
    MSG_TYPE_STANDARD = "standard"
    MSG_TYPE_LOG = "log"

    IndexItem = namedtuple("IndexItem", "item retries")

    def __init__(self, setup_logging_fl=False):
        # Get rabbit-specific section of config file
        self.whole_config = load_config()
        self.config = self.whole_config[RABBIT_CONFIG_SECTION]
        
        # Load exchange section of config as this is the only required part for 
        # sending messages
        self.exchange = self.config["exchange"]
      
        self.connection = None
        self.channel = None
        
        if setup_logging_fl:
            self.setup_logging()
    
    @retry(RabbitRetryError, tries=5, delay=1, backoff=2, logger=logger)
    def get_connection(self):
        try:
            if self.connection is None or not (self.connection.is_open and self.channel.is_open):
                # Get the username and password for rabbit
                rabbit_user = self.config["user"]
                rabbit_password = self.config["password"]

                # Start the rabbitMQ connection
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        self.config["server"],
                        credentials=pika.PlainCredentials(rabbit_user, rabbit_password),
                        virtual_host=self.config["vhost"],
                        heartbeat=60
                    )
                )

                # Create a new channel
                channel = connection.channel()
                channel.basic_qos(prefetch_count=1)

                self.connection = connection
                self.channel = channel

                # Declare the exchange config. Also provides a hook for other bindings (e.g. queues) 
                # to be declared in child classes.
                self.declare_bindings()
        except (AMQPError, AMQPChannelError, AMQPConnectionError, AMQPHeartbeatTimeout) as e:
            raise RabbitRetryError(str(e), ampq_exception=e)

    def declare_bindings(self) -> None:
        self.channel.exchange_declare(exchange=self.exchange["name"], 
                                      exchange_type=self.exchange["type"])

    @classmethod
    def create_message(cls, transaction_id: UUID, data: List[str], user: str = None, 
                       group: str = None, target: str = None) -> str:
        """
        Create message to add to rabbit queue. Message is in json format with 
        metadata described in DETAILS and data, i.e. the filelist of interest,
        under DATA. 

        :param str transaction_id:  ID of transaction as provided by fast-api
        :param str data:            file or filelist of interest
        :param str user:            user who sent request
        :param str group:           group that user belongs to 
        :param str target:          target that files are being moved to (only 
                                    valid for PUT, PUTLIST commands)

        :return:    JSON encoded string in the proper format for message passing

        """
        timestamp = datetime.now().isoformat(sep='-')
        # Create initial retry list full of zeroes
        retry_list = [0 for _ in data]
        message_dict = {
            cls.MSG_DETAILS: {
                cls.MSG_TRANSACT_ID: str(transaction_id),
                cls.MSG_TIMESTAMP: timestamp,
                cls.MSG_USER: user,
                cls.MSG_GROUP: group,
                cls.MSG_TARGET: target
            }, 
            cls.MSG_DATA: {
                cls.MSG_FILELIST: data,
                cls.MSG_FILELIST_RETRIES: retry_list
            },
            cls.MSG_TYPE: cls.MSG_TYPE_STANDARD
        }

        return json.dumps(message_dict)

    def publish_message(self, routing_key: str, msg: str) -> None:
        self.channel.basic_publish(
            exchange=self.exchange['name'],
            routing_key=routing_key,
            properties=pika.BasicProperties(content_encoding='application/json'),
            body=msg
        )

    def close_connection(self) -> None:
        self.connection.close()

    _default_logging_conf = {
        LOGGING_CONFIG_ENABLE: True,
        LOGGING_CONFIG_LEVEL: RK_LOG_INFO,
        LOGGING_CONFIG_FORMAT: '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        LOGGING_CONFIG_STDOUT: False,
        LOGGING_CONFIG_STDOUT_LEVEL: RK_LOG_WARNING,
    }
    def setup_logging(self, enable: bool = True, log_level: str = None, log_format: str = None,
                      add_stdout_fl: bool = False, stdout_log_level: str = None,
                      log_files: List[str]=None) -> None:
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
            # Attempt to load config from 'logging' section of .server_config file.
            global_logging_config = self.whole_config[LOGGING_CONFIG_SECTION]

            # Merge with default config dict to ensure all options have a value
            global_logging_config = self._default_logging_conf | global_logging_config
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
                stdout_log_level = global_logging_config[LOGGING_CONFIG_STDOUT_LEVEL]
            sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(getattr(logging, stdout_log_level.upper()))
            sh.setFormatter(formatter)

            logger.addHandler(sh)
            logger.info(f"Standard-out logger set up at {stdout_log_level}")
        
        # If something has been specified in log_files attempt to load it
        if log_files is not None or LOGGING_CONFIG_FILES in global_logging_config:
            try: 
                # Load log_files from server_config if not specified from kwargs
                if log_files is None:
                    log_files = global_logging_config[LOGGING_CONFIG_FILES]
            except KeyError as e:
                logger.warning(f"Failed to load log files from config: {str(e)}")
                return
            
            # For each log file specified make and attach a filehandler with 
            # the same log_level and log_format as specified globally.
            if isinstance(log_files, list):
                for log_file in log_files:
                    try:
                        # Make log file in separate logger
                        fh = logging.FileHandler(log_file)
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
                        logger.warning(f"Failed to create log file for {log_file}: {str(e)}")

    def log(self, log_message: str, log_level: str, target: str, **kwargs) -> None:
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
            logger.error(f"Given log level ({log_level}) not in approved list of logging levels. \n"
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

    @classmethod
    def create_log_message(cls, message: str, target: str, route: str = None) -> bytes:
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

        return json.dumps(message_dict)