# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from distutils.log import debug
import sys
from datetime import datetime
from uuid import UUID
import json
import logging
from typing import List

import pika
from pika.exceptions import AMQPConnectionError, AMQPHeartbeatTimeout, \
                            AMQPChannelError, AMQPError
from retry import retry

from ..server_config import LOGGING_CONFIG_FILES, LOGGING_CONFIG_STDOUT, load_config, RABBIT_CONFIG_SECTION, \
                            LOGGING_CONFIG_SECTION, LOGGING_CONFIG_LEVEL, \
                            LOGGING_CONFIG_STDOUT_LEVEL, LOGGING_CONFIG_FORMAT, \
                            LOGGING_CONFIG_ENABLE
from ..errors import RabbitRetryError

logger = logging.getLogger(__name__)

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

    # Exchange routing key parts – monitoring levels
    RK_LOG_NONE = "none"
    RK_LOG_INFO = "info"
    RK_LOG_WARNING = "warn"
    RK_LOG_ERROR = "err"
    RK_LOG_DEBUG = "debug"
    RK_LOG_CRITICAL = "critical"

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
    def create_message(cls, transaction_id: UUID, data: str, user: str = None, 
                       group: str = None, target: str = None) -> str:
        """
        Create message to add to rabbit queue. Message is in json format with 
        metadata described in DETAILS and data, i.e. the filelist of interest,
        under DATA. 

        :param transaction_id: ID of transaction as provided by fast-api
        :param data:        (str)   file or filelist of interest
        :param user:        (str)   user who sent request
        :param group:       (str)   group that user belongs to 
        :param target:      (str)   target that files are being moved to (only 
                                    valid for PUT, PUTLIST commands)

        :return:    JSON encoded string in the proper format for message passing

        """
        timestamp = datetime.now().isoformat(sep='-')
        message_dict = {
            cls.MSG_DETAILS: {
                cls.MSG_TRANSACT_ID: str(transaction_id),
                cls.MSG_TIMESTAMP: timestamp,
                cls.MSG_USER: user,
                cls.MSG_GROUP: group,
                cls.MSG_TARGET: target
            }, 
            cls.MSG_DATA: {
                cls.MSG_FILELIST: data
            }
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
        LOGGING_CONFIG_STDOUT_LEVEL: RK_LOG_INFO,
    }
    def setup_logging(self, enable=True, log_level: str = None, log_format: str = None,
                      add_stdout_fl: bool = False, stdout_log_level: str = None,
                      log_files: List[str]=None) -> None:
        """
        Sets up logging for a publisher (i.e. the nlds-api server) with each of 
        the configuration options able to be overridden by kwargs.
             
        """
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

        print(global_logging_config)

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

        # Optionally add stdout printing in addition to default stderr
        if add_stdout_fl or (LOGGING_CONFIG_STDOUT in global_logging_config 
                             and global_logging_config[LOGGING_CONFIG_STDOUT]):
            if stdout_log_level is None:
                stdout_log_level = global_logging_config[LOGGING_CONFIG_STDOUT_LEVEL]
            sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(getattr(logging, stdout_log_level.upper()))
            sh.setFormatter(formatter)

            logger.addHandler(sh)
        
        # If something has been specified in log_files attempt to load it
        if log_files is not None or LOGGING_CONFIG_FILES in global_logging_config:
            try: 
                # Load log_files from server_config if not specified from kwargs
                if log_files is None:
                    log_files = global_logging_config[LOGGING_CONFIG_FILES]
            except KeyError as e:
                logger.warning(f"Failed to laod log files from config: {str(e)}")
                return
            
            # For each log file specified make and attach a filehandler with 
            # the same log_level and log_format as specified globally.
            if isinstance(log_files, list):
                for log_file in log_files:
                    try:
                        # Make log file in current directory
                        fh = logging.FileHandler(log_file)
                        fh.setLevel(getattr(logging, log_level.upper()))
                        fh.setFormatter(formatter)
                        logger.addHandler(fh)
                    except Exception as e:
                        # TODO: Should probably do something more robustly with 
                        # this error message, but sending a message to the queue 
                        # at startup seems excessive? 
                        logger.warning(f"Failed to create log files: {str(e)}")

        print(logger.handlers)