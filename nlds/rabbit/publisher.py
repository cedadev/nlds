# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from datetime import datetime
from uuid import UUID
import json
import logging

import pika
from pika.exceptions import AMQPConnectionError, AMQPHeartbeatTimeout, AMQPChannelError, AMQPError
from retry import retry

from ..utils.constants import RABBIT_CONFIG_SECTION
from ..server_config import load_config
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

    # Exchange routing key parts – actions
    RK_INITIATE = "init"
    RK_COMPLETE = "complete"

    # Exchange routing key parts – monitoring levels
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

    def __init__(self):
        # Get rabbit-specific section of config file
        self.whole_config = load_config()
        self.config = self.whole_config[RABBIT_CONFIG_SECTION]
        
        # Load exchange section of config as this is the only required part for 
        # sending messages
        self.exchange = self.config["exchange"]
      
        self.connection = None
        self.channel = None
    
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
                       group: str = str, target: str = None) -> str:
        """
        Create message to add to rabbit queue. Message is in json format with 
        metadata described in DETAILS and data, i.e. the filelist of interest,
        under DATA. 

        :param transaction_id: ID of transaction as provided by fast-api
        :param data:    (str)   file or filelist of interest
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
