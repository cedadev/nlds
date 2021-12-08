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

import pika

from ..utils.constants import RABBIT_CONFIG_SECTION
from ..server_config import load_config


class RabbitMQPublisher():

    def __init__(self):
        # Get rabbit-specific section of config file
        whole_config = load_config()
        self.config = whole_config[RABBIT_CONFIG_SECTION]

        # Load exchange section of config as this is the only required part for 
        # sending messages
        self.exchange = self.config["exchange"]
      
        self.connection = None
        self.channel = None
        
    def get_connection(self):
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

    def declare_bindings(self) -> None:
        self.channel.exchange_declare(exchange=self.exchange["name"], exchange_type=self.exchange["type"])

    @staticmethod
    def create_message(transaction_id: UUID, action: str, contents: str) -> str:
        """
        Create message to add to rabbit queue. Message matches format of deposit logs.
        date_time:transaction_id:action:message_contents

        :param transaction_id: ID of transaction as provided by fast-api
        :param action: Action constant (GET, PUT etc.)
        :return: string containing essential information for processor
        """
        time = datetime.now().isoformat(sep='-')

        return f"{time}:{transaction_id}:{action}:[{contents}]"

    def publish_message(self, routing_key: str, msg: str) -> None:
        self.channel.basic_publish(
            exchange=self.exchange['name'],
            routing_key=routing_key,
            body=msg
        )

    def close_connection(self) -> None:
        self.connection.close()
