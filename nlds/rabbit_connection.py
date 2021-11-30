# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from datetime import datetime
from typing import List, Union
import os
from uuid import UUID

import pika
from pika import exchange_type

from .utils.constants import RABBIT_CONFIG_SECTION
from .server_config import load_config


class RabbitMQConnection():

    def __init__(self):
        # Get rabbit-specific section of config file
        whole_config = load_config()
        self.config = whole_config[RABBIT_CONFIG_SECTION]

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

            # Declare relevant exchanges
            channel.exchange_declare(exchange=self.exchange["name"], exchange_type=self.exchange["type"])

            self.connection = connection
            self.channel = channel

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
