from configparser import ConfigParser
from datetime import datetime
from typing import List, Union
import os
import uuid

import pika

from .nlds_setup import RABBIT_CONFIG_FILE_LOCATION

class RabbitMQConnection():

    def __init__(self, config: str = RABBIT_CONFIG_FILE_LOCATION):
        self.conf = ConfigParser()
        self.conf.read(os.path.abspath(f"{config}"))

        self.queue = self.conf.get('server', 'rabbit_queue')

        self.connection = None
        self.channel = None
        
    
    def get_connection(self):
        if self.connection is None or not (self.connection.is_open and self.channel.is_open):
            # Get the username and password for rabbit
            rabbit_user = self.conf.get('server', 'rabbit_user')
            rabbit_password = self.conf.get('server', 'rabbit_password')

            # Start the rabbitMQ connection
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    self.conf.get('server', 'rabbit_server'),
                    credentials=pika.PlainCredentials(rabbit_user, rabbit_password),
                    virtual_host=self.conf.get('server', 'rabbit_vhost'),
                    heartbeat=300
                )
            )

            # Create a new channel
            channel = connection.channel()

            # Declare relevant queue
            channel.queue_declare(queue=self.queue)

            self.connection = connection
            self.channel = channel

    @staticmethod
    def create_message(transaction_id: uuid, action: str, contents: str) -> str:
        """
        Create message to add to rabbit queue. Message matches format of deposit logs.
        date_time:transaction_id:action:message_contents

        :param transaction_id: ID of transaction as provided by fast-api
        :param action: Action constant (GET, PUT etc.)
        :return: string containing essential information for processor
        """
        time = datetime.now().isoformat(sep='-')

        return f"{time}:{transaction_id}:{action}:[{contents}]"

    def publish_message(self, msg: str) -> None:
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue,
            body=msg
        )

    def close_connection(self) -> None:
        self.connection.close()
