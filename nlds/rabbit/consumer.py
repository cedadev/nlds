# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '07 Dec 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

import functools
from abc import ABC, abstractmethod
import logging
from typing import Dict, List

from pika.exceptions import StreamLostError
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method, Header
from pika.amqp_object import Method
from pika.spec import Channel
from pydantic import BaseModel

from nlds.rabbit.publisher import RabbitMQPublisher
from nlds.utils.constants import RABBIT_CONFIG_QUEUES, DETAILS
logger = logging.getLogger()


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
            bindings=[RabbitQEBinding(exchange=exchange, routing_key=routing_key)]
        )


class RabbitMQConsumer(ABC, RabbitMQPublisher):
    DEFAULT_QUEUE_NAME = "test_q"
    DEFAULT_ROUTING_KEY = "test"
    DEFAULT_EXCHANGE_NAME = "test_exchange"
    DEFAULT_REROUTING_INFO = "->"

    def __init__(self, queue=None):
        super().__init__()

        # Load queue config if it exists in .server_config file.
        if RABBIT_CONFIG_QUEUES in self.config:
            self.queues = [RabbitQueue(**q) for q in self.config[RABBIT_CONFIG_QUEUES]]
            # If queue specified then select only that configuration
            if queue and queue in self.queues:
                self.queues = self.queues.pop(queue)
            else: 
                # TODO: Replace with logging
                print("Requested queue not in configuration.")
        else:
            print("WARN: Using default queue config - only fit for testing purposes.")
            self.queues = [RabbitQueue.from_defaults(
                self.DEFAULT_QUEUE_NAME,
                self.DEFAULT_EXCHANGE_NAME, 
                self.DEFAULT_ROUTING_KEY
            )]    
    
    @abstractmethod
    def callback(self, ch: Channel, method: Method, properties: Header, body: bytes, 
                 connection: Connection):
        pass

    def declare_bindings(self) -> None:
        """
        Overridden method from Publisher, additionally declares the queues and queue-exchange 
        bindings outlined in the config file. If no queues were set then the default - generated 
        within __init__, is used instead. 

        """
        super().declare_bindings()
        for queue in self.queues:
            self.channel.queue_declare(queue=queue.name)
            for binding in queue.bindings:
                self.channel.queue_bind(exchange=binding.exchange, 
                                        queue=queue.name, 
                                        routing_key=binding.routing_key)
            # Apply callback to all queues
            wrapped_callback = functools.partial(self.callback, connection=self.connection)
            self.channel.basic_consume(queue=queue.name, 
                                       on_message_callback=wrapped_callback, 
                                       auto_ack=True)
    
    @staticmethod
    def verify_routing_key(routing_key: str) -> None:
        """
        Method to simply verify and split the routing key into parts. 

        :return: 3-tuple of routing key parts
        """
        rk_parts = routing_key.split('.')
        if len(rk_parts) != 3:
            raise ValueError(f"Routing key ({routing_key}) malformed, should consist of 3 parts.")
        return rk_parts

    def append_route_info(self, body: Dict, route_info: str = None):
        if route_info is None: 
            route_info = self.DEFAULT_REROUTING_INFO
        if 'route' in body[DETAILS]:
            body[DETAILS]['route'] += route_info
        else:
            body[DETAILS]['route'] = route_info
        return body
    
    def run(self):
        """
        Method to run when thread is started. Creates an AMQP connection
        and sets some exception handling.

        A common exception which occurs is StreamLostError.
        The connection should get reset if that happens.

        :return:
        """

        while True:
            self.get_connection()

            try:
                logger.info('READY')
                print('READY')
                self.channel.start_consuming()

            except KeyboardInterrupt:
                self.channel.stop_consuming()
                break

            except StreamLostError as e:
                # Log problem
                logger.error('Connection lost, reconnecting', exc_info=e)
                continue

            except Exception as e:
                logger.critical(e)

                self.channel.stop_consuming()
                break
