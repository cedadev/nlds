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

from pika.exceptions import StreamLostError, AMQPConnectionError
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method, Header
from pika.amqp_object import Method
from pika.spec import Channel
from pydantic import BaseModel

from .publisher import RabbitMQPublisher
from ..utils.constants import RABBIT_CONFIG_QUEUE_NAME, RABBIT_CONFIG_QUEUES

logger = logging.getLogger(__name__)


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

    def __init__(self, queue: str = None):
        super().__init__()
        # TODO: Replace all printing with logging
        # TODO: (2021-12-21) Only one queue can be specified at the moment, 
        # should be able to specify multiple queues to subscribe to but this 
        # isn't a priority.
        self.queue = queue
        try:
            if queue is not None:
                # If queue specified then select only that configuration
                if RABBIT_CONFIG_QUEUES in self.config:
                    # Load queue config if it exists in .server_config file.
                    self.queues = [RabbitQueue(**q) for q in self.config[RABBIT_CONFIG_QUEUES] 
                                   if q[RABBIT_CONFIG_QUEUE_NAME] == queue]
                else: 
                    raise ValueError("Not rabbit queues found in config.")
                
                if queue not in [q.name for q in self.queues]:
                    raise ValueError("Requested queue not in configuration.")
            
            else:
                raise ValueError("No queue specified, switching to default config.")
                
        except Exception as e:
            print(str(e))
            print("WARN: Using default queue config - only fit for testing purposes.")
            self.queue = self.DEFAULT_QUEUE_NAME
            self.queues = [RabbitQueue.from_defaults(
                self.DEFAULT_QUEUE_NAME,
                self.DEFAULT_EXCHANGE_NAME, 
                self.DEFAULT_ROUTING_KEY
            )]  

        # Load consumer-specific config
        if self.queue in self.whole_config:
            self.consumer_config = self.whole_config[self.queue]
        else: 
            self.consumer_config = None
    
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
    def split_routing_key(routing_key: str) -> None:
        """
        Method to simply verify and split the routing key into parts. 

        :return: 3-tuple of routing key parts
        """
        rk_parts = routing_key.split('.')
        if len(rk_parts) != 3:
            raise ValueError(f"Routing key ({routing_key}) malformed, should consist of 3 parts.")
        return rk_parts

    @classmethod
    def append_route_info(cls, body: Dict, route_info: str = None):
        if route_info is None: 
            route_info = cls.DEFAULT_REROUTING_INFO
        if cls.MSG_ROUTE in body[cls.MSG_DETAILS]:
            body[cls.MSG_DETAILS][cls.MSG_ROUTE] += route_info
        else:
            body[cls.MSG_DETAILS][cls.MSG_ROUTE] = route_info
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
                startup_message = f"{self.DEFAULT_QUEUE_NAME} - READY"
                logger.info(startup_message)
                print(startup_message)
                self.channel.start_consuming()

            except KeyboardInterrupt:
                self.channel.stop_consuming()
                break

            except (StreamLostError, AMQPConnectionError) as e:
                # Log problem
                logger.error('Connection lost, reconnecting', exc_info=e)
                continue

            except Exception as e:
                logger.critical(e)

                self.channel.stop_consuming()
                break
