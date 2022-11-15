import uuid
import socket

import pika
from pika.frame import Method, Header
from pika.channel import Channel
from pika.connection import Connection

from .publisher import RabbitMQPublisher

class RabbitMQRPCPublisher(RabbitMQPublisher):

    def __init__(self):
        super().__init__()

        self.response = None
        self.corr_id = None

    def declare_bindings(self) -> None:
        # Declare an exclusive queue to receive our reply back on. Here we use
        # the hostname of the machine running the Publisher so it is 
        # (a) consistent upon redeclaring bindings, and (b) unique for multiple 
        # instances of the server being run concurrently.
        result = self.channel.queue_declare(
            queue=socket.gethostname(), 
            exclusive=True
        )
        self.callback_queue = result.method.queue

        # Define a basic consumer with auto-acknowledgement of messages
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.callback,
            auto_ack=True
        )

    def callback(self, ch: Channel, method: Method, properties: Header, 
                 body: bytes):
        # Check if message matches our correlation_id and stash the message 
        # contents if so
        if self.corr_id == properties.correlation_id:
            self.response = body

    async def call(self, msg_dict: dict, routing_key: str = 'rpc_queue'):
        self.response = None
        # Create a unique correlation_id to recognise the correct message when 
        # it comes back
        self.corr_id = str(uuid.uuid4())

        self.publish_message(
            routing_key=routing_key,
            msg_dict=msg_dict,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            exchange={'name':''}
        )
        self.connection.process_data_events(time_limit=None)
        return self.response
