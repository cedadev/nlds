import uuid
import socket
import os

from retry import retry
import pika
from pika.frame import Method, Header
from pika.channel import Channel
from pika.exceptions import ChannelClosedByBroker

from .publisher import RabbitMQPublisher

class RabbitMQRPCPublisher(RabbitMQPublisher):
    RPC_CONFIG_SECTION = "rpc_publisher"
    RPC_TIME_LIMIT = "time_limit"
    RPC_QUEUE_EXCLUSIVITY = "queue_exclusivity_fl"
    DEFAULT_CONFIG = {
        RPC_TIME_LIMIT: 30, # seconds
        RPC_QUEUE_EXCLUSIVITY: True,
    }

    def __init__(self):
        super().__init__()

        self.response = None
        self.corr_id = None
        self.queue_suffix = 0

        rpc_config = self.DEFAULT_CONFIG

        # Merge rpc config section into default (overriding defaults) if present
        if self.RPC_CONFIG_SECTION in self.whole_config:
            rpc_config = rpc_config | self.whole_config[self.RPC_CONFIG_SECTION]
        
        # Cast to int and error/exit if not 
        try:
            self.time_limit = int(rpc_config[self.RPC_TIME_LIMIT])
        except ValueError:
            raise ValueError(f"time_limit config option must be an integer.")
        try:
            self.q_exclusivity_fl = bool(rpc_config[self.RPC_QUEUE_EXCLUSIVITY])
        except (ValueError, TypeError):
            raise ValueError(f"The {self.RPC_QUEUE_EXCLUSIVITY} config option "
                             "must be True or False.")

    @retry(ChannelClosedByBroker, tries=5, backoff=1, delay=0)
    def declare_bindings(self) -> None:
        # Declare an exclusive queue to receive our reply back on. Here we use
        # the hostname of the machine running the Publisher and the pid of the 
        # thread/process so it is (a) consistent upon redeclaring bindings, and 
        # (b) unique for multiple instances of the server being run concurrently
        
        # Also include a failsafe for appending a number if somehow multiple 
        # queues of the same name are created. 
        queue_name = f"{socket.gethostname()}_{os.getpid()}"
        if self.queue_suffix > 0:
            queue_name = f"{queue_name}_{self.queue_suffix}"

        try:
            result = self.channel.queue_declare(
                queue=queue_name, 
                exclusive=self.q_exclusivity_fl,
            )
            self.callback_queue = result.method.queue

            # Define a basic consumer with auto-acknowledgement of messages
            self.channel.basic_consume(
                queue=self.callback_queue,
                on_message_callback=self.callback,
                auto_ack=True
            )
        except ChannelClosedByBroker as e:
            self.queue_suffix += 1
            raise e

    def callback(self, ch: Channel, method: Method, properties: Header, 
                 body: bytes):
        # Check if message matches our correlation_id and stash the message 
        # contents if so
        if self.corr_id == properties.correlation_id:
            self.response = body

    async def call(self, msg_dict: dict, routing_key: str = 'rpc_queue', time_limit: int = None, correlation_id: str = None):
        if time_limit is None:
            time_limit = self.time_limit
            
        # Create a unique correlation_id to recognise the correct message when 
        # it comes back
        
        if correlation_id is None:
            self.corr_id = str(uuid.uuid4())
        else:
            self.corr_id = correlation_id
            
        self.response = None
        

        self.publish_message(
            routing_key=routing_key,
            msg_dict=msg_dict,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
                expiration=f'{time_limit*1000}'
            ),
            exchange={'name':''}
        )
        self.connection.process_data_events(time_limit=time_limit)
        return self.response
