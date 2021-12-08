"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '07 Dec 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

# Typing imports
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

# NLDS imports
from nlds.rabbit.consumer import RabbitMQConsumer
from nlds.utils.constants import PUT
from utils.constants import CATALOGUE, COMPLETE, INDEX, INITIATE, MONITOR, ROOT, TRANSFER, TRIAGE, LOG_INFO 

class NLDSWorkerConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "nlds_q"
    DEFAULT_ROUTING_KEY = f"{TRIAGE}.*.*"
    DEFAULT_ROUTING_INFO = "->NLDS_Q->Exchange"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
    
    def callback(self, ch: Channel, method: Method, properties: Header, body: bytes, connection: Connection):
        # Convert body from bytes to string for ease of manipulation
        body = body.decode("utf-8")

        print(f" [x] Received {body} ({method.routing_key})")
        print(f" [x] Appending rerouting information to message: {self.DEFAULT_ROUTING_INFO} ")

        print(f" [x]  Checking routing_key and re-routing")
        try:
            rk_parts = self.verify_routing_key(method.routing_key)
        except ValueError:
            print(' [XXX] Routing key inappropriate length, exiting callback.')
            return
        
        # Edit routing key when reinserting message into exchange to avoid infinite loop
        rk_parts[0] = ROOT

        # If putting then first scan file/filelist
        if PUT in rk_parts[1].upper():
            rk_parts[1] = INDEX
            print(f" [x]  Sending put command to be indexed")
            new_routing_key = ".".join(rk_parts)
            self.publish_message(new_routing_key, f"{body}{self.DEFAULT_ROUTING_INFO}")
        # If index complete then pass for transfer and cataloging
        elif rk_parts[1] == f"{INDEX}" and rk_parts[2] == f"{COMPLETE}":
            print(f" [x] Scan successful, pass message back to transfer and cataloging queues")

            for queue in [TRANSFER, CATALOGUE]:
                print(f" [x]  Sending to {queue} queue")
                new_rk_parts = [ROOT, queue, INITIATE]
                new_routing_key = ".".join(new_rk_parts)
                # For prototyping purposes append additional message trail info.
                self.publish_message(new_routing_key, f"{body}{self.DEFAULT_ROUTING_INFO}")        

    def publish_message(self, routing_key: str, msg: str) -> None:
        """
        Wrapper around original publish message to additionally send message to monitoring
        queue. 
        """
        super().publish_message(routing_key, msg)

        # Additionally send same message to monitoring.
        rk_parts = routing_key.split(".")
        rk_parts[1] = MONITOR
        rk_parts[2] = LOG_INFO
        new_routing_key = ".".join(rk_parts)
        super().publish_message(new_routing_key, msg)

if __name__ == "__main__":
    consumer = NLDSWorkerConsumer()
    consumer.run()