"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '07 Dec 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

import json

# Typing imports
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

# NLDS imports
from nlds.rabbit.consumer import RabbitMQConsumer

class NLDSWorkerConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "nlds_q"
    DEFAULT_ROUTING_KEY = f"{RabbitMQConsumer.RK_ROOT}.{RabbitMQConsumer.RK_ROUTE}.{RabbitMQConsumer.RK_WILD}"
    DEFAULT_REROUTING_INFO = "->NLDS_Q"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
    
    def callback(self, ch: Channel, method: Method, properties: Header, 
                 body: bytes, connection: Connection):
        # NRM - more comments for the function methods, please!
        # Convert body from bytes to string for ease of manipulation
        body_json = json.loads(body)

        self.log(f"Received {json.dumps(body_json, indent=4)} \nwith " 
                 f"routing_key: {method.routing_key}", self.RK_LOG_INFO)
        self.log(f"Appending rerouting information to message: {self.DEFAULT_REROUTING_INFO} ", 
                 self.RK_LOG_DEBUG)
        body_json = self.append_route_info(body_json)

        self.log(f"Checking routing_key and re-routing", self.RK_LOG_DEBUG)
        self.log(method.routing_key, self.RK_LOG_DEBUG)
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError:
            self.log('Routing key inappropriate length, exiting callback.', 
                     self.RK_LOG_ERROR)
            return
        
        # If putting then first scan file/filelist
        if self.RK_PUT in rk_parts[2]:
            self.log(f"Sending put command to be indexed", self.RK_LOG_INFO)
            
            new_routing_key = ".".join([self.RK_ROOT, self.RK_INDEX, self.RK_INITIATE])
            self.publish_and_log_message(new_routing_key, json.dumps(body_json))
        
        # If a task has completed, initiate new tasks
        elif rk_parts[2] == f"{self.RK_COMPLETE}":
            # If index completed then pass file list for transfer and cataloging
            if rk_parts[1] == f"{self.RK_INDEX}":
                self.log(f"Scan successful, pass message back to transfer and " 
                         "cataloging queues", self.RK_LOG_INFO)
                for queue in [self.RK_TRANSFER]:
                    self.log(f"Sending  message to {queue} queue", self.RK_LOG_INFO)
                    new_routing_key = ".".join([self.RK_ROOT, queue, self.RK_INITIATE])
                    self.publish_and_log_message(new_routing_key, json.dumps(body_json))

            # If transfer or catalogue completed then forward confirmation to 
            # monitor
            elif rk_parts[1] == f"{self.RK_CATALOGUE}" or rk_parts[1] == f"{self.RK_TRANSFER}":
                self.log(f"Sending message to {self.RK_MONITOR} queue", self.RK_LOG_INFO)
                new_routing_key = ".".join([self.RK_ROOT, self.RK_MONITOR, rk_parts[2]])
                self.publish_and_log_message(new_routing_key, json.dumps(body_json))
                                     
        self.log(f"Worker callback complete!", self.RK_LOG_INFO)

    def publish_and_log_message(self, routing_key: str, msg: str, log_fl=True) -> None:
        """
        Wrapper around original publish message to additionally send message to 
        logging queue. Useful for debugging purposes to be able to see the 
        content being managed by the worker. 
        """
        self.publish_message(routing_key, msg)

        if log_fl:  
            # Additionally send same message to logging with debug priority.
            self.log(msg, self.RK_LOG_DEBUG)


def main():
    consumer = NLDSWorkerConsumer()
    consumer.run()

if __name__ == "__main__":
    main()