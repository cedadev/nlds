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
    DEFAULT_ROUTING_KEY = (f"{RabbitMQConsumer.RK_ROOT}."
                           f"{RabbitMQConsumer.RK_ROUTE}."
                           f"{RabbitMQConsumer.RK_WILD}")
    DEFAULT_REROUTING_INFO = "->NLDS_Q"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
    
    def callback(self, ch: Channel, method: Method, properties: Header, 
                 body: bytes, connection: Connection) -> None:
        # NRM - more comments for the function methods, please!
        # Convert body from bytes to string for ease of manipulation
        body_json = json.loads(body)

        self.log(f"Received {json.dumps(body_json, indent=4)} \nwith " 
                 f"routing_key: {method.routing_key}", self.RK_LOG_INFO)

        self.log(f"Appending rerouting information to message: "
                 f"{self.DEFAULT_REROUTING_INFO} ", self.RK_LOG_DEBUG)
        body_json = self.append_route_info(body_json)

        # Check the routing key is a valid, 3-piece key
        self.log(f"Checking routing_key: {method.routing_key}", 
                 self.RK_LOG_DEBUG)
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError:
            self.log('Routing key inappropriate length, exiting callback.', 
                     self.RK_LOG_ERROR)
            return
        
        # Minimum verification of message contents - checking the two major 
        # sections (details and data) are present 
        for msg_section in (self.MSG_DATA, self.MSG_DETAILS):
            if msg_section not in body_json:
                self.log(f"Invalid message received - missing {msg_section} "
                         f"section. \nExiting callback", self.RK_LOG_ERROR)
                return
        
        msg_data = body_json[self.MSG_DATA]
        msg_details = body_json[self.MSG_DETAILS]

        # If message coming from api-server then stash the original action in 
        # the message details so as not to lose it post-indexing.
        if self.RK_ROUTE == rk_parts[1]:
            msg_details[self.MSG_API_ACTION] = rk_parts[2]

        # If putting then first scan file/filelist
        if self.RK_PUT in rk_parts[2]:
            self.log(f"Sending put command to be indexed", self.RK_LOG_INFO)
            
            new_routing_key = ".".join([self.RK_ROOT, 
                                        self.RK_INDEX, 
                                        self.RK_INITIATE])
            self.publish_and_log_message(new_routing_key, json.dumps(body_json))
        
        elif self.RK_GET in rk_parts[2]:
            # Will probably need to fetch data from catalogue first, but can 
            # bypass if a specific transaction is specified
            # NOTE: in this case we probably still want to the check the 
            # catalogue for whether the file is stored inside, also need to be 
            # able to handle multiple transaction_ids.
            source_transact = None
            if self.MSG_SOURCE_TRANSACTION in msg_details:
                source_transact = msg_details[self.MSG_SOURCE_TRANSACTION]
            else:
                self.log("No 'source_transaction' in message details, message "
                         "malformed or sent in error.", self.RK_LOG_WARNING)

            if source_transact:
                # NOTE: Refactor this (and below) into function?
                self.log(f"Transaction ID specified, passing message to get "
                         "transfer queue", self.RK_LOG_INFO)
                
                # Dig out saved api-action to determine which transfer queue to 
                # send to. 
                try:
                    api_action = body_json[self.MSG_DETAILS][self.MSG_API_ACTION]
                except KeyError:
                    self.log("No api-action in message details, cannot "
                             "determine which transfer queue to send to.", 
                             self.RK_LOG_ERROR)
                    return
                queue = f"{self.RK_TRANSFER}-{api_action}"
                new_routing_key = ".".join([self.RK_ROOT, queue, self.RK_START])
                self.log(f"Sending  message to {queue} queue with routing "
                            f"key {new_routing_key}", self.RK_LOG_INFO)
                self.publish_and_log_message(new_routing_key, 
                                             json.dumps(body_json))
            
            else:
                # Otherwise fetch the relevant data from the catalogue 
                NotImplementedError('Catalogue not implemented yet!')

        # If a task has completed, initiate new tasks
        elif rk_parts[2] == f"{self.RK_COMPLETE}":
            # If index completed then pass file list for transfer and cataloging
            if rk_parts[1] == f"{self.RK_INDEX}":
                self.log(f"Scan successful, passing message to transfer queue", 
                         self.RK_LOG_INFO)
                
                # Dig out saved api-action to determine which transfer queue to 
                # send to. 
                try:
                    api_action = body_json[self.MSG_DETAILS][self.MSG_API_ACTION]
                except KeyError:
                    self.log("No api-action in message details, cannot "
                             "determine which transfer queue to send to.", 
                             self.RK_LOG_ERROR)
                    return
                queue = f"{self.RK_TRANSFER}-{api_action}"
                new_routing_key = ".".join([self.RK_ROOT, queue, self.RK_START])
                self.log(f"Sending  message to {queue} queue with routing "
                            f"key {new_routing_key}", self.RK_LOG_INFO)
                self.publish_and_log_message(new_routing_key, 
                                             json.dumps(body_json))

            # If transfer or catalogue completed then forward confirmation to 
            # monitor
            elif (rk_parts[1] == f"{self.RK_CATALOGUE}" 
                  or rk_parts[1] == f"{self.RK_TRANSFER}"):
                self.log(f"Sending message to {self.RK_MONITOR} queue", 
                         self.RK_LOG_INFO)
                new_routing_key = ".".join([self.RK_ROOT, 
                                            self.RK_MONITOR, 
                                            rk_parts[2]])
                self.publish_and_log_message(new_routing_key, 
                                             json.dumps(body_json))
                                     
        self.log(f"Worker callback complete!", self.RK_LOG_INFO)

    def publish_and_log_message(self, routing_key: str, msg: str, 
                                log_fl=True) -> None:
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