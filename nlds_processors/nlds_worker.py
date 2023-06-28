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
from nlds.rabbit.consumer import RabbitMQConsumer, State

class NLDSWorkerConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "nlds_q"
    DEFAULT_ROUTING_KEY = (f"{RabbitMQConsumer.RK_ROOT}.",
                           f"{RabbitMQConsumer.RK_ROUTE}.",
                           f"{RabbitMQConsumer.RK_WILD}")
    DEFAULT_REROUTING_INFO = "->NLDS_Q"
    DEFAULT_STATE = State.ROUTING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)


    def _process_message(self, method: Method, body: bytes, properties: Header) -> tuple[list, dict]:
        """Process the message to get the routing key parts, message data
        and message details"""
        # Convert body from bytes to string for ease of manipulation
        body_json = json.loads(body)

        self.log(f"Received {json.dumps(body_json, indent=4)} \nwith " 
                 f"routing_key: {method.routing_key}", self.RK_LOG_INFO)
        
        
        # This checks if the message was for a system status check
        try:
            api_method = body_json[self.MSG_DETAILS][self.MSG_API_ACTION]
        except KeyError:
            self.log(f"Message did not contain api_method", self.RK_LOG_INFO)
            api_method = None
           
        
        # If recieved system test message, reply to it (this is for system status check)
        if api_method == "system_stat":
            self.publish_message(
                properties.reply_to,
                msg_dict=body_json,         #body_json important here
                exchange={'name': ''},
                correlation_id=properties.correlation_id
            )
            return
        

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
        return rk_parts, body_json


    def _process_rk_put(self, body_json: dict) -> None:
        self.log(f"Sending put command to be indexed", self.RK_LOG_INFO)
        
        new_routing_key = ".".join([self.RK_ROOT, 
                                    self.RK_INDEX, 
                                    self.RK_INITIATE])
        self.publish_and_log_message(new_routing_key, body_json)

        # Do initial monitoring update to ensure that a subrecord at ROUTING is 
        # created before the first job 
        self.log(f"Updating monitor", self.RK_LOG_INFO)
        new_routing_key = ".".join([self.RK_ROOT, 
                                    self.RK_MONITOR_PUT, 
                                    self.RK_START])
        body_json[self.MSG_DETAILS][self.MSG_STATE] = State.ROUTING.value
        self.publish_and_log_message(new_routing_key, body_json)


    def _process_rk_get(self, body_json: dict) -> None:
        # forward to catalog_get
        queue = f"{self.RK_CATALOG_GET}"
        new_routing_key = ".".join([self.RK_ROOT, 
                                    queue, 
                                    self.RK_START])
        self.log(f"Sending  message to {queue} queue with routing "
                 f"key {new_routing_key}", self.RK_LOG_INFO)
        self.publish_and_log_message(new_routing_key, body_json)

        # Do initial monitoring update to ensure that a subrecord at ROUTING is 
        # created before the first job 
        self.log(f"Updating monitor", self.RK_LOG_INFO)
        new_routing_key = ".".join([self.RK_ROOT, 
                                    self.RK_MONITOR_PUT, 
                                    self.RK_START])
        body_json[self.MSG_DETAILS][self.MSG_STATE] = State.ROUTING.value
        self.publish_and_log_message(new_routing_key, body_json)


    def _process_rk_list(self, body_json: dict) -> None:
        # forward to catalog_get
        # NOTE: Is this needed now we're doing RPCs from the api-server?
        queue = f"{self.RK_CATALOG_GET}"
        new_routing_key = ".".join([self.RK_ROOT, queue, self.RK_LIST])        
        self.log(f"Sending  message to {queue} queue with routing "
                 f"key {new_routing_key}", self.RK_LOG_INFO)
        self.publish_and_log_message(new_routing_key, body_json)


    def _process_rk_index_complete(self, body_json: dict) -> None:
        # forward to catalog-put on the catalog_q
        self.log(f"Index successful, sending file list for cataloguing.", 
                 self.RK_LOG_INFO)

        queue = f"{self.RK_CATALOG_PUT}"
        new_routing_key = ".".join([self.RK_ROOT, queue,
                                    self.RK_START])
        self.log(f"Sending  message to {queue} queue with routing "
                 f"key {new_routing_key}", self.RK_LOG_INFO)
        self.publish_and_log_message(new_routing_key, body_json)


    def _process_rk_catalog_put_complete(self, body_json: dict) -> None:
        self.log(f"Catalog successful, sending filelist for transfer", 
                 self.RK_LOG_INFO)

        queue = f"{self.RK_TRANSFER_PUT}"
        new_routing_key = ".".join([self.RK_ROOT, 
                                    queue, 
                                    self.RK_START])
        self.log(f"Sending  message to {queue} queue with routing "
                 f"key {new_routing_key}", self.RK_LOG_INFO)
        self.publish_and_log_message(new_routing_key, body_json)


    def _process_rk_transfer_put_complete(self, body_json: dict) -> None:
        # Nothing happens after a successful transfer anymore, so we leave this 
        # empty in case any future messages are required (queueing archive for 
        # example)
        pass


    def _process_rk_transfer_put_failed(self, body_json: dict) -> None:
        self.log(f"Transfer unsuccessful, sending failed files back to catalog "
                  "for deletion", 
                 self.RK_LOG_INFO)

        queue = f"{self.RK_CATALOG_DEL}"
        new_routing_key = ".".join([self.RK_ROOT, 
                                    queue, 
                                    self.RK_START])
        self.log(f"Sending  message to {queue} queue with routing "
                 f"key {new_routing_key}", self.RK_LOG_INFO)
        self.publish_and_log_message(new_routing_key, body_json)


    def _process_rk_catalog_get_complete(self, rk_parts: list, 
                                         body_json: dict) -> None:
        # forward confirmation to monitor
        self.log(f"Sending message to {self.RK_MONITOR} queue", 
                    self.RK_LOG_INFO)
        new_routing_key = ".".join([self.RK_ROOT, 
                                    self.RK_MONITOR, 
                                    rk_parts[2]])
        self.publish_and_log_message(new_routing_key, body_json)

        # forward to transfer_get
        queue = f"{self.RK_TRANSFER_GET}"
        new_routing_key = ".".join([self.RK_ROOT, 
                                    queue, 
                                    self.RK_START])
        self.log(f"Sending  message to {queue} queue with routing "
                    f"key {new_routing_key}", self.RK_LOG_INFO)
        self.publish_and_log_message(new_routing_key, body_json)


    def callback(self, ch: Channel, method: Method, properties: Header, 
                 body: bytes, connection: Connection) -> None:
        rk_parts, body_json = self._process_message(method, body, properties)
        
        # If putting then first scan file/filelist
        if self.RK_PUT in rk_parts[2]:
            self._process_rk_put(body_json)
        
        elif self.RK_GET in rk_parts[2]:
            self._process_rk_get(body_json)

        elif self.RK_LIST in rk_parts[2]:
            self._process_rk_list(body_json)

        # If a task has completed, initiate new tasks
        elif rk_parts[2] == f"{self.RK_COMPLETE}":
            # If index completed then pass file list for transfer and cataloging
            if rk_parts[1] == f"{self.RK_INDEX}":
                self._process_rk_index_complete(body_json)

            # If transfer_put completed
            elif (rk_parts[1] == f"{self.RK_TRANSFER_PUT}"):
                self._process_rk_transfer_put_complete(body_json)

            # if catalog_put completed
            elif (rk_parts[1] == f"{self.RK_CATALOG_PUT}"):
                self._process_rk_catalog_put_complete(body_json)

            # if catalog_get completed
            elif (rk_parts[1] == f"{self.RK_CATALOG_GET}"):
                self._process_rk_catalog_get_complete(rk_parts, body_json)

        elif rk_parts[2] == f"{self.RK_FAILED}":
            # if transfer_put failed then we need to remove the failed files 
            # from the catalog
            if rk_parts[1] == f"{self.RK_TRANSFER_PUT}":
                self._process_rk_transfer_put_failed(body_json)

        self.log(f"Worker callback complete!", self.RK_LOG_INFO)


    def publish_and_log_message(self, routing_key: str, msg: dict, 
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