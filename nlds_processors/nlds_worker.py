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

        print(f" [x] Received {body} ({method.routing_key})")
        print(f" [x] Appending rerouting information to message: {self.DEFAULT_REROUTING_INFO} ")
        body_json = self.append_route_info(body_json)

        print(f" [x] Checking routing_key and re-routing")
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError:
            print(' [XXX] Routing key inappropriate length, exiting callback.')
            return
        
        # If putting then first scan file/filelist
        if self.RK_PUT in rk_parts[2].upper():
            print(f" [x] Sending put command to be indexed")
            
            new_routing_key = ".".join([self.RK_ROOT, self.RK_INDEX, self.RK_INITIATE])
            self.publish_message(new_routing_key, json.dumps(body_json))
        
        # If a task has completed, initiate new tasks
        elif rk_parts[2] == f"{self.RK_COMPLETE}":
            # If index completed then pass file list for transfer and cataloging
            if rk_parts[1] == f"{self.RK_INDEX}":
                print(f" [x] Scan successful, pass message back to transfer and" 
                      " cataloging queues")
                for queue in [self.RK_TRANSFER, self.RK_CATALOGUE]:
                    print(f" [x]  Sending to {queue} queue")
                    new_routing_key = ".".join([self.RK_ROOT, queue, self.RK_INITIATE])
                    self.publish_message(new_routing_key, json.dumps(body_json))

            # If transfer or catalogue completed then forward confirmation to 
            # monitor
            # TODO This might not be strictly necessary, could get those 
            # consumers to do so directly instead of going back via the NLDS 
            # worker
            elif rk_parts[1] == f"{self.RK_CATALOGUE}" or rk_parts[1] == f"{self.RK_TRANSFER}":
                print(f" [x]  Sending to {self.RK_MONITOR} queue")
                new_routing_key = ".".join([self.RK_ROOT, self.RK_MONITOR, rk_parts[2]])
                self.publish_message(new_routing_key, json.dumps(body_json), 
                                     monitor_fl=False)
                                     
        print(f" [x] DONE! \n")

    def publish_message(self, routing_key: str, msg: str, monitor_fl=True) -> None:
        """
        Wrapper around original publish message to additionally send message to monitoring
        queue. 
        """
        super().publish_message(routing_key, msg)

        if monitor_fl:
            # Additionally send same message to monitoring.
            rk_parts = routing_key.split(".")
            rk_parts[1] = self.RK_MONITOR
            rk_parts[2] = self.RK_LOG_INFO
            new_routing_key = ".".join(rk_parts)
            super().publish_message(new_routing_key, msg)

if __name__ == "__main__":
    consumer = NLDSWorkerConsumer()
    consumer.run()