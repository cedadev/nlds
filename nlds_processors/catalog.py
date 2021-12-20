import json

from nlds.rabbit.consumer import RabbitMQConsumer
from utils.constants import ROOT, CATALOGUE, WILD

class CatalogConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "catalog_q"
    DEFAULT_ROUTING_KEY = f"{ROOT}.{CATALOGUE}.{WILD}"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

    def callback(self, ch, method, properties, body, connection):
        # Convert body from bytes to json for ease of manipulation
        body = json.loads(body)

        print(f" [x] Received {json.dumps(body)} from {self.queues[0].name} " +
              f"({method.routing_key})")
        
if __name__ == "__main__":
    consumer = CatalogConsumer()
    consumer.run()