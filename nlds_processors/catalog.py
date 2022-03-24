import json

from nlds.rabbit.consumer import RabbitMQConsumer
class CatalogConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "catalog_q"
    DEFAULT_ROUTING_KEY = f"{RabbitMQConsumer.RK_ROOT}.{RabbitMQConsumer.RK_CATALOGUE}."\
                          f"{RabbitMQConsumer.RK_WILD}"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

    def callback(self, ch, method, properties, body, connection):
        # Convert body from bytes to json for ease of manipulation
        body = json.loads(body)

        self.log(f"Received {json.dumps(body)} from {self.queues[0].name} "
                 f"({method.routing_key})", self.RK_LOG_INFO)
        

def main():
    consumer = CatalogConsumer()
    consumer.run()

if __name__ == "__main__":
    main()