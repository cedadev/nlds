from nlds.rabbit.consumer import RabbitMQConsumer
from utils.constants import ROOT, TRANSFER

class TransferConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "transfer_q"
    DEFAULT_ROUTING_KEY = f"{ROOT}.{TRANSFER}.*"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
        
    def callback(self, ch, method, properties, body, connection):
        # Convert body from bytes to string for ease of manipulation
        body = body.decode("utf-8")

        print(f" [x] Received {body} from {self.queues[0].name} ({method.routing_key})")
        
if __name__ == "__main__":
    consumer = TransferConsumer()
    consumer.run()