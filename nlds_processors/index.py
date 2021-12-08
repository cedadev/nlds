from utils.constants import COMPLETE, ROOT, INDEX, TRIAGE
from nlds.rabbit.consumer import RabbitMQConsumer

class IndexerConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "indexer_q"
    DEFAULT_ROUTING_KEY = f"{ROOT}.{INDEX}.*"
    DEFAULT_ROUTING_INFO = f"->INDEX_Q->Exchange"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
    
    def callback(self, ch, method, properties, body, connection):
        # Convert body from bytes to string for ease of manipulation
        body = body.decode("utf-8")

        print(f" [x] Received {body} from {self.queues[0].name} ({method.routing_key})")
        print(f" [...] Scan goes here...")
        print(f" [x] Returning file list to worker and appending route info ({self.DEFAULT_ROUTING_INFO})")

        rk_parts = method.routing_key.split('.')
        try:
            rk_parts = self.verify_routing_key(method.routing_key)
        except ValueError:
            print(' [XXX] Routing key inappropriate length, exiting callback.')
            return
        
        # Reinsert message into exchange with edited routing key to avoid infinite loop
        rk_parts[0] = TRIAGE
        rk_parts[2] = COMPLETE

        new_routing_key = ".".join(rk_parts)
        self.publish_message(new_routing_key, f"{body}{self.DEFAULT_ROUTING_INFO}")

        print(f" [x] DONE! \n")
       
        
if __name__ == "__main__":
    consumer = IndexerConsumer()
    consumer.run()