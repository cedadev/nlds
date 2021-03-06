from nlds.rabbit.consumer import RabbitMQConsumer

class TransferConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "transfer_q"
    DEFAULT_ROUTING_KEY = f"{RabbitMQConsumer.RK_ROOT}.{RabbitMQConsumer.RK_TRANSFER}.{RabbitMQConsumer.RK_WILD}"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
        
    def callback(self, ch, method, properties, body, connection):
        # Convert body from bytes to string for ease of manipulation
        body = body.decode("utf-8")

        self.log(f"Received {body} from {self.queues[0].name} ({method.routing_key})", 
                 self.RK_LOG_INFO)


def main():
    consumer = TransferConsumer()
    consumer.run()

if __name__ == "__main__":
    main()