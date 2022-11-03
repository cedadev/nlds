import json

from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

from nlds.rabbit.consumer import RabbitMQConsumer

class TestConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "test_q"
    DEFAULT_ROUTING_KEY = (f"{RabbitMQConsumer.RK_ROOT}."
                           f"test."
                           f"{RabbitMQConsumer.RK_WILD}")
    DEFAULT_REROUTING_INFO = f"->TEST_Q"

    def callback(self,ch: Channel, method: Method, properties: Header, 
                 body: bytes, connection: Connection) -> None:

        print("Callback being executed!")
        self.publish_rpc_message(properties, "Test-y test-y test test")

def main():
    consumer = TestConsumer()
    consumer.run()

if __name__ == "__main__":
    main()