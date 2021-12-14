import json

from nlds.rabbit.consumer import RabbitMQConsumer
from nlds.utils.constants import DATA, DETAILS
from nlds_processors.utils.constants import WILD
from utils.constants import ROOT, MONITOR

class MonitorConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "monitor_q"
    DEFAULT_REROUTING_KEY = f"{ROOT}.{MONITOR}.{WILD}"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
        
    def callback(self, ch, method, properties, body, connection):
        # Convert body from bytes to json for ease of manipulation
        body_json = json.loads(body)

        print(f" [x] Received {body} from {self.queues[0].name} ({method.routing_key})")
        print(f" [x] Attempting to print prettily...")
        print(json.dumps(body_json, indent=4))

        print(body_json[DATA])
        print(body_json[DETAILS])
        print(f" [x] DONE! \n")
        
if __name__ == "__main__":
    consumer = MonitorConsumer()
    consumer.run()