import json
import logging

from nlds.rabbit.consumer import RabbitMQConsumer
from nlds.utils.constants import DATA, DETAILS
from nlds_processors.utils.constants import WILD
from utils.constants import ROOT, MONITOR, LOG_INFO, LOG_ERROR, LOG_WARNING, \
                            LOG_DEBUG, LOG_CRITICAL

logger = logging.getLogger(__name__)

logging_function = {
    LOG_DEBUG: logger.debug,
    LOG_INFO: logger.info,
    LOG_WARNING: logger.warning,
    LOG_ERROR: logger.error,
    LOG_CRITICAL: logger.critical
}

class MonitorConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "monitor_q"
    DEFAULT_REROUTING_KEY = f"{ROOT}.{MONITOR}.{WILD}"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
        
    def callback(self, ch, method, properties, body, connection):
        # Convert body from bytes to json for ease of manipulation
        body_json = json.loads(body)

        print(f" [x] Received {body} from {self.queues[0].name} ({method.routing_key})")
        rk_parts = self.verify_routing_key(method.routing_key)

        logging_function[rk_parts[2]](json.dumps(body_json))

        print(f" [x] DONE! \n")
        
if __name__ == "__main__":
    consumer = MonitorConsumer()
    consumer.run()