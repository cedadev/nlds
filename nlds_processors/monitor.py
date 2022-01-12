import json
import logging

from nlds.rabbit.consumer import RabbitMQConsumer

logger = logging.getLogger(__name__)

class MonitorConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "monitor_q"
    DEFAULT_ROUTING_KEY = f"{RabbitMQConsumer.RK_ROOT}.{RabbitMQConsumer.RK_MONITOR}."\
                          f"{RabbitMQConsumer.RK_WILD}"

    _logging_function = {
        RabbitMQConsumer.RK_LOG_DEBUG: logger.debug,
        RabbitMQConsumer.RK_LOG_INFO: logger.info,
        RabbitMQConsumer.RK_LOG_WARNING: logger.warning,
        RabbitMQConsumer.RK_LOG_ERROR: logger.error,
        RabbitMQConsumer.RK_LOG_CRITICAL: logger.critical
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
        
    def callback(self, ch, method, properties, body, connection):
        # Convert body from bytes to json for ease of manipulation
        body_json = json.loads(body)

        print(f" [x] Received {body} from {self.queues[0].name} ({method.routing_key})")
        rk_parts = self.split_routing_key(method.routing_key)

        self._logging_function[rk_parts[2]](json.dumps(body_json))

        print(f" [x] DONE! \n")

        
def main():
    consumer = MonitorConsumer()
    consumer.run()

if __name__ == "__main__":
    main()