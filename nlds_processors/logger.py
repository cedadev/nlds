import json
import logging
import sys

from nlds.rabbit.consumer import RabbitMQConsumer

logger = logging.getLogger(__name__)

class LoggingConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "logging_q"
    DEFAULT_ROUTING_KEY = f"{RabbitMQConsumer.RK_ROOT}.{RabbitMQConsumer.RK_LOG}."\
                          f"{RabbitMQConsumer.RK_WILD}"
    DEFAULT_LOGGING_LEVEL = RabbitMQConsumer.RK_LOG_INFO

    _logging_function = {
        RabbitMQConsumer.RK_LOG_DEBUG: logger.debug,
        RabbitMQConsumer.RK_LOG_INFO: logger.info,
        RabbitMQConsumer.RK_LOG_WARNING: logger.warning,
        RabbitMQConsumer.RK_LOG_ERROR: logger.error,
        RabbitMQConsumer.RK_LOG_CRITICAL: logger.critical
    }
    _logging_levels = {
        RabbitMQConsumer.RK_LOG_NONE: 0,      
        RabbitMQConsumer.RK_LOG_INFO: 1,
        RabbitMQConsumer.RK_LOG_DEBUG: 2,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME, setup_logging_fl=True):
        super().__init__(queue=queue, setup_logging_fl=setup_logging_fl)

        if "logging_level" in self.consumer_config:
            self.logging_level = self.consumer_config["logging_level"]
        else: 
            self.logging_level = self.DEFAULT_LOGGING_LEVEL
        self.log_no = self._logging_levels[self.logging_level]
        
    def callback(self, ch, method, properties, body, connection):
        # Convert body from bytes to json for ease of manipulation
        body_json = json.loads(body)

        if self.log_no >= 1:
            # Print route information
            print(f" [x] Received message with route {body_json[self.MSG_DETAILS][self.MSG_ROUTE]}")
        if self.log_no >= 2:
            # Print whole json message
            print(json.dumps(body_json, indent=4))

        print(f" [x] DONE! \n")

        
def main():
    consumer = LoggingConsumer()
    consumer.run()

if __name__ == "__main__":
    main()