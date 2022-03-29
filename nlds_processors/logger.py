import json
import logging
import traceback

from nlds.rabbit.consumer import RabbitMQConsumer
from nlds.server_config import LOGGING_CONFIG_FILES, LOGGING_CONFIG_LEVEL, LOGGING_CONFIG_SECTION, LOGGING_CONFIG_STDOUT_LEVEL

logger = logging.getLogger("nlds.root")

class LoggingConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "logging_q"
    DEFAULT_ROUTING_KEY = f"{RabbitMQConsumer.RK_ROOT}.{RabbitMQConsumer.RK_LOG}."\
                          f"{RabbitMQConsumer.RK_WILD}"
    DEFAULT_LOGGING_LEVEL = RabbitMQConsumer.RK_LOG_INFO

    _logging_levels = (
        RabbitMQConsumer.RK_LOG_DEBUG,
        RabbitMQConsumer.RK_LOG_INFO,
        RabbitMQConsumer.RK_LOG_WARNING,
        RabbitMQConsumer.RK_LOG_ERROR,
        RabbitMQConsumer.RK_LOG_CRITICAL
    )
    _logging_modes = {
        RabbitMQConsumer.RK_LOG_NONE: 0,      
        RabbitMQConsumer.RK_LOG_INFO: 1,
        RabbitMQConsumer.RK_LOG_DEBUG: 2,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME, setup_logging_fl=True):
        super().__init__(queue=queue, setup_logging_fl=setup_logging_fl)
        
    def callback(self, ch, method, properties, body, connection):
        # Convert body from bytes to json for ease of manipulation
        body_json = json.loads(body)

        # Verify routing key is appropriate
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError:
            logger.error("Routing key inappropriate format, exiting callback.")
            logger.debug(traceback.format_exc())
            return

        # Print certain outputs to global logger output depending on the stdout 
        # log level.
        logger.info(f"Received message with route {body_json[self.MSG_DETAILS][self.MSG_ROUTE]}")
        
        # Print whole json message if in debug
        logger.debug(json.dumps(body_json, indent=4))

        # The log level should be in the routing key, the logger to use should 
        # be in the message body under MSG_DETAILS:MSG_LOG_TARGET
        if rk_parts[2] not in self._logging_levels:
            logger.error(f"Invalid routing key provided, log_level is set to an invalid value ({rk_parts[2]})\n"
                         f"Should be one of {self._logging_levels}.")
            logger.debug(traceback.format_exc())
            return
        
        try:
            consumer = body_json[RabbitMQConsumer.MSG_DETAILS][RabbitMQConsumer.MSG_LOG_TARGET]
        except KeyError as e:
            logger.error(f"Invalid message contents, log target should be in the details section of the message body.")
            logger.debug(traceback.format_exc(), exc_info=e)
            return
        
        # Get curated list of loggers to verify the log_target can actually be used.
        loggers = {name: logging.getLogger(name) for name in logging.root.manager.loggerDict 
                   if self.LOGGER_PREFIX == name[:5]}
        try:
            consumer_logger = loggers[consumer]
        except KeyError as e:
            logger.error(f"Invalid log target provided, consumer does not have a valid logger/handler setup "
                         f"({consumer})\nShould be one of {list(loggers.keys())}.", exc_info=e)
            logger.debug(traceback.format_exc())
            return
        logging_func = self.get_logging_func(rk_parts[2], logger_like=consumer_logger)

        # Check message body contains log message, under MSG_DATA:MSG_LOG_MESSAGE
        try:
            log_message = body_json[RabbitMQConsumer.MSG_DATA][RabbitMQConsumer.MSG_LOG_MESSAGE]
        except KeyError as e:
            logger.error(f"Invalid message contents, log message should be in the data section of the message body.",
                         exc_info=e)
            logger.debug(traceback.format_exc())
            return

        # Finally, log the message
        logging_func(log_message)

        logger.info(f"Callback finished. \n")

    @staticmethod
    def get_logging_func(log_level: str, logger_like: logging.Logger = logger):
        """
        Selects the appropriate logging function, given a logging level, from a 
        Logger object or Logger-like object. The global logger is used by 
        default (logging.getLogger(__name__)) but anything which has the 
        standard logging operations (debug, info, warning etc. ) can be used 
        i.e. the logging package

        :param str log_level:       Logging level requested
        :param Logger logger_like:  Logger object to get function from
        :return:    The logging function to use
        """
        logging_function = {
            RabbitMQConsumer.RK_LOG_DEBUG: logger_like.debug,
            RabbitMQConsumer.RK_LOG_INFO: logger_like.info,
            RabbitMQConsumer.RK_LOG_WARNING: logger_like.warning,
            RabbitMQConsumer.RK_LOG_ERROR: logger_like.error,
            RabbitMQConsumer.RK_LOG_CRITICAL: logger_like.critical
        }
        return logging_function[log_level]
        
def main():
    consumer = LoggingConsumer()
    consumer.run()

if __name__ == "__main__":
    main()