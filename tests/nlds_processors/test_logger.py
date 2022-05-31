import copy
import json 
import pytest
import functools
import logging

from nlds.rabbit import publisher as publ
from nlds.rabbit.publisher import RabbitMQPublisher as rmqp
from nlds_processors.logger import LoggingConsumer

def mock_load_config(template_config):
    return template_config

class MockLogger(LoggingConsumer):
    def callback(self, ch, method, properties, body, connection):
        pass

@pytest.fixture()
def default_logger(monkeypatch, template_config):
    # Ensure template is loaded instead of .server_config
    monkeypatch.setattr(publ, "load_config", functools.partial(mock_load_config, template_config))
    return LoggingConsumer()

def assert_last_caplog(caplog, log_level='ERROR', clear_fl=False):
    # Check that the last logrecord is a message of log_level
    assert caplog.records[-1].levelname == log_level
    if clear_fl:
        caplog.clear()

@pytest.mark.parametrize("rk_log_level", LoggingConsumer._logging_levels)
def test_callback(caplog, default_logger, default_rmq_method, 
                  default_rmq_logmsg_dict, rk_log_level):
    # Let caplog capture all log messages
    caplog.set_level(logging.DEBUG)

    test_message = 'message'

    # Add necessary logging data to message dict and convert into 
    # callback-friendly bytes. Note we're using general nlds logger here
    default_rmq_logmsg_dict[rmqp.MSG_DATA][rmqp.MSG_LOG_TARGET] = 'nlds'
    default_rmq_logmsg_dict[rmqp.MSG_DATA][rmqp.MSG_LOG_MESSAGE] = test_message
    routed_body = json.dumps(default_rmq_logmsg_dict)

    # Attempt to run callback with default message, should complete, but raise 
    # an error in the logs due to a faulty routing_key
    default_logger.callback(None, default_rmq_method, None, routed_body, None)
    assert_last_caplog(caplog, clear_fl=True)

    # Attempt to run with a known, sensible routing key
    custom_method = copy.deepcopy(default_rmq_method)
    custom_method.routing_key = f"nlds.test.{rk_log_level}"
    default_logger.callback(None, custom_method, None, routed_body, None)

    # Check that the proper message type was logged - the penultimate should 
    # be the type asked for, the final should be a confirmation info message
    assert caplog.records[-2].levelname == rk_log_level.upper()
    assert caplog.records[-2].message == test_message
    assert caplog.records[-1].levelname == 'INFO'

    # Check it breaks with a non-sensible routing key
    custom_method.routing_key = 'nlds.test.non-sensible'
    default_logger.callback(None, custom_method, None, routed_body, None)
    assert_last_caplog(caplog)

    # Check it breaks with a non-sensible log target
    custom_method.routing_key = f"nlds.test.info"
    default_rmq_logmsg_dict[rmqp.MSG_DATA][rmqp.MSG_LOG_TARGET] = 'test'
    routed_body = json.dumps(default_rmq_logmsg_dict)
    default_logger.callback(None, custom_method, None, routed_body, None)
    assert_last_caplog(caplog)

