import os
import json
from uuid import UUID
import logging

import pytest

from nlds.rabbit.publisher import RabbitMQPublisher


TEMPLATE_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 
                                    'server-config.json')

@pytest.fixture
def template_config():
    config_path = TEMPLATE_CONFIG_PATH
    fh = open(config_path)
    return json.load(fh)

@pytest.fixture
def test_uuid():
    return UUID("3fa85f64-5717-4562-b3fc-2c963f66afa6")

@pytest.fixture
def edge_values():
    return ("", " ", ".", None, "None")

class MockRabbitMethod():
    def __init__(self, routing_key=''):
        self.routing_key = routing_key

@pytest.fixture
def default_rmq_method(routing_key="nlds.test.test"):
    return MockRabbitMethod(routing_key=routing_key)

@pytest.fixture
def default_rmq_body(test_uuid):
    return RabbitMQPublisher.create_message(test_uuid, "data", "user", "group", "target")

@pytest.fixture
def default_rmq_log_body():
    return RabbitMQPublisher.create_log_message("message", "target")

@pytest.fixture
def default_rmq_message_dict(default_rmq_body):
    return json.loads(default_rmq_body)

@pytest.fixture
def default_rmq_logmsg_dict(default_rmq_log_body):
    return json.loads(default_rmq_log_body)

@pytest.fixture
def routed_rmq_message_dict(default_rmq_message_dict):
    default_rmq_message_dict[RabbitMQPublisher.MSG_DETAILS][RabbitMQPublisher.MSG_ROUTE] = "place->place"
    return default_rmq_message_dict

@pytest.fixture(autouse=False)
def no_logs_gte_error(caplog):
    """
    Automatically tests/asserts messages where an error or critical level 
    message is logged.
    """
    yield
    errors = [record for record in caplog.get_records('call') if record.levelno >= logging.ERROR]
    assert not errors

@pytest.fixture
def assert_last_caplog(caplog, log_level='ERROR', clear_fl=False):
    # Check that the last logrecord is a message of log_level
    assert caplog.records[-1].levelname == log_level
    if clear_fl:
        caplog.clear()