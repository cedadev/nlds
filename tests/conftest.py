import os
import json
from uuid import UUID
import logging
from datetime import datetime

import pytest

from nlds.rabbit.publisher import RabbitMQPublisher as RMQP
import nlds.rabbit.message_keys as MSG
from nlds.details import PathDetails, Retries


TEMPLATE_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "req-config-template.json"
)


@pytest.fixture
def template_config():
    config_path = TEMPLATE_CONFIG_PATH
    with open(config_path) as fh:
        config_dict = json.load(fh)
    return config_dict


@pytest.fixture
def test_uuid():
    return UUID("3fa85f64-5717-4562-b3fc-2c963f66afa6")


@pytest.fixture
def edge_values():
    return ("", " ", ".", None, "None")


class MockRabbitMethod:
    def __init__(self, routing_key=""):
        self.routing_key = routing_key


@pytest.fixture
def default_rmq_method(routing_key="nlds.test.test"):
    return MockRabbitMethod(routing_key=routing_key)


@pytest.fixture
def default_rmq_body(test_uuid):
    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: str(test_uuid),
            MSG.SUB_ID: str(test_uuid),
            MSG.TIMESTAMP: datetime.now().isoformat(sep="-"),
            MSG.USER: "user",
            MSG.GROUP: "group",
            MSG.TENANCY: "tenancy",
            MSG.ACCESS_KEY: "access_key",
            MSG.SECRET_KEY: "secret_key",
        },
        MSG.DATA: {
            # Convert to PathDetails for JSON serialisation
            MSG.FILELIST: [
                PathDetails(original_path="item_path"),
            ],
        },
        **Retries().to_dict(),
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    return json.dumps(msg_dict)


@pytest.fixture
def default_rmq_log_body():
    return json.dumps(RMQP.create_log_message("message", "target"))


@pytest.fixture
def default_rmq_message_dict(default_rmq_body):
    return json.loads(default_rmq_body)


@pytest.fixture
def default_rmq_logmsg_dict(default_rmq_log_body):
    return json.loads(default_rmq_log_body)


@pytest.fixture
def routed_rmq_message_dict(default_rmq_message_dict):
    default_rmq_message_dict[MSG.DETAILS][MSG.ROUTE] = "place->place"
    return default_rmq_message_dict


@pytest.fixture(autouse=False)
def no_logs_gte_error(caplog):
    """
    Automatically tests/asserts messages where an error or critical level
    message is logged.
    """
    yield
    errors = [
        record
        for record in caplog.get_records("call")
        if record.levelno >= logging.ERROR
    ]
    assert not errors


@pytest.fixture
def assert_last_caplog(caplog, log_level="ERROR", clear_fl=False):
    # Check that the last logrecord is a message of log_level
    assert caplog.records[-1].levelname == log_level
    if clear_fl:
        caplog.clear()
