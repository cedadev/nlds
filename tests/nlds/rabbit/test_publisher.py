import json
from socket import gaierror
import copy

import pytest
import functools

from nlds.rabbit import publisher as publ
from nlds.rabbit.publisher import RabbitMQPublisher
from nlds.server_config import LOGGING_CONFIG_ENABLE, LOGGING_CONFIG_FORMAT, \
                               LOGGING_CONFIG_LEVEL, LOGGING_CONFIG_SECTION, \
                               LOGGING_CONFIG_STDOUT, LOGGING_CONFIG_STDOUT_LEVEL

def mock_load_config(template_config):
    return template_config

@pytest.fixture()
def default_publisher(monkeypatch, template_config):
    # Ensure template is loaded instead of .server_config
    monkeypatch.setattr(publ, "load_config", functools.partial(mock_load_config, template_config))
    
    # Check that the publisher with callback can be created
    return RabbitMQPublisher()

def message_assertions(message):
    # Test message output format is correct
    assert isinstance(message, str)
    message = json.loads(message)

    # Check message contains expected details
    assert RabbitMQPublisher.MSG_DETAILS in message
    message_details = message[RabbitMQPublisher.MSG_DETAILS]
    assert RabbitMQPublisher.MSG_TRANSACT_ID in message_details
    assert RabbitMQPublisher.MSG_TIMESTAMP in message_details
    assert RabbitMQPublisher.MSG_USER in message_details
    assert RabbitMQPublisher.MSG_GROUP in message_details
    assert RabbitMQPublisher.MSG_TARGET in message_details
    assert RabbitMQPublisher.MSG_ACCESS_KEY in message_details
    assert RabbitMQPublisher.MSG_SECRET_KEY in message_details
    assert RabbitMQPublisher.MSG_TENANCY in message_details

    # Check message contains expected data
    assert RabbitMQPublisher.MSG_DATA in message
    message_data = message[RabbitMQPublisher.MSG_DATA]
    assert RabbitMQPublisher.MSG_FILELIST in message_data

    assert RabbitMQPublisher.MSG_TYPE in message

def test_constructor(default_publisher):
    # Check that the publisher with callback can be created
    assert default_publisher.whole_config['authentication']['jasmin_authenticator']['user_profile_url'] == "{{ user_profile_url }}"

@pytest.mark.parametrize("uuid", [123, "123"])
@pytest.mark.parametrize("filelist", [
    "test_file.txt", 
    "[test_1, test_2]", 
    ["test_1", "test_2"],
    [123, 100],
])
def test_create_message(test_uuid, uuid, filelist):
    # Test with definitely functional data
    message = RabbitMQPublisher.create_message(
        test_uuid, 
        ["data", "data_again"], 
        "access_key",
        "secret_key", 
        user="user",
        group="group", 
        target="target",
        tenancy="tenancy"
    )
    message_assertions(message)

    # Test whether optional args are optional
    message = RabbitMQPublisher.create_message(test_uuid, ["data_0", "data_1"],  
                                               "access_key", "secret_key")
    message_assertions(message)

    # Test with definitely non-functional data
    with pytest.raises(TypeError):
        message = RabbitMQPublisher.create_message(test_uuid, 123, 
                                                   "access_key", "secret_key")
        message = RabbitMQPublisher.create_message(test_uuid, {"k": "v"}, 
                                                   "access_key", "secret_key")

    # Test with the parameterisations
    message = RabbitMQPublisher.create_message(test_uuid, filelist, 
                                               "access_key", "secret_key")
    message_assertions(message)
    message = RabbitMQPublisher.create_message(uuid, filelist, 
                                               "access_key", "secret_key")
    message_assertions(message)

def test_publish_message(default_publisher):
    # Trying to send a message without first making a connection should fail. 
    with pytest.raises(AttributeError):
        default_publisher.publish_message("test.rk", "test message")

    # Attempting to establish a connection with the template config should also 
    # fail with a socket error
    with pytest.raises(gaierror):
        default_publisher.get_connection()

    # TODO: Make mock connection object and send messages through it?

def test_setup_logging(monkeypatch, default_publisher):
    # Running with enabled=false should complete with no problems
    default_publisher.setup_logging(enable=False)

    # Get the default logging config dict
    logging_config = copy.deepcopy(default_publisher._default_logging_conf)

    # Attempt to use it, unchanged, in place of the template config 
    monkeypatch.setattr(default_publisher, "whole_config", logging_config)
    default_publisher.setup_logging()

    # Attempt to use it with a single missing key
    logging_config.pop(LOGGING_CONFIG_LEVEL)
    monkeypatch.setattr(default_publisher, "whole_config", logging_config)
    default_publisher.setup_logging()
    
    # Attempt to setup logging with no logging options in server config. Default 
    # options should be used. 
    monkeypatch.setattr(default_publisher, "whole_config", dict())
    default_publisher.setup_logging()

    # Attempt with all kwargs
    logging_config = copy.deepcopy(default_publisher._default_logging_conf)
    log_level = logging_config[LOGGING_CONFIG_LEVEL]
    log_format = logging_config[LOGGING_CONFIG_FORMAT]
    add_stdout_fl = logging_config[LOGGING_CONFIG_STDOUT]
    stdout_lvl = logging_config[LOGGING_CONFIG_STDOUT_LEVEL]
    default_publisher.setup_logging(log_level=log_level, log_format=log_format, 
                                    add_stdout_fl=add_stdout_fl, stdout_log_level=stdout_lvl)

    # Attempt with only some kwargs
    default_publisher.setup_logging(log_level=log_level, add_stdout_fl=add_stdout_fl, 
                                    stdout_log_level=stdout_lvl)
    default_publisher.setup_logging(log_level=log_level, stdout_log_level=stdout_lvl)
    default_publisher.setup_logging(log_level=log_level)

    # TODO: Probably should be some form of input checking for the values pulled 
    # from .server_config 