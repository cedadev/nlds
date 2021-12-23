import json
from socket import gaierror

import pytest
import functools

from nlds.rabbit import publisher as publ
from nlds.rabbit.publisher import RabbitMQPublisher

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

    # Check message contains expected data
    assert RabbitMQPublisher.MSG_DATA in message
    message_data = message[RabbitMQPublisher.MSG_DATA]
    assert RabbitMQPublisher.MSG_FILELIST in message_data

def test_constructor(default_publisher):
    # Check that the publisher with callback can be created
    assert default_publisher.whole_config['authentication']['jasmin_authenticator']['user_profile_url'] == "{{ user_profile_url }}"

@pytest.mark.parametrize("uuid", [123, "123"])
@pytest.mark.parametrize("filelist", ["test_file.txt", "[test_1, test_2]"])
def test_create_message(test_uuid, uuid, filelist):
    # Test with definitely functional data
    message = RabbitMQPublisher.create_message(test_uuid, "data", "user", "group", "target")
    message_assertions(message)

    # Test whether optional args are optional
    message = RabbitMQPublisher.create_message(test_uuid, "data")
    message_assertions(message)

    # Test with the parameterisations
    message = RabbitMQPublisher.create_message(test_uuid, filelist)
    message_assertions(message)
    message = RabbitMQPublisher.create_message(uuid, filelist)
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