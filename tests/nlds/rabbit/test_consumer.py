import json

import pytest
import functools

from nlds.rabbit import publisher as publ
from nlds.rabbit.consumer import RabbitMQConsumer


def mock_load_config(template_config):
    return template_config

class MockConsumerNoCallback(RabbitMQConsumer):
    pass

class MockConsumer(RabbitMQConsumer):
    def callback(self, ch, method, properties, body, connection):
        pass

@pytest.fixture()
def default_consumer(monkeypatch, template_config):
    # Ensure template is loaded instead of .server_config
    monkeypatch.setattr(publ, "load_config", functools.partial(mock_load_config, template_config))
    return MockConsumer()

@pytest.mark.parametrize("queue_param", [None, 'test_string', "{{ rabbit_queue_name }}"])
def test_constructor(monkeypatch, template_config, queue_param):
    # Ensure template is loaded instead of 
    monkeypatch.setattr(publ, "load_config", functools.partial(mock_load_config, template_config))

    # Test that the consumer cannot be instantiated directly
    with pytest.raises(TypeError):
        RabbitMQConsumer()
        # Or without first instantiating the callback method
        MockConsumerNoCallback()
    
    # Check that a standard consumer with a defined callback can be instantiated
    consumer = MockConsumer(queue=queue_param)  
    # It should not have consumer-specific configuration (which should be an empty dict)
    assert isinstance(consumer.consumer_config, dict) and not consumer.consumer_config 

def test_split_routing_key(edge_values):
    assert len(RabbitMQConsumer.split_routing_key("test.test.test")) == 3
    with pytest.raises(ValueError):
        RabbitMQConsumer.split_routing_key("test.test")
        RabbitMQConsumer.split_routing_key("testtesttest")
        RabbitMQConsumer.split_routing_key("test/test/test")
        for ev in edge_values:
            RabbitMQConsumer.split_routing_key(ev)

@pytest.mark.parametrize("preroute", ["exchange", " ", ""])
@pytest.mark.parametrize("broken_preroute", [None, 0])
def test_append_route_info(default_rmq_body, edge_values, preroute, 
                           broken_preroute):
    # Requires a dictionary with details (i.e. of the appropriate format)
    with pytest.raises(KeyError):
        RabbitMQConsumer.append_route_info({}, "test_route")

    # create dummy message. Should fail in bytes form, needs to be a dict
    message = default_rmq_body
    with pytest.raises(TypeError):
        RabbitMQConsumer.append_route_info(message, "test_route")
    message = json.loads(message)

    # This should be able to be routed and rerouted
    routed_message = RabbitMQConsumer.append_route_info(message, "test_route,")
    routed_message = RabbitMQConsumer.append_route_info(routed_message, "test_route")

    # Prepare a pre-routed message with a test parameter
    routed_message[RabbitMQConsumer.MSG_DETAILS][RabbitMQConsumer.MSG_ROUTE] = preroute
    RabbitMQConsumer.append_route_info(routed_message, "test_route")
    RabbitMQConsumer.append_route_info(routed_message, preroute)

    for ev in edge_values:
        # Try an edge value and then rerouting an edge value
        new_routed_message = RabbitMQConsumer.append_route_info(message, ev)
        RabbitMQConsumer.append_route_info(new_routed_message, ev)
        RabbitMQConsumer.append_route_info(routed_message, ev)

    # Having a pre-routed message with a non-string route should break
    routed_message[RabbitMQConsumer.MSG_DETAILS][RabbitMQConsumer.MSG_ROUTE] = broken_preroute
    with pytest.raises(TypeError):
        RabbitMQConsumer.append_route_info(message, broken_preroute)
        RabbitMQConsumer.append_route_info(routed_message, "test_route")
        RabbitMQConsumer.append_route_info(routed_message, broken_preroute)