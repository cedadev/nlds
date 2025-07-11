# encoding: utf-8
"""
test_consumer.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import json

import pytest
import functools

from nlds.rabbit import publisher as publ
from nlds.rabbit.consumer import RabbitMQConsumer as RMQP
import nlds.rabbit.message_keys as MSG


def mock_load_config(template_config):
    return template_config


class MockConsumerNoCallback(RMQP):
    pass


class MockConsumer(RMQP):
    def callback(self, ch, method, properties, body, connection):
        pass


@pytest.fixture()
def default_consumer(monkeypatch, template_config):
    # Ensure template is loaded instead of .server_config
    monkeypatch.setattr(
        "nlds.server_config.load_config",
        functools.partial(mock_load_config, template_config),
    )
    return MockConsumer()


@pytest.mark.parametrize(
    "queue_param", ["catalog_q", "index_q", "transfer_get_q"]
)
def test_constructor(monkeypatch, template_config, queue_param):
    # Ensure template is loaded instead of
    monkeypatch.setattr(
        "nlds.server_config.load_config",
        functools.partial(mock_load_config, template_config),
    )

    # Test that the consumer cannot be instantiated directly
    with pytest.raises(TypeError):
        RMQP()
        # Or without first instantiating the callback method
        MockConsumerNoCallback()

    # Check that a standard consumer with a defined callback can be instantiated
    consumer = MockConsumer(queue=queue_param)
    assert isinstance(consumer.consumer_config, dict)


def test_split_routing_key(edge_values):
    assert len(RMQP.split_routing_key("test.test.test")) == 3
    with pytest.raises(ValueError):
        RMQP.split_routing_key("test.test")
        RMQP.split_routing_key("testtesttest")
        RMQP.split_routing_key("test/test/test")
        for ev in edge_values:
            RMQP.split_routing_key(ev)


@pytest.mark.parametrize("preroute", ["exchange", " ", ""])
@pytest.mark.parametrize("broken_preroute", [None, 0])
def test_append_route_info(default_rmq_body, edge_values, preroute, broken_preroute):
    # Requires a dictionary with details (i.e. of the appropriate format)
    with pytest.raises(KeyError):
        RMQP.append_route_info({}, "test_route")

    # create dummy message. Should fail in bytes form, needs to be a dict
    message = default_rmq_body
    with pytest.raises(TypeError):
        RMQP.append_route_info(message, "test_route")
    message = json.loads(message)

    # This should be able to be routed and rerouted
    routed_message = RMQP.append_route_info(message, "test_route,")
    routed_message = RMQP.append_route_info(routed_message, "test_route")

    # Prepare a pre-routed message with a test parameter
    routed_message[MSG.DETAILS][MSG.ROUTE] = preroute
    RMQP.append_route_info(routed_message, "test_route")
    RMQP.append_route_info(routed_message, preroute)

    for ev in edge_values:
        # Try an edge value and then rerouting an edge value
        new_routed_message = RMQP.append_route_info(message, ev)
        RMQP.append_route_info(new_routed_message, ev)
        RMQP.append_route_info(routed_message, ev)

    # Having a pre-routed message with a non-string route should break
    routed_message[MSG.DETAILS][MSG.ROUTE] = broken_preroute
    with pytest.raises(TypeError):
        RMQP.append_route_info(message, broken_preroute)
        RMQP.append_route_info(routed_message, "test_route")
        RMQP.append_route_info(routed_message, broken_preroute)
