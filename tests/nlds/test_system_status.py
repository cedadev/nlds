import pytest
import asyncio
import time
import requests
import json

import jinja2.environment
from fastapi import Request
from requests.auth import HTTPBasicAuth
import abc

from nlds.routers import system
from nlds.rabbit import rpc_publisher



@pytest.fixture
def loop():
    # allows the testing of asynchronus functions using an event loop
    
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


"""
Testing the get_consumer_status() function in system.py
"""


def mock_callback(host_ip, api_port, queue_name, login, password, vhost):
    # returns a list of mock consumers simulating what get_consumer_info does
    
    mock_consumer_tags = ["mock_tag_1", "mock_tag_2", "mock_tag_3", 
                          "mock_tag_4", "mock_tag_5"]
    return(mock_consumer_tags)


def mock_callback_offline(host_ip, api_port, queue_name, 
                          login, password, vhost):
    # returns an empty list of mock consumers 
    # simulating what get_consumer_info does if no consumers are online
    
    mock_consumer_tags = []
    return(mock_consumer_tags)


def mock_callback_rabbit(host_ip, api_port, queue_name, 
                          login, password, vhost):
    # throws a custom error summulating what happens if rabbits is offline
    
    raise system.RabbitError


def mock_callback_exception(host_ip, api_port, queue_name, 
                          login, password, vhost):
    # throws a RequestException simmulating what happens if requests is offline
    
    raise(requests.exceptions.RequestException)


def mock_callback_request(host_ip, api_port, queue_name, 
                          login, password, vhost):
    # throws a custom error simmulating what happens if requests is offline
    
    raise(system.RequestError)


async def mock_consumer(*args, **kwargs):
    # simulates a message being sent to a consumer only returning a value if
    # it was not meant to be ignored in the details
    
    saved_args = locals()
    
    if saved_args["kwargs"]["msg_dict"]["details"]["ignore_message"] == False:
        return("existance")
    else:
        return None
    
    
async def mock_slow_consumer(*args, **kwargs):
    # waits 2 seconds and checks if its slower than the time limit imposed
    # simulating if a consumer was running slowly
    
    saved_args = locals()
    time.sleep(2)
    if saved_args["kwargs"]["time_limit"] >= 2:
        return("existance")
    else:
        return None



def test_get_consumer_status_rabbits_offline(monkeypatch, 
                                             loop: asyncio.AbstractEventLoop):
    # test if it gives the correct response if the rabbit server is offline
    
    # the 2 variables required to run the get_consumer_status function
    time_limit = 5
    msg_dict = {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
            }
        }
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_info", mock_callback_rabbit)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, 
                        "call", mock_consumer)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    consumer = loop.run_until_complete(system.get_consumer_status(
        "consumer_q", "consumer", msg_dict, time_limit, 0))
    consumer = consumer[0]
    
    assert consumer == {"val": ("Rabbit error"), "colour": "PURPLE"}
    
    assert consumer["val"] == "Rabbit error"
    
    assert consumer["colour"] == "PURPLE"


def test_get_consumer_status_requests_failed(monkeypatch, 
                                             loop: asyncio.AbstractEventLoop):
    # test if it handels the error properly if requests is offline
    
    # the 2 variables required to run the get_consumer_status function
    time_limit = 5
    msg_dict = {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
            }
        }
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_info", mock_callback_exception)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, 
                        "call", mock_consumer)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    consumer = loop.run_until_complete(system.get_consumer_status(
        "consumer_q", "consumer", msg_dict, time_limit, 0))
    consumer = consumer[0]
    
    assert consumer == {'val': 'Failed to make request', 'colour': 'PURPLE'}
    
    assert consumer["val"] == "Failed to make request"
    
    assert consumer["colour"] == "PURPLE"
    
    
def test_get_consumer_status_requests_error(monkeypatch, 
                                             loop: asyncio.AbstractEventLoop):
    # test if it handels the error properly if requests is offline
    
    # the 2 variables required to run the get_consumer_status function
    time_limit = 5
    msg_dict = {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
            }
        }
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_info", mock_callback_request)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, 
                        "call", mock_consumer)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    consumer = loop.run_until_complete(system.get_consumer_status(
        "consumer_q", "consumer", msg_dict, time_limit, 0))
    consumer = consumer[0]
    
    assert consumer == {'val': 'Failed to make request', 'colour': 'PURPLE'}
    
    assert consumer["val"] == "Failed to make request"
    
    assert consumer["colour"] == "PURPLE"


def test_consumer_all_online(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if the output for all consumers online is correct
    
    # the 2 variables required to run the get_consumer_status function
    time_limit = 5
    msg_dict = {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
            }
        }
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_info", mock_callback)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, 
                        "call", mock_consumer)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    consumer = loop.run_until_complete(
        system.get_consumer_status(
            "consumer_q", "consumer", msg_dict, time_limit, 0))
    consumer = consumer[0]
    
    assert consumer == {"val": "All Consumers Online (5/5)", "colour": "GREEN"}
    
    assert consumer["val"] == "All Consumers Online (5/5)"
    
    assert consumer["colour"] == "GREEN"
    

def test_consumer_all_offline(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if the output for all consumers offline is correct
    
    # the 2 variables required to run the get_consumer_status function
    time_limit = 5
    msg_dict = {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
            }
        }
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_info", mock_callback)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, 
                        "call", mock_consumer)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    consumer = loop.run_until_complete(
        system.get_consumer_status(
            "consumer_q", "consumer", msg_dict, time_limit, 9))
    consumer = consumer[0]
    
    assert consumer == {
        "val": "All Consumers Offline (0/5)" , 
        "colour": "RED", 
        "failed": ["mock_tag_1", "mock_tag_2", 
                   "mock_tag_3", "mock_tag_4", "mock_tag_5"]
        }
    
    assert consumer["val"] == "All Consumers Offline (0/5)"
    
    assert consumer["colour"] == "RED"
    
    assert consumer["failed"] == ["mock_tag_1", "mock_tag_2", 
                   "mock_tag_3", "mock_tag_4", "mock_tag_5"]
    
    
    
def test_consumer_some_online(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if the output for some consumers online is correct
    
    # the 2 variables required to run the get_consumer_status function
    time_limit = 5
    msg_dict = {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
            }
        }
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_info", mock_callback)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, 
                        "call", mock_consumer)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    consumer = loop.run_until_complete(
        system.get_consumer_status(
            "consumer_q", "consumer", msg_dict, time_limit, 2))
    consumer = consumer[0]
    
    assert consumer == {
        "val": "Consumers Online (3/5)", 
        "colour": "ORANGE", 
        "failed": ["mock_tag_1", "mock_tag_2"]
        }
        
    assert consumer["val"] == "Consumers Online (3/5)"
    
    assert consumer["colour"] == "ORANGE"
    
    assert consumer["failed"] == ["mock_tag_1", "mock_tag_2"]
    
    
def test_consumer_none_running(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if the output for no consumers running is correct
    
    # the 2 variables required to run the get_consumer_status function
    time_limit = 5
    msg_dict = {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
            }
        }
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_info", mock_callback_offline)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, 
                        "call", mock_consumer)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    consumer = loop.run_until_complete(
        system.get_consumer_status(
            "consumer_q", "consumer", msg_dict, time_limit, 0))
    consumer = consumer[0]
    
    assert consumer == {
        "val": "All Consumers Offline (None running)", 
        "colour": "RED"
        }
    
    assert consumer["val"] == "All Consumers Offline (None running)"
    
    assert consumer["colour"] == "RED"


def test_slow_consumer_all_offline(monkeypatch, 
                                   loop: asyncio.AbstractEventLoop):
    # test if the output for all slow consumers offline is correct
    
    # the 2 variables required to run the get_consumer_status function
    time_limit = 1
    msg_dict = {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
            }
        }
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_info", mock_callback)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, 
                        "call", mock_slow_consumer)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    consumer = loop.run_until_complete(
        system.get_consumer_status(
            "consumer_q", "consumer", msg_dict, time_limit, 9))
    consumer = consumer[0]
    assert consumer == {
        "val": "All Consumers Offline (0/5)", 
        "colour": "RED", 
        "failed": ["mock_tag_1", "mock_tag_2", "mock_tag_3", 
                   "mock_tag_4", "mock_tag_5"]
        }
    
    assert consumer["val"] == "All Consumers Offline (0/5)"
    
    assert consumer["colour"] == "RED"

    assert consumer["failed"] == ["mock_tag_1", "mock_tag_2", "mock_tag_3", 
                                  "mock_tag_4", "mock_tag_5"]



"""
Testing the get() function in system.py
"""


async def mock_get_consumer_status(key, target, msg_dict, 
                                   time_limit, skip_num=0):
    # returns a dictionary value for get_consumer_status as a simple mock function
    
    mock_consumer_tags = ({
            "val": ("All Consumers Online (5/5)"), 
            "colour": "GREEN"
            }, 5, 5)
    return mock_consumer_tags


async def mock_green_consumer_status(key, target, msg_dict, 
                                   time_limit, skip_num=0):
    # returns a dictionary value for get_consumer_status as a simple mock function
    
    mock_consumer_tags = ({
            "val": ("All Consumers Online (5/5)"), 
            "colour": "GREEN"
            }, 1, 1)
    return mock_consumer_tags


async def mock_red_consumer_status(key, target, msg_dict, 
                                   time_limit, skip_num=0):
    # returns a dictionary value for get_consumer_status as a simple mock function
    # used to test if all consumers have failed
    
    consumers_fail = ["mock_tag_1", "mock_tag_2"]
    mock_consumer_tags = ({"val": ("All Consumers Offline (0/5)"), 
                          "colour": "RED", "failed": consumers_fail
                }, 1, 1)
    return mock_consumer_tags


async def mock_blue_consumer_status(key, target, msg_dict, 
                                   time_limit, skip_num=0):
    # returns a dictionary value for get_consumer_status as a simple mock function
    # used to test no consumers running
    
    mock_consumer_tags = ({
        "val": "All Consumers Offline (None running)", "colour": "RED"}, 1, 1)
    return mock_consumer_tags


def test_get_success(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if get function correctly runs
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_status", mock_get_consumer_status)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    get = loop.run_until_complete(system.get(Request, time_limit=5 ,microservice="all"))
    
    # gets the output as a dict to be easily manipulated
    attrs = (get.__dict__)
    
    # removes dictionary entries that would be too difficult to consistently test
    # e.g: HTML code that will keep updating
    attrs.pop('background')
    attrs.pop('body')
    attrs.pop('raw_headers')
    
    to_assert = ("{'template': <Template 'index.html'>, "
"'context': {'request': <class 'starlette.requests.Request'>, 'status': "
"{'monitor': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'catalog': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'nlds_worker': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'index': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'get_transfer': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'put_transfer': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'logger': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'archive_get': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'archive_put': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'failed': "
"{'failed_num': 0, 'failed_colour': 'alert-success'}}}, 'status_code': 200}")
    
    
    status = ("{'monitor': {'val': 'All Consumers Online (5/5)', "
"'colour': 'GREEN'}, "
"'catalog': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'nlds_worker': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'index': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'get_transfer': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'put_transfer': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'logger': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'archive_get': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'archive_put': {'val': 'All Consumers Online (5/5)', 'colour': 'GREEN'}, "
"'failed': {'failed_num': 0, 'failed_colour': 'alert-success'}}")
    
    assert str(attrs) == to_assert
    
    assert "template" in attrs
    
    assert isinstance(attrs["template"], jinja2.environment.Template)

    assert attrs["status_code"] == 200
    
    assert isinstance(attrs["context"]["request"], abc.ABCMeta)
    
    assert str(attrs["context"]["status"]) == status


def test_get_alert_green(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if get function correctly returns a green alert
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_status", 
                        mock_green_consumer_status)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    get = loop.run_until_complete(system.get(Request, time_limit=5 ,microservice="all"))
    
    # gets the output as a dict to be easily manipulated
    attrs = (get.__dict__)
    
    # retrieves the specific item to be tested from the dictionary
    failed = attrs["context"]["status"]["failed"]
    
    assert failed == {'failed_num': 0, 'failed_colour': 'alert-success'}
    
    assert failed["failed_num"] == 0
    
    assert failed["failed_colour"] == "alert-success"


def test_get_alert_red(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if get function correctly returns a red alert
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_status", mock_red_consumer_status)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    get = loop.run_until_complete(system.get(Request, time_limit=5 ,microservice="all"))
    
    # gets the output as a dict to be easily manipulated
    attrs = (get.__dict__)
    
    # retrieves the specific item to be tested from the dictionary
    failed = attrs["context"]["status"]["failed"]
    
    assert failed == {'failed_num': 18, 'failed_colour': 'alert-danger'}
    
    assert failed["failed_num"] == 18
    
    assert failed["failed_colour"] == "alert-danger"
    

def test_get_alert_blue(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if get function correctly returns a blue alert
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_status", mock_blue_consumer_status)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    get = loop.run_until_complete(system.get(Request, time_limit=5 ,microservice="all"))
    
    # gets the output as a dict to be easily manipulated
    attrs = (get.__dict__)
    
    # retrieves the specific item to be tested from the dictionary
    failed = attrs["context"]["status"]["failed"]
    
    assert failed == {'failed_num': 0, 'failed_colour': 'alert-info'}
    
    assert failed["failed_num"] == 0
    
    assert failed["failed_colour"] == "alert-info"


"""
Testing the get_consumer_info() function in system.py
"""

def mock_ignore(ignore):
    # used by the test in place of .json() which when ran with other monkeypatches
    # didn't work as the datatypes where different
    
    return ignore


def mock_get_request(*args, **kwargs):
    # returns a dictionary of what would be returned from requests.get as a mock
    
    dict_value = ({"consumer_details": [{"consumer_tag": "mock_tag_1"}, 
                                        {"consumer_tag": "mock_tag_2"}, 
                                        {"consumer_tag": "mock_tag_3"}, 
                                        {"consumer_tag": "mock_tag_4"}, 
                                        {"consumer_tag": "mock_tag_5"}]})
    return(dict_value)


def test_get_consumer_info_success(monkeypatch):
    # test if get_consumer_info function correctly runs
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(requests, "get", mock_get_request)
    monkeypatch.setattr(system, "convert_json", mock_ignore)
    
    key = "catalog_q"
    
    # gets the output from get_consumer_info to be tested on
    info = system.get_consumer_info("server", "port", key, "user", 
                                    "password", "vhost")
    
    assert info == ["mock_tag_1", "mock_tag_2", "mock_tag_3", 
                    "mock_tag_4", "mock_tag_5"]


"""
this tests the get_service_json function
"""


def test_get_service_json_success(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if get function correctly runs
    
    # replaces certain functions with mock functions that are a lot less 
    # complicated and return a simple value for what is being tested
    monkeypatch.setattr(system, "get_consumer_status", mock_get_consumer_status)
    
    # uses a pytest fixture to make an event loop that will run the asyncronus
    # function that is being called and store its output
    get = loop.run_until_complete(system.get_service_json(Request, "monitor", 3))
    
    del get["pid"]
    del get["hostname"]
    
    assert {"microservice_name":"monitor",
            "total_num":5,
            "num_failed":0,
            "num_success":5,
            "failed_list":[]} == get




# to test run python -m pytest tests/nlds/test_system_status.py in the top nlds directory