import pytest
import asyncio
import time
import requests
import json

from fastapi import Request
from requests.auth import HTTPBasicAuth

from nlds.routers import system
from nlds.rabbit import rpc_publisher


"""
Testing the get_consumer_status() function in system.py
"""


def mock_callback(host_ip, api_port, queue_name, login, password, vhost):
    mock_consumer_tags = ["mock_tag_1", "mock_tag_2", "mock_tag_3", "mock_tag_4", "mock_tag_5"]
    return(mock_consumer_tags)

def mock_callback_offline(host_ip, api_port, queue_name, login, password, vhost):
    mock_consumer_tags = []
    return(mock_consumer_tags)

async def mock_consumer(*args, **kwargs):
    saved_args = locals()
    
    if saved_args["kwargs"]["msg_dict"]["details"]["ignore_message"] == False:
        return("existance")
    else:
        return
    
async def mock_slow_consumer(*args, **kwargs):
    saved_args = locals()
    time.sleep(2)
    if saved_args["kwargs"]["time_limit"] >= 2:
        return("existance")
    else:
        return
    

@pytest.fixture
def loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def test_consumer_all_online(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if the output for all consumers online is correct
    
    time_limit = 5
    msg_dict = {"details": {"api_action": "system_stat", "target_consumer": "", "ignore_message": False}}
    
    monkeypatch.setattr(system, "get_consumer_info", mock_callback)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, "call", mock_consumer)
    
    consumer = loop.run_until_complete(system.get_consumer_status("consumer_q", "consumer", msg_dict, time_limit, 0))
    assert consumer == {"val": "All Consumers Online (5/5)", "colour": "GREEN"}
    
    

def test_consumer_all_offline(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if the output for all consumers offline is correct
    
    time_limit = 5
    msg_dict = {"details": {"api_action": "system_stat", "target_consumer": "", "ignore_message": False}}
    
    monkeypatch.setattr(system, "get_consumer_info", mock_callback)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, "call", mock_consumer)
    
    consumer = loop.run_until_complete(system.get_consumer_status("consumer_q", "consumer", msg_dict, time_limit, 9))
    assert consumer == {"val": "All Consumers Offline (0/5)" , "colour": "RED", "failed": ["mock_tag_1", "mock_tag_2", "mock_tag_3", "mock_tag_4", "mock_tag_5"]}
    
    
    
def test_consumer_some_online(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if the output for some consumers online is correct
    
    time_limit = 5
    msg_dict = {"details": {"api_action": "system_stat", "target_consumer": "", "ignore_message": False}}
    
    monkeypatch.setattr(system, "get_consumer_info", mock_callback)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, "call", mock_consumer)
    
    consumer = loop.run_until_complete(system.get_consumer_status("consumer_q", "consumer", msg_dict, time_limit, 2))
    assert consumer == {"val": "Consumers Online (3/5)", "colour": "ORANGE", "failed": ["mock_tag_1", "mock_tag_2"]}
    
    
    
def test_consumer_none_running(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if the output for no consumers running is correct
    
    time_limit = 5
    msg_dict = {"details": {"api_action": "system_stat", "target_consumer": "", "ignore_message": False}}
    
    monkeypatch.setattr(system, "get_consumer_info", mock_callback_offline)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, "call", mock_consumer)
    
    consumer = loop.run_until_complete(system.get_consumer_status("consumer_q", "consumer", msg_dict, time_limit, 0))
    assert consumer == {"val": "All Consumers Offline (None running)", "colour": "RED"}



def test_slow_consumer_all_offline(monkeypatch, loop: asyncio.AbstractEventLoop):
    # test if the output for all slow consumers offline is correct
    
    time_limit = 1
    msg_dict = {"details": {"api_action": "system_stat", "target_consumer": "", "ignore_message": False}}
    
    monkeypatch.setattr(system, "get_consumer_info", mock_callback)
    monkeypatch.setattr(rpc_publisher.RabbitMQRPCPublisher, "call", mock_slow_consumer)
    
    consumer = loop.run_until_complete(system.get_consumer_status("consumer_q", "consumer", msg_dict, time_limit, 9))
    assert consumer == {"val": "All Consumers Offline (0/5)" , "colour": "RED", "failed": ["mock_tag_1", "mock_tag_2", "mock_tag_3", "mock_tag_4", "mock_tag_5"]}

    



"""
Testing the get() function in system.py
"""


async def mock_get_consumer_status(key, target, msg_dict, time_limit, skip_num=0):
    mock_consumer_tags = ["mock_tag_1", "mock_tag_2", "mock_tag_3", "mock_tag_4", "mock_tag_5"]
    return mock_consumer_tags


def test_get_success(monkeypatch, loop: asyncio.AbstractEventLoop):
    monkeypatch.setattr(system, "get_consumer_status", mock_get_consumer_status)
    
    get = loop.run_until_complete(system.get(Request))
    attrs = (get.__dict__)
    attrs.pop('background')
    attrs.pop('body')
    attrs.pop('raw_headers')
    
    assert str(attrs) == "{'template': <Template 'index.html'>, 'context': {'request': <class 'starlette.requests.Request'>, 'stats': {'monitor': ['mock_tag_1', 'mock_tag_2', 'mock_tag_3', 'mock_tag_4', 'mock_tag_5'], 'catalog': ['mock_tag_1', 'mock_tag_2', 'mock_tag_3', 'mock_tag_4', 'mock_tag_5'], 'nlds_worker': ['mock_tag_1', 'mock_tag_2', 'mock_tag_3', 'mock_tag_4', 'mock_tag_5'], 'index': ['mock_tag_1', 'mock_tag_2', 'mock_tag_3', 'mock_tag_4', 'mock_tag_5'], 'get_transfer': ['mock_tag_1', 'mock_tag_2', 'mock_tag_3', 'mock_tag_4', 'mock_tag_5'], 'put_transfer': ['mock_tag_1', 'mock_tag_2', 'mock_tag_3', 'mock_tag_4', 'mock_tag_5'], 'logger': ['mock_tag_1', 'mock_tag_2', 'mock_tag_3', 'mock_tag_4', 'mock_tag_5'], 'failed': {'failed_num': 0, 'failed_colour': 'alert-success'}}}, 'status_code': 200}"




"""
Testing the get_consumer_info() function in system.py
"""

def mock_ignore(ignore):
    return ignore

def mock_get_request(*args, **kwargs):
    dict_value = ({"consumer_details": [{"consumer_tag": "mock_tag_1"}, {"consumer_tag": "mock_tag_2"}, {"consumer_tag": "mock_tag_3"}, {"consumer_tag": "mock_tag_4"}, {"consumer_tag": "mock_tag_5"}]})
    return(dict_value)

def test_get_consumer_info_success(monkeypatch):
    monkeypatch.setattr(requests, "get", mock_get_request)
    monkeypatch.setattr(system, "convert_json", mock_ignore)
    
    key = "catalog_q"
    
    info = system.get_consumer_info("server", "port", key, "user", "password", "vhost")
    
    assert info == ["mock_tag_1", "mock_tag_2", "mock_tag_3", "mock_tag_4", "mock_tag_5"]


    
#do Neils steps on slack
#nlds-test to use pytest better than here (that will open the qs for you)
#read up on pytest and pytest fixtures



#await time.sleep           <-- done?

#make test for the get() in system.py, monkypatch for the get_consumer_status()     <-- done?

#write unit test for api call (get_consumer_info()), need to monkeypatch requests.get       <-- done?


#have a look at bootstrap (css thing) to make things look nice (centralised)