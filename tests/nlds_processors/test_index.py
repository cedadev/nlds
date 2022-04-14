import logging

import pytest
import functools

from nlds.rabbit import publisher as publ
from nlds_processors.index import IndexerConsumer

def mock_load_config(template_config):
    return template_config

@pytest.fixture()
def default_indexer(monkeypatch, template_config):
    # Ensure template is loaded instead of .server_config
    monkeypatch.setattr(publ, "load_config", functools.partial(mock_load_config, template_config))
    return IndexerConsumer()

def test_callback(monkeypatch, default_indexer, default_rmq_method, default_rmq_body):
    monkeypatch.setattr(default_indexer, "publish_message", lambda *_: None)
    
    # Attempt to run callback with default message, should work!
    default_indexer.callback(None, default_rmq_method, None, default_rmq_body, None)

def test_index(monkeypatch, caplog, default_indexer, default_rmq_message_dict):
    monkeypatch.setattr(default_indexer, "publish_message", lambda *_: None)
    # Let caplog capture all log messages
    caplog.set_level(logging.DEBUG)

    # Should work with any number of retries
    for i in range(8):
        default_indexer.index(['.', ], [i, ], 'test', default_rmq_message_dict)

    # Passing in a retrylist which doesn't match the length of filelist should 
    # produce a warning.
    default_indexer.index(['.', ], [8, 0, ], 'test', default_rmq_message_dict)
    # The 3rd oldest log message should be a warning, the penultimate should be 
    # a additional debug info message, the final should be a confirmation info 
    # message upon sending a problem list.
    assert caplog.records[-3].levelname == "WARNING"
    assert caplog.records[-2].levelname == "DEBUG"
    assert caplog.records[-1].levelname == "INFO"
    caplog.clear()

