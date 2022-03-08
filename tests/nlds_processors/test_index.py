import json
from multiprocessing import connection

import pytest
import functools

from nlds.rabbit import publisher as publ
from nlds_processors.index import IndexerConsumer
from tests.conftest import default_rmq_body

def mock_load_config(template_config):
    return template_config

class MockIndexer(IndexerConsumer):
    def callback(self, ch, method, properties, body, connection):
        pass

@pytest.fixture()
def default_indexer(monkeypatch, template_config):
    # Ensure template is loaded instead of .server_config
    monkeypatch.setattr(publ, "load_config", functools.partial(mock_load_config, template_config))
    return MockIndexer()

def test_callback(monkeypatch, default_indexer, default_rmq_method, default_rmq_body):
    # Attempt to run callback with default message, should work!
    default_indexer.callback(None, default_rmq_method, None, default_rmq_body, None)

    # Attempt to run with a broken 

def test_index(default_indexer):
    default_indexer.index(['.'])