from collections import namedtuple
import logging
import os
from pathlib import Path

import pytest
import functools

from nlds.rabbit import publisher as publ
import nlds_processors.index as ind
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


def test_check_path_access(monkeypatch, default_indexer):
    # Point to non-existent test file
    p = Path("test.py")

    print(default_indexer.check_permissions_fl)
    # Should fail with improperly initialised uid and gid
    with pytest.raises(ValueError):
        default_indexer.check_path_access(p, access=os.R_OK)

    # Should work if we set uid and gid to some value (shouldn't matter which 
    # as this file doesn't exist so will return false)
    default_indexer.uid = 100
    default_indexer.gid = 100
    assert default_indexer.check_path_access(p, access=os.R_OK) == False

    # Set permissions checking to false for now
    default_indexer.check_permissions_fl = False

    monkeypatch.setattr(Path, "exists", lambda _: True)
    mp_exists = Path("test.py")
    assert default_indexer.check_path_access(mp_exists) == True
    
    monkeypatch.setattr(Path, "exists", lambda _: False)
    mp_no_exists = Path("test.py")
    assert default_indexer.check_path_access(mp_no_exists) == False

    # Now we test if checking permissions will work
    default_indexer.check_permissions_fl = True

    # Will need a mock stat result
    StatResult = namedtuple("StatResult", "st_mode st_uid st_gid")
    sr = StatResult(int(0o100400), 0, 0)

    # If exists and has permissions, should be true
    monkeypatch.setattr(ind, "check_permissions", 
                        lambda uid, gid, access=None, stat_result=None: True)
    monkeypatch.setattr(Path, "exists", lambda *_: True)
    mp_exists = Path("test.py")
    assert default_indexer.check_path_access(mp_exists, stat_result=sr) == True

    # If exists and doesn't have permissions, should be false as we're checking 
    # permissions!
    monkeypatch.setattr(ind, "check_permissions", 
                        lambda uid, gid, access=None, stat_result=None: False)
    monkeypatch.setattr(Path, "exists", lambda _: True)
    mp_no_exists = Path("test.py")
    assert default_indexer.check_path_access(mp_exists, stat_result=sr) == False

    # If we don't pass a stat_result at all, it should try to stat the file 
    # itself, thus failing because the file doesn't exist. Note that this isn't 
    # a normal mode of operation as we're explicitly setting exists to true. 
    with pytest.raises(FileNotFoundError):
        default_indexer.check_path_access(mp_exists)

    # Should fail with a value error if no valid path is given
    with pytest.raises(ValueError):
        default_indexer.check_path_access(None)
        default_indexer.check_path_access("test_file.py")
        default_indexer.check_path_access(1)