from collections import namedtuple
import os

import pytest
import functools

from nlds.rabbit import publisher as publ
import nlds.rabbit.consumer as cons
from nlds.details import PathDetails
from nlds_processors.index import IndexerConsumer

def mock_load_config(template_config):
    return template_config

@pytest.fixture()
def default_indexer(monkeypatch, template_config):
    # Ensure template is loaded instead of .server_config
    monkeypatch.setattr(publ, "load_config", functools.partial(
        mock_load_config, 
        template_config
        )
    )
    return IndexerConsumer()

def test_callback(monkeypatch, default_indexer, default_rmq_method, 
                  default_rmq_body):
    monkeypatch.setattr(default_indexer, "publish_message", lambda *_: None)
    
    # Attempt to run callback with default message, should work!
    default_indexer.callback(None, default_rmq_method, None, default_rmq_body, 
                             None)

def test_index(monkeypatch, caplog, default_indexer, 
               default_rmq_message_dict, fs):
    # Deactivate messaging for test environment and initialise uid and gid
    monkeypatch.setattr(default_indexer, "publish_message", 
                        lambda *_args, **_kwargs: None)
    default_indexer.reset()
    default_indexer.uid = 100
    default_indexer.gid = 100

    # Should fail upon trying to unpack the namedtuple
    with pytest.raises(AttributeError):
        default_indexer.index([("/", 0), ], 'test', default_rmq_message_dict)
    default_indexer.reset()
    default_indexer.uid = 100
    default_indexer.gid = 100


    dirs = [
        "/test/1-1/2-1/3-1/4-1/5-1",
        "/test/1-1/2-1/3-2",
        "/test/1-1/2-1/3-3",
        "/test/1-2/2-2/3-4",
        "/test/1-3/2-3",
        "/test/1-4/2-4/3-3/4-2/5-2/6-1",
    ]
    files = [
        "/test/1-1/2-1/3-1/test-1.txt",
        "/test/1-1/2-1/3-2/test-2.txt",
        "/test/1-1/2-2/3-4/test-3.txt",
        "/test/1-4/2-4/3-3/4-2/5-2/6-1/test-4.txt",
    ]
    for d in dirs:
        fs.create_dir(d)
    for f in files:
        fs.create_file(f)


    expected_filelist = [
        PathDetails(original_path="/test/1-1/2-1/3-1/test-1.txt"),
        PathDetails(original_path="/test/1-1/2-1/3-2/test-2.txt"),
        PathDetails(original_path="/test/1-1/2-2/3-3/test-3.txt"),
        PathDetails(original_path="/test/1-4/2-4/3-3/4-2/5-2/6-1/test-4.txt"),
    ]

    # Should work with any number of retries under the limit
    for i in range(default_indexer.max_retries):
        test_filelist = [PathDetails(original_path="/test/", retries=i)]
        default_indexer.index(test_filelist, 'test', default_rmq_message_dict)

        assert len(default_indexer.completelist) == len(expected_filelist)
        assert len(default_indexer.retrylist) == 0
        assert len(default_indexer.failedlist) == 0

        default_indexer.reset()
        default_indexer.uid = 100
        default_indexer.gid = 100
    
    # All files should be in failed list with any number of retries over the 
    # limit
    for i in range(default_indexer.max_retries + 1, 10):
        test_filelist = [PathDetails(original_path="/test/", retries=i)]
        default_indexer.index(test_filelist, 'test', default_rmq_message_dict)

        assert len(default_indexer.completelist) == 0
        assert len(default_indexer.retrylist) == 0
        assert len(default_indexer.failedlist) == 1   # length of initial list!

        default_indexer.reset()
        default_indexer.uid = 100
        default_indexer.gid = 100



def test_check_path_access(monkeypatch, default_indexer):
    """This remains here bacuse it was first written here, the tests are still 
    valid for the wrapper class.
    """
    from pathlib import Path

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
    monkeypatch.setattr(cons, "check_permissions", 
                        lambda uid, gid, access=None, stat_result=None: True)
    monkeypatch.setattr(Path, "exists", lambda *_: True)
    mp_exists = Path("test.py")
    assert default_indexer.check_path_access(mp_exists, stat_result=sr) == True

    # If exists and doesn't have permissions, should be false as we're checking 
    # permissions!
    monkeypatch.setattr(cons, "check_permissions", 
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