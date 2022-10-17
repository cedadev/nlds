from pathlib import Path
import json

import pytest

from nlds.details import PathDetails
from nlds.utils.permissions import check_permissions

def test_path_details():
    # Attempt to make a path details object
    pd = PathDetails(original_path=__file__)
    pd.stat()

    # Test that path property works as intended.
    assert isinstance(pd.path, Path)
    assert str(pd.path) == pd.original_path

    # Attempt to make into an nlds-like message and then json dump/load it to 
    # make sure everyhting works as it should
    filelist = [pd, pd]
    message_dict = {
        "DATA": {
            "DATA_FILELIST": filelist
        }
    }
    byte_str = json.dumps(message_dict)
    loaded = json.loads(byte_str)
    pd_from_msg = PathDetails.from_dict(loaded["DATA"]["DATA_FILELIST"][0])
    assert pd_from_msg == pd

    # Test that creation from a stat_result is the same as the original object
    stat_result = Path(__file__).lstat()
    pd_from_stat = PathDetails.from_stat(__file__, stat_result=stat_result)
    assert pd_from_stat == pd

    # Check the approximated stat_result from the get_stat_result() method
    sr_from_pd = pd.get_stat_result()
    assert sr_from_pd.st_mode == stat_result.st_mode
    assert sr_from_pd.st_size == stat_result.st_size
    assert sr_from_pd.st_uid == stat_result.st_uid
    assert sr_from_pd.st_gid == stat_result.st_gid
    assert sr_from_pd.st_atime == stat_result.st_atime
    assert sr_from_pd.st_mtime == stat_result.st_mtime
    assert sr_from_pd != stat_result
    assert (
        check_permissions(20, 100, path=__file__) 
        == check_permissions(20, 100, stat_result=sr_from_pd)
    )
