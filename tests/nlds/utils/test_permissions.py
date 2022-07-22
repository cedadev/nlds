from collections import namedtuple
import os
import pytest

from nlds.utils.permissions import check_permissions

def test_check_permissions():
    StatResult = namedtuple("StatResult", "st_mode st_uid st_gid")

    # Create a file that's readable only by root
    sr = StatResult(int(0o100400), 0, 0)
    
    # Standard user check should fail, even if in same group as root. 
    assert not check_permissions(1000, 20, access=os.R_OK, stat_result=sr)
    assert not check_permissions(1000, 0, access=os.R_OK, stat_result=sr)
    # Should work as root!
    assert check_permissions(0, 0, access=os.R_OK, stat_result=sr)

    # Should work as root only if we ask for read check
    assert check_permissions(0, 0, access=os.R_OK, stat_result=sr)
    assert not check_permissions(0, 0, access=os.W_OK, stat_result=sr)
    assert not check_permissions(0, 0, access=os.X_OK, stat_result=sr)

    # Create a file that's writable only by root
    sr = StatResult(int(0o100200), 0, 0)

    for access in [os.R_OK, os.W_OK, os.X_OK]:
        assert not check_permissions(1000, 20, access=access, stat_result=sr)
        assert not check_permissions(1000, 0, access=access, stat_result=sr)

    # Should work as root only if we ask for write check
    assert not check_permissions(0, 0, access=os.R_OK, stat_result=sr)
    assert check_permissions(0, 0, access=os.W_OK, stat_result=sr)
    assert not check_permissions(0, 0, access=os.X_OK, stat_result=sr)

    # Should also work regardless of group
    assert check_permissions(0, 50, access=os.W_OK, stat_result=sr)
    assert check_permissions(0, 20, access=os.W_OK, stat_result=sr)


    # Create a file that's Executable only by root
    sr = StatResult(int(0o100100), 0, 0)

    # Shouldn't work for non-root users in any context
    for access in [os.R_OK, os.W_OK, os.X_OK]:
        assert not check_permissions(1000, 20, access=access, stat_result=sr)
        assert not check_permissions(1000, 0, access=access, stat_result=sr)

    # Should work as root only if we ask for executable check
    assert not check_permissions(0, 0, access=os.R_OK, stat_result=sr)
    assert not check_permissions(0, 0, access=os.W_OK, stat_result=sr)
    assert check_permissions(0, 0, access=os.X_OK, stat_result=sr)

    # Should also work regardless of group
    for grp in range(1, 100):
        assert check_permissions(0, grp, access=os.X_OK, stat_result=sr)


    # # Create a file that's readable by user and group
    # sr = StatResult(int(0o10440), 1000, 20)

    # # Should work for user in
    # for access in [os.R_OK, os.W_OK, os.X_OK]:
    #     assert not check_permissions(1000, 20, access=access, stat_result=sr)
    #     assert not check_permissions(1000, 0, access=access, stat_result=sr)

    # # Should work as root only if we ask for executable check
    # assert not check_permissions(0, 0, access=os.R_OK, stat_result=sr)
    # assert not check_permissions(0, 0, access=os.W_OK, stat_result=sr)
    # assert check_permissions(0, 0, access=os.X_OK, stat_result=sr)

    # # Should also work regardless of group
    # for grp in range(1, 100):
    #     assert check_permissions(0, grp, access=os.X_OK, stat_result=sr)