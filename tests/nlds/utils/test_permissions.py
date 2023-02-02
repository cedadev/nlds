from collections import namedtuple
import os
import pytest

from nlds.utils.permissions import check_permissions

def check_modes(stat_result, user, group, outcomes=(True, True, True)):
    """Convenience function for testing each of the three different modes of 
    access (os.R_OK, os.W_OK, os.X_OK) in one line for a specific user/group.
    
    """
    assert outcomes[0] ==  check_permissions(user, group, access=os.R_OK, 
                                             stat_result=stat_result)
    assert outcomes[1] ==  check_permissions(user, group, access=os.W_OK, 
                                             stat_result=stat_result)
    assert outcomes[2] ==  check_permissions(user, group, access=os.X_OK, 
                                             stat_result=stat_result)

def test_check_permissions():
    # Define the minimum stat_result named tuple
    StatResult = namedtuple("StatResult", "st_mode st_uid st_gid")


    ### User checks

    # Create a stat result for a file that's readable only by root
    sr = StatResult(int(0o100400), 0, 0)
    
    # Firstly, check that passing gids as a non-list breaks appropriately
    with pytest.raises(ValueError):
        check_modes(sr, 1000, 20, outcomes=[False, False, False])
        check_modes(sr, 1000, 'a', outcomes=[False, False, False])

    # Standard user checks should fail, even if in same group as root. 
    check_modes(sr, 1000, [20,], outcomes=[False, False, False])
    check_modes(sr, 1000, [0,], outcomes=[False, False, False])

    # Should work as root only if we ask for read check
    check_modes(sr, 0, [0,], outcomes=[True, False, False])


    # Create a file that's writable only by root
    sr = StatResult(int(0o100200), 0, 0)

    # Standard user checks should fail, even if in same group as root. 
    check_modes(sr, 1000, [20,], outcomes=[False, False, False])
    check_modes(sr, 1000, [0,], outcomes=[False, False, False])

    # Should work as root only if we ask for write check
    check_modes(sr, 0, [0,], outcomes=[False, True, False])

    # Should also work regardless of group
    check_modes(sr, 0, [20,], outcomes=[False, True, False])
    check_modes(sr, 0, [50,], outcomes=[False, True, False])
    check_modes(sr, 0, [100,], outcomes=[False, True, False])
    check_modes(sr, 0, [20,50,100,], outcomes=[False, True, False])


    # Create a file that's executable only by root
    sr = StatResult(int(0o100100), 0, 0)

    # Standard user checks should fail, even if in same group as root. 
    check_modes(sr, 1000, [20], outcomes=[False, False, False])
    check_modes(sr, 1000, [0,], outcomes=[False, False, False])

    # Should work as root only if we ask for execute check
    check_modes(sr, 0, [0,], outcomes=[False, False, True])

    # Should also work regardless of group
    check_modes(sr, 0, [20,], outcomes=[False, False, True])
    check_modes(sr, 0, [50,], outcomes=[False, False, True])
    check_modes(sr, 0, [100,], outcomes=[False, False, True])


    # Create a file that's full permissions for owner
    sr = StatResult(int(0o100700), 0, 0)

    # Standard user checks should fail, even if in same group as root. 
    check_modes(sr, 1000, [20,], outcomes=[False, False, False])
    check_modes(sr, 1000, [0,], outcomes=[False, False, False])

    # Should work as root in all cases
    check_modes(sr, 0, [0,], outcomes=[True, True, True])

    # Should also work regardless of group
    check_modes(sr, 0, [20,], outcomes=[True, True, True])
    check_modes(sr, 0, [50,], outcomes=[True, True, True])
    check_modes(sr, 0, [100,], outcomes=[True, True, True])


    # Create a file that's read & write permissions for owner
    sr = StatResult(int(0o100600), 0, 0)

    # Standard user checks should fail, even if in same group as root. 
    check_modes(sr, 1000, [20,], outcomes=[False, False, False])
    check_modes(sr, 1000, [0,], outcomes=[False, False, False])

    # Should work as root in all but the executable case
    check_modes(sr, 0, [0,], outcomes=[True, True, False])

    # Should also work regardless of group
    check_modes(sr, 0, [20,], outcomes=[True, True, False])
    check_modes(sr, 0, [50,], outcomes=[True, True, False])
    check_modes(sr, 0, [100,], outcomes=[True, True, False])


    # Create a file that's write & execute permissions for owner (unlikely?)
    sr = StatResult(int(0o100300), 0, 0)

    # Standard user checks should fail, even if in same group as root. 
    check_modes(sr, 1000, [20,], outcomes=[False, False, False])
    check_modes(sr, 1000, [0,], outcomes=[False, False, False])

    # Should work as root in all but the read case
    check_modes(sr, 0, [0,], outcomes=[False, True, True])

    # Should also work regardless of group
    check_modes(sr, 0, [20,], outcomes=[False, True, True])
    check_modes(sr, 0, [50,], outcomes=[False, True, True])
    check_modes(sr, 0, [100,], outcomes=[False, True, True])


    # Create a file that's read & execute permissions for owner
    sr = StatResult(int(0o100500), 0, 0)

    # Standard user checks should fail, even if in same group as root. 
    check_modes(sr, 1000, [20,], outcomes=[False, False, False])
    check_modes(sr, 1000, [0,], outcomes=[False, False, False])

    # Should work as root in all but the write case
    check_modes(sr, 0, [0,], outcomes=[True, False, True])

    # Should also work regardless of group
    check_modes(sr, 0, [20,], outcomes=[True, False, True])
    check_modes(sr, 0, [50,], outcomes=[True, False, True])
    check_modes(sr, 0, [100,], outcomes=[True, False, True])


    # Create a file that's null permissions for owner
    sr = StatResult(int(0o100000), 0, 0)

    # All checks should fail, even if in same group as root, even if owner of 
    # file.
    check_modes(sr, 1000, [20,], outcomes=[False, False, False])
    check_modes(sr, 1000, [0,], outcomes=[False, False, False])
    check_modes(sr, 0, [0,], outcomes=[False, False, False])
    check_modes(sr, 0, [20,], outcomes=[False, False, False])
    check_modes(sr, 0, [50,], outcomes=[False, False, False])
    check_modes(sr, 0, [100,], outcomes=[False, False, False])

    ### User + group checks

    # Create a file that's readable by user and group, created by a random user
    sr = StatResult(int(0o10440), 1000, 20)

    # Standard user owns, so should be able to read, regardless of 
    # group. 
    check_modes(sr, 1000, [20,], outcomes=[True, False, False])
    check_modes(sr, 1000, [0,], outcomes=[True, False, False])
    check_modes(sr, 1000, [50,], outcomes=[True, False, False])

    # Should work for a group user, but otherwise not accessible
    check_modes(sr, 1001, [20,], outcomes=[True, False, False])
    check_modes(sr, 1001, [10, 20, 50, 100], outcomes=[True, False, False])
    check_modes(sr, 1001, [21,], outcomes=[False, False, False])


    # Create a file that's readable, writable and executable by user and 
    # readable by group, created by a standard user
    sr = StatResult(int(0o10740), 1000, 20)

    # Standard user owns, so should be able to do everything, regardless of 
    # group. 
    check_modes(sr, 1000, [20,], outcomes=[True, True, True])
    check_modes(sr, 1000, [0,], outcomes=[True, True, True])
    check_modes(sr, 1000, [50,], outcomes=[True, True, True])

    # Should be readable for a group user, but otherwise not accessible at all  
    check_modes(sr, 1001, [20,], outcomes=[True, False, False])
    check_modes(sr, 1001, [21,], outcomes=[False, False, False])


    # Create a file that's readable, writable and executable by user, writable 
    # and readable by group, and readable and executable by other. Created by a 
    # standard user
    sr = StatResult(int(0o10764), 1000, 20)

    # Standard user owns, so should be able to do everything, regardless of 
    # group. 
    check_modes(sr, 1000, [20,], outcomes=[True, True, True])
    check_modes(sr, 1000, [0,], outcomes=[True, True, True])
    check_modes(sr, 1000, [50,], outcomes=[True, True, True])

    # Should be readable and writable for a group user 
    check_modes(sr, 1001, [20,], outcomes=[True, True, False])
    # For everyone else (i.e. not the owner, not in the same group) should be 
    # readable  
    check_modes(sr, 1001, [21,], outcomes=[True, False, False])

    ### That's enough for now, if we want this to be exhaustive then we probably 
    ### should do this programmatically. 