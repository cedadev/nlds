from multiprocessing.sharedctypes import Value
import os
from typing import NamedTuple


ACCESSES = (
    os.R_OK,    # =4 
    os.W_OK,    # =2
    os.X_OK,    # =1
)

def check_permissions(uid: int, gid: int, access=os.R_OK, path: str = None,
                      stat_result: NamedTuple = None) -> bool:
    # TODO: (2022-07-25) Should we be adding an exception for root whereby any 
    # check returns True? Probably not super necessary as it's very unlikely to 
    # be actually used by root in our context, but is technically incomplete 
    # otherwise.

    if access not in ACCESSES:
        raise ValueError("Invalid access bit passed, must be one of "
                         f"{ACCESSES}.")

    if stat_result is None and path is not None:
        stat_result = os.lstat(path)
    elif stat_result is None and path is None:
        raise ValueError("Neither path nor a valid stat result of a path were "
                         "given so cannot continue. One is required.")
    
    # Get file permissions mask from stat result
    mode = stat_result.st_mode & 0o777
    if uid != stat_result.st_uid and gid != stat_result.st_gid:
        # Check other permissions, bitwise-and the file permissions mask with 
        # the appropriate access mask.
        return bool((access) & mode)
    elif uid != stat_result.st_uid and gid == stat_result.st_gid:
        # Check group permissions, Multiplied by 8 to shift the access bit 1 
        # place to the left in octary (e.g. 040 for group read)
        return bool((access * 8) & mode)
    elif uid == stat_result.st_uid:
        # Check user permissions. Multiplied by 64 to shift the access bit 2 
        # places to the left in octary (e.g. 400 for group read)
        return bool((access * 64) & mode)
    else:
        # I don't think it's possible to end up here but return false just in 
        # case
        return False