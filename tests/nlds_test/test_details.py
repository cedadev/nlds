# encoding: utf-8
"""
test_details.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from pathlib import Path
import json

from nlds.details import PathDetails, PathLocations, PathLocation
from nlds.utils.permissions import check_permissions


def test_path_location():
    # check that the path location serialises
    location = PathLocation(
        storage_type="object_storage",
        url_scheme="https",
        url_netloc="cedadev-o",
        root="neils-bucket",
        path="file",
        access_time="now",
    )

    loc_dict = location.to_dict()
    test_loc = PathLocation.from_dict(loc_dict)
    assert location == test_loc

    # check that adding the location to the message serialises the locations correctly
    locations = PathLocations()
    assert locations.count == 0
    assert locations.locations == []

    # add the location and serialise to JSON
    locations.add(location)
    assert locations.count == 1
    locations_json = locations.to_json()
    test_locations = PathLocations.from_dict(locations_json)
    assert locations == test_locations


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
    message_dict = {"DATA": {"DATA_FILELIST": filelist}}
    byte_str = json.dumps(message_dict)
    loaded = json.loads(byte_str)
    pd_from_msg = PathDetails.from_dict(loaded["DATA"]["DATA_FILELIST"][0])
    assert pd_from_msg == pd

    # Test that creation from a stat_result is the same as the original object
    stat_result = Path(__file__).lstat()
    pd_from_stat = PathDetails.from_stat_result(__file__, stat_result=stat_result)
    assert pd_from_stat == pd

    # Similarly check that the from_path method creates an equivalent
    # path_details object
    pd_from_path = PathDetails.from_path(__file__)
    assert pd_from_path == pd

    # Check the approximated stat_result from the get_stat_result() method
    sr_from_pd = pd.get_stat_result()
    assert sr_from_pd.st_mode == stat_result.st_mode
    assert sr_from_pd.st_size == stat_result.st_size
    assert sr_from_pd.st_uid == stat_result.st_uid
    assert sr_from_pd.st_gid == stat_result.st_gid
    assert sr_from_pd.st_atime == stat_result.st_atime
    assert sr_from_pd != stat_result
    assert check_permissions(
        20,
        [
            100,
        ],
        path=__file__,
    ) == check_permissions(
        20,
        [
            100,
        ],
        stat_result=sr_from_pd,
    )

    # Check that from_dict() and to_json() work
    pd_json = pd.to_json()
    pd_from_json = PathDetails.from_dict(pd_json)
    assert pd == pd_from_json

    # Check contents of json?
    assert "file_details" in pd_json


def test_serialisation():
    """Test that the message is encoded / decoded as a whole"""
    pd = PathDetails(original_path=__file__)
    pd.stat()

    # add a location
    location = PathLocation(
        storage_type="object_storage",
        url_scheme="https",
        url_netloc="cedadev-o",
        root="neils-bucket",
        path="file",
        access_time="now",
    )

    # add the location
    pd.locations.add(location)

    # Attempt to make into an nlds-like message and then json dump/load it to
    # make sure everyhting works as it should
    filelist = [pd]
    message_dict = {"DATA": {"DATA_FILELIST": filelist}}
    byte_str = json.dumps(message_dict)
    loaded = json.loads(byte_str)
    pd_from_msg = PathDetails.from_dict(loaded["DATA"]["DATA_FILELIST"][0])
    assert pd_from_msg == pd


def test_object_name():
    pd = PathDetails(original_path=__file__)
    pd.stat()

    # add a location
    location = PathLocation(
        storage_type="object_storage",
        url_scheme="https",
        url_netloc="cedadev-o",
        root="neils-bucket",
        path="file",
        access_time="now",
    )
    pd.locations.add(location)
    os_loc = pd.object_name
    assert os_loc == "nlds.neils-bucket:file"


if __name__ == "__main__":
    test_path_details()
    test_path_location()
    test_serialisation()
    test_object_name()
