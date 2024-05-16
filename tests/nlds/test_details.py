from pathlib import Path
import json

from nlds.details import PathDetails, Retries, PathLocations, PathLocation
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


def test_retries():
    retries = Retries()

    # Check that incrementing without a reason works
    assert retries.count == 0
    assert len(retries.reasons) == 0
    retries.add()
    assert retries.count == 1
    assert len(retries.reasons) == 0

    # Check that reset actually resets the count
    retries.reset()
    assert retries.count == 0
    assert len(retries.reasons) == 0

    # Try incrementing with a reason
    retries.add(reason="Test retry")
    assert retries.count == 1
    assert len(retries.reasons) == 1

    # Try incrementing with another reason
    retries.add(reason="Different test reason")
    assert retries.count == 2
    assert len(retries.reasons) == 2

    # Check that reset does indeed work for a list of 2 reasons
    retries.reset()
    assert retries.count == 0
    assert len(retries.reasons) == 0

    # A None should be interpreted as 'not a reason' so shouldn't add to the
    # reasons list
    retries.add(reason=None)
    assert retries.count == 1
    assert len(retries.reasons) == 0

    # Convert to dict and check integrity of output.
    # Should be in an outer dict called 'retries'
    r_dict = retries.to_dict()
    assert "retries" in r_dict

    # Should contain a count and a reasons list
    assert "count" in r_dict["retries"]
    assert isinstance(r_dict["retries"]["count"], int)

    assert "reasons" in r_dict["retries"]
    assert isinstance(r_dict["retries"]["reasons"], list)

    # Attempt to make a Retries object from the dictionary
    new_retries = Retries.from_dict(r_dict)
    assert new_retries.count == 1
    assert len(new_retries.reasons) == 0

    # Attempt alternative constructor usage
    alt_retries = Retries(**r_dict["retries"])
    assert alt_retries.count == 1
    assert len(alt_retries.reasons) == 0

    assert new_retries == alt_retries


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
    assert "retries" in pd_json


def test_serialisation():
    """Test that the message is encoded / decoded as a whole"""
    pd = PathDetails(original_path=__file__)
    pd.stat()

    # add a retry
    pd.retries.add(reason="A test reason")

    # add a location
    location = PathLocation(
        storage_type = "object_storage",
        url_scheme = "https",
        url_netloc = "cedadev-o",
        root = "neils-bucket",
        path = "file",
        access_time="now"
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
    print(pd_from_msg)
    assert pd_from_msg == pd


if __name__ == "__main__":
    test_path_details()
    test_retries()
    test_path_location()
    test_serialisation()
