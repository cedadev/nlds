# encoding: utf-8
"""
details.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from collections import namedtuple
from enum import Enum
from typing import Optional, List, Dict, TypeVar, Any
from json import JSONEncoder
from pathlib import Path
import stat
import os
from os import stat_result
from urllib import parse as urlparse

from pydantic import BaseModel

from nlds.utils.permissions import check_permissions
import nlds.rabbit.message_keys as MSG
from nlds.errors import MessageError


# Patch the JSONEncoder so that a custom json serialiser can be run instead of
# of the default, if one exists. This patches for ALL json.dumps calls.
def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class PathType(Enum):
    FILE = 0
    DIRECTORY = 1
    LINK = 2
    # maintaining Enum numbering for database compatibility
    NOT_RECOGNISED = 5
    UNINDEXED = 6

    def __str__(self):
        return [
            "FILE",
            "DIRECTORY",
            "LINK",
            # empty strings to maintain DB compatibility
            "",
            "",
            "NOT_RECOGNISED",
            "UNINDEXED",
        ][self.value]


class PathLocation(BaseModel):
    storage_type: Optional[str] = None
    url_scheme: Optional[str] = None
    url_netloc: Optional[str] = None
    root: Optional[str] = None
    path: Optional[str] = None
    access_time: Optional[float] = None
    aggregation_id: Optional[int] = None

    def to_dict(self) -> Dict:
        return {
            "storage_type": self.storage_type,  # e.g. OBJECT_STORAGE or TAPE
            "url_scheme": self.url_scheme,  # e.g. http://
            "url_netloc": self.url_netloc,  # e.g. cedadev-o (tenancy)
            "root": self.root,  # e.g. <bucket_name>
            "path": self.path,  # e.g. /gws/cedaproc/file1.txt
            "access_time": self.access_time,
        }

    @classmethod
    def from_dict(cls, dictionary: Dict[str, Any]):
        return cls(
            storage_type=dictionary["storage_type"],
            url_scheme=dictionary["url_scheme"],
            url_netloc=dictionary["url_netloc"],
            root=dictionary["root"],
            path=dictionary["path"],
            access_time=dictionary["access_time"],
        )

    @property
    def url(self):
        return urlparse.urlunparse(
            [self.url_scheme, self.url_netloc, self.root + self.path, None, None, None]
        )


LocationType = TypeVar("LocationType", bound=PathLocation)


class PathLocations(BaseModel):
    count: Optional[int] = 0
    locations: Optional[List[LocationType]] = []

    def add(self, location: PathLocation) -> None:
        if location.storage_type in self.locations:
            raise MessageError(
                f"PathLocations already contains a PathLocation of the type "
                f"{location.storage_type}"
            )
        self.count += 1
        self.locations.append(location)

    def reset(self) -> None:
        self.count = 0
        self.locations.clear()

    def to_json(self) -> Dict:
        out_dict = {}
        for l in self.locations:
            out_dict[l.storage_type] = l.to_dict()
        return {MSG.STORAGE_LOCATIONS: out_dict}

    def has_storage_type(self, storage_type):
        """Determine whether the path locations contains a specific storage_type
        storage_type = MSG.OBJECT_STORAGE | MSG.TAPE"""
        for l in self.locations:
            if l.storage_type == storage_type:
                return True
        return False

    @classmethod
    def from_dict(cls, dictionary: Dict[str, Any]) -> None:
        pl = cls()
        d2 = dictionary[MSG.STORAGE_LOCATIONS]
        for d in d2:
            pl.add(PathLocation.from_dict(d2[d]))
        return pl


LocationsType = TypeVar("LocationsType", bound=PathLocations)


class PathDetails(BaseModel):
    original_path: Optional[str] = None
    path_type: Optional[PathType] = PathType.UNINDEXED
    link_path: Optional[str] = None
    size: Optional[int] = None
    user: Optional[int] = None
    group: Optional[int] = None
    mode: Optional[int] = None
    permissions: Optional[int] = None
    access_time: Optional[float] = None

    locations: Optional[LocationsType] = PathLocations()

    failure_reason: Optional[str] = None
    holding_id: Optional[int] = None

    @property
    def path(self) -> str:
        return Path(self.original_path)

    def to_json(self):
        return {
            "file_details": {
                "original_path": self.original_path,
                "path_type": self.path_type.value,
                "link_path": self.link_path,
                "size": self.size,
                "user": self.user,
                "group": self.group,
                "permissions": self.permissions,
                "mode": self.mode,
                "access_time": self.access_time,
                "failure_reason": self.failure_reason,
                "holding_id": self.holding_id,
            },
            **self.locations.to_json(),
        }

    @classmethod
    def from_dict(cls, json_contents: Dict[str, Any]):
        if MSG.STORAGE_LOCATIONS in json_contents:
            locations = PathLocations.from_dict(json_contents)
        else:
            locations = PathLocations()
        return cls(**json_contents["file_details"], locations=locations)

    @classmethod
    def from_path(cls, path: str):
        pd = cls(original_path=path)
        pd.stat()
        return pd

    @classmethod
    def from_filemodel(cls, file: BaseModel):
        """Create from a File model returned from the database."""
        # copy the basic info
        pd = cls()
        pd.original_path = file.original_path
        pd.path_type = file.path_type
        pd.link_path = file.link_path
        pd.size = file.size
        pd.user = file.user
        pd.group = file.group
        pd.permissions = file.file_permissions

        # copy the storage locations
        pd.locations = PathLocations()
        for fl in file.locations:
            pl = PathLocation()
            pl.storage_type = fl.storage_type.to_json()
            pl.url_scheme = fl.url_scheme
            pl.url_netloc = fl.url_netloc
            pl.root = fl.root
            pl.path = fl.path
            pl.access_time = fl.access_time.timestamp()
            pl.aggregation_id = fl.aggregation_id
            pd.locations.add(pl)

        return pd

    @classmethod
    def from_stat_result(cls, path: str, stat_result: stat_result):
        pd = cls(original_path=path)
        pd.stat(stat_result=stat_result)
        return pd

    def stat(self, stat_result: stat_result = None):
        if not stat_result:
            stat_result = self.path.lstat()

        # Include this check so mypy knows it's definitely a stat_result
        if stat_result is None:
            raise ValueError(f"stat_result is None for path {self.path}")

        self.mode = stat_result.st_mode  # only for internal use

        self.size = stat_result.st_size
        self.permissions = self.mode & 0o777
        self.user = stat_result.st_uid
        self.group = stat_result.st_gid
        self.access_time = stat_result.st_atime
        self.link_path = None
        if stat.S_ISLNK(self.mode):
            self.path_type = PathType.LINK
            self.link_path = self.path.resolve().as_posix() # convert this to a string
        elif stat.S_ISDIR(self.mode):
            self.path_type = PathType.DIRECTORY
        elif stat.S_ISREG(self.mode):
            self.path_type = PathType.FILE
        else:
            # Might be worth throwing an error here?
            self.path_type = PathType.NOT_RECOGNISED

    def get_stat_result(self):
        """Returns an approximation of an lstat() result with the appropriate
        metadata from the class. If self._mode has not been set then
        self.permissions will be returned instead.
        """
        StatResult = namedtuple(
            "StatResult", "st_mode st_uid st_gid st_atime " "st_size"
        )
        if not self.mode:
            mode = self.permissions
        else:
            mode = self.mode
        return StatResult(
            mode,
            self.user,
            self.group,
            self.access_time,
            self.size,
        )

    def check_permissions(self, uid: int, gid: int, access=os.R_OK):
        return check_permissions(
            uid,
            gid,
            access=access,
            path=self.original_path,
            stat_result=self.get_stat_result(),
        )

    def _get_location(self, location_type: str):
        """Get the PathLocation for the location_type (OBJECT_STORAGE|TAPE)."""
        # note - this only returns the first object - this is fine for now, but might
        # need amending if users want to use different tenancies
        for pl in self.locations.locations:
            if pl.storage_type == location_type:
                return pl
        return None

    def set_object_store(self, tenancy: str, bucket: str) -> None:
        """Set the OBJECT_STORAGE details for the file.
        This allows the object name to then be derived programmatically using a
        function, rather than munging the name every time it is used.
        The details for the PathLocation struct are:
            storage_type = "OBJECT_STORAGE"
            url_scheme = "http://"
            url_netloc = tenancy
            root = bucket = transaction_id
            path = original_path
        """
        # create the PathLocation and assign details to it
        pl = PathLocation(
            storage_type=MSG.OBJECT_STORAGE,
            url_scheme="http",
            url_netloc=tenancy,
            root=bucket,
            path=self.original_path,
        )
        self.locations.add(pl)
        return pl

    def get_object_store(self) -> PathLocation | None:
        """Get the PathLocation for the object storage file."""
        return self._get_location(MSG.OBJECT_STORAGE)

    @property
    def bucket_name(self) -> str | None:
        """Get the 1st object storage location and return the bucket by munging the string:
        bucket_name = f"nlds.{root}"
        """
        pl = self._get_location(MSG.OBJECT_STORAGE)
        if pl is None:
            return None
        else:
            bucket_name = f"nlds.{pl.root}"
            return bucket_name

    @property
    def object_name(self) -> str | None:
        """Get the 1st object storage location and return the object_name
        object_name = f"{location.path}"
        """
        pl = self._get_location(MSG.OBJECT_STORAGE)
        if pl is None:
            return None
        else:
            object_name = f"{pl.path}"
            return object_name

    def set_tape(self, server: str, tapepath: str, tarfile: str) -> None:
        """Set the TAPE details for the file.
        This allows the tape name to then be derived programmatically using a
        function, rather than munging the name every time it is used.
        The details for the PathLocation struct are:
            storage_type = "TAPE"
            url_scheme = "root://"
            url_netloc = server
            root = tapepath
            path = tarfile
        """
        # create the PathLocation and assign details to it
        pl = PathLocation(
            storage_type=MSG.TAPE,
            url_scheme="root",
            url_netloc=server,
            root=tapepath,
            path=tarfile,
        )
        self.locations.add(pl)
        return pl

    def get_tape(self) -> PathLocation | None:
        """Get the PathLocation for the tape file."""
        # note - this only returns the first object - this is fine for now, but might
        # need amending if users want to use different tenancies
        """Get the PathLocation for the object storage file."""
        return self._get_location(MSG.TAPE)

    @property
    def tape_name(self) -> str | None:
        pl = self._get_location(MSG.TAPE)
        if pl is None:
            return None
        else:
            tape_name = f"{pl.url_scheme}://{pl.url_netloc}/{pl.root}/{pl.path}"
            return tape_name
