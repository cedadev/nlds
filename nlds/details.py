from collections import namedtuple
from enum import Enum
from typing import NamedTuple, Optional, List, Dict
from datetime import datetime
import json
from pathlib import Path
import stat
import os

from pydantic import BaseModel

from .utils.permissions import check_permissions

class PathType(Enum):
    FILE = 0
    DIRECTORY = 1
    LINK_UNCLASSIFIED = 2
    LINK_COMMON_PATH = 3
    LINK_ABSOLUTE_PATH = 4
    NOT_RECOGNISED = 5

class PathDetails(BaseModel):
    original_path: str
    nlds_object: Optional[str]
    size: Optional[int]
    user: Optional[int]
    group: Optional[int]
    _mode: Optional[int]
    permissions: Optional[int]
    access_time: Optional[datetime]
    modify_time: Optional[datetime]
    path_type: Optional[str]
    link_path: Optional[PathType]
    retries: Optional[int] = 0
    retry_reasons: Optional[List[str]] = []

    @property
    def path(self) -> str:
        return Path(self.original_path)

    def to_json(self):
        return json.dumps(self.to_msg_dict(), default=str, sort_keys=True)

    def to_msg_dict(self):
        return { 
            "file_details": {
                "original_path": self.original_path,
                "nlds_object": self.nlds_object,
                "size": self.size,
                "user": self.user,
                "group": self.group,
                "permissions": self.permissions,
                "access_time": self.access_time,
                "modify_time": self.modify_time,
                "path_type": self.path_type,
                "link_path": self.link_path,  
            },
            "retries": self.retries,
            "retry_reasons": self.retry_reasons
        }
    
    @classmethod
    def from_dict(cls, json_contents: Dict[str, str]):
        return cls(**json_contents['file_details'], 
                   retries=json_contents["retries"],
                   retry_reasons=json_contents["retry_reasons"])
    
    @classmethod
    def from_path(cls, path: str):
        pd = cls(original_path=path)
        return pd.stat()

    @classmethod
    def from_stat(cls, path: str, stat_result: NamedTuple):
        pd = cls(original_path=path)
        return pd.stat(stat_result=stat_result)

    def stat(self, stat_result: NamedTuple = None):
        if not stat_result:
            stat_result = self.path.lstat()

        self._mode = stat_result.st_mode

        self.size = stat_result.st_size / 1000 # in kB
        self.permissions = self._mode & 0o777
        self.user = stat_result.st_uid
        self.group = stat_result.st_gid
        self.access_time = datetime.fromtimestamp(stat_result.st_atime)
        self.modify_time = datetime.fromtimestamp(stat_result.st_mtime)
        self.link_path = None
        if (stat.S_ISLNK(self._mode)):
            self.path_type = PathType.LINK_UNCLASSIFIED
            self.link_path = self.path.resolve()
        elif (stat.S_ISDIR(self._mode)):
            self.path_type = PathType.DIRECTORY
        elif (stat.S_ISREG(self._mode)):
            self.path_type = PathType.FILE
        # TODO (2022-08-18): Implement a way of detecting absolute/common paths
        else:
            # Might be worth throwing an error here?
            self.path_type = PathType.NOT_RECOGNISED
    
    def get_stat_result(self):
        """Returns an approximation of an lstat() result with the appropriate 
        metadata from the class. If self._mode has not been set then 
        self.permissions will be returned instead.
        """
        StatResult = namedtuple("StatResult", 
                                "st_mode st_uid st_gid st_atime st_mtime "
                                "st_size")
        if not self._mode:
            mode = self.permissions
        else:
            mode = self._mode
        return StatResult(mode, self.uid, self.gid, 
                          self.access_time.timestamp, 
                          self.modify_time.timestamp,
                          self.size * 1000)

    def check_permissions(self, uid: int, gid: int, access=os.R_OK):
        return check_permissions(uid, gid, access=access, 
                                 path=self.original_path, 
                                 stat_result=self.get_stat_result())

    def increment_retry(self):
        self.retries += 1 

