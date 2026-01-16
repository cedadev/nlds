# encoding: utf-8
"""
statting_consumer.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Dict, List, NamedTuple, Any
import grp
import pwd
import pathlib as pth
import os

from retry import retry

from nlds.rabbit.consumer import RabbitMQConsumer as RMQC
import nlds.rabbit.message_keys as MSG
import nlds.rabbit.routing_keys as RK
from nlds.rabbit.state import State
from nlds.details import PathDetails
from nlds.utils.permissions import check_permissions


class StattingConsumer(RMQC):
    """
    SubClass of RabbitMQConsumer, the standard NLDS consumer, to isolate
    functionality required for statting files and checking file permissions.
    This is to separate out consumers which, or at least in principle can, stat
    each  file in a given filelist and check permissions (Transfer, Indexer),
    from those that have no need of this functionality (Catalog, Monitor).

    Inextricably linked to this is the checking of the output filelists
    (complete, failed) to split them more finely based on the result of
    the stat result (total message file size etc.). As such several refining
    functions are refactored here as well.
    """

    # The consumer configuration variables required for this consumer
    _FILELIST_MAX_SIZE = "filelist_max_size"
    _FILELIST_MAX_LENGTH = "filelist_max_length"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    _CHECK_FILESIZE = "check_filesize_fl"

    # The corresponding default values
    DEFAULT_CONSUMER_CONFIG = {
        _FILELIST_MAX_SIZE: 16 * 1024 * 1024,
        _FILELIST_MAX_LENGTH: 1024,
        _CHECK_FILESIZE: True,
    }

    def __init__(self, queue: str = None, setup_logging_fl: bool = False):
        super().__init__(queue=queue, setup_logging_fl=setup_logging_fl)

        # Member variables to temporarily hold user- and group-id of a message
        self.gids = None
        self.uid = None

        # Member variable to keep track of the total filesize of a message's
        # filelist
        self.completelist_size = 0
        self.filelist_max_size = StattingConsumer.DEFAULT_CONSUMER_CONFIG[
            StattingConsumer._FILELIST_MAX_SIZE
        ]
        self.filelist_max_len = StattingConsumer.DEFAULT_CONSUMER_CONFIG[
            StattingConsumer._FILELIST_MAX_LENGTH
        ]

    def reset(self) -> None:
        super().reset()
        self.gids = None
        self.uid = None
        self.completelist_size = 0

    def append_and_send(
        self,
        pathlist: List[PathDetails],
        path_details: PathDetails,
        routing_key: str,
        body_json: Dict[str, str],
        state: State = None,
    ) -> None:
        """Append a path details item to an existing PathDetails list and then
        determine if said list requires sending to the exchange due to size
        limits (can be either file size or list length).
        The PathDetails list is passed in as path_list

        The list will by default be capped by list-length, with the maximum
        determined by a filelist_max_length config variable (defaults to 1000).
        This behaviour is overriden by specifying the filesize kwarg, which is
        in kilobytes and must be an integer, at consumption time.

        The message body and destination should be specified through body_json
        and routing_key respectively.

        NOTE: This was refactored here from the index/transfer processors.
        Might make more sense to put it somewhere else given there are specific
        config variables required (filelist_max_length, filelist_max_size) which could
        fail with an AttributeError?
        NOTE 2: NRM refactored this to remove the FilelistType, and just pass the
        completed or failed list in by reference.  If we bring retries back then we
        can do this with the retry list as well.
        """
        # Select correct pathlist and append the given PathDetails object
        pathlist.append(path_details)

        # If filesize has been passed then use total list size as message cap
        if path_details.size:
            self.completelist_size += path_details.size

            # Send directly to exchange and reset filelist
            if self.completelist_size >= self.filelist_max_size:
                self.send_pathlist(pathlist, routing_key, body_json, state=state)
                pathlist.clear()
                self.completelist_size = 0

        # The default message cap is the length of the pathlist. This applies
        # to failed or problem lists by default
        elif len(pathlist) >= self.filelist_max_len:
            # Send directly to exchange and reset filelist
            self.send_pathlist(pathlist, routing_key, body_json, state=state)
            pathlist.clear()
            self.completelist_size = 0

    def _fail_all(
        self,
        filelist: List[PathDetails],
        rk_parts: List[str],
        body_json: Dict[str, Any],
        msg: str,
    ):
        # fail all the files in the filelist
        rk_transfer_failed = ".".join([rk_parts[0], rk_parts[1], RK.FAILED])
        for file in filelist:
            file.failure_reason = msg

        self.send_pathlist(filelist, rk_transfer_failed, body_json, state=State.FAILED)

    @retry((KeyError, ValueError), tries=-1, delay=2, backoff=2, max_delay=60)
    def set_ids(
        self,
        body_json: Dict[str, str],
    ) -> None:
        """Changes the real user- and group-ids stored in the class to that
        specified in the incoming message details section so that permissions
        on each file in a filelist can be checked.

        """
        # Use the retry mechanism to loop while trying to set the uids and gids.
        # This is to get around the pods in the Kubernetes Cluster taking a while to
        # read the LDAP configurations.

        # Attempt to get uid from, given username, in password db.
        try:
            username = body_json[MSG.DETAILS][MSG.USER]
            pwddata = pwd.getpwnam(username)
            pwd_uid = pwddata.pw_uid
            pwd_gid = pwddata.pw_gid
        except KeyError as e:
            self.log(f"Problem fetching uid using username {username}", RK.LOG_ERROR)
            raise e

        # Get list of groups containing the given username
        gids = {g.gr_gid for g in grp.getgrall() if username in g.gr_mem}
        # Add the gid from the pwd call.
        gids.add(pwd_gid)
        gids = list(gids)

        # Check for list validity
        if len(gids) == 0:
            raise ValueError(
                f"Problem fetching gid list, no matching gids "
                f"found. User name was {username} ({pwd_uid})"
            )

        self.uid = pwd_uid
        self.gids = gids

    def check_path_access(
        self, path: pth.Path, stat_result: NamedTuple = None, access: int = os.R_OK
    ) -> bool:
        """Checks that the given path is accessible, either by checking for its
        existence and by doing a permissions check on the file's bitmask. This requires
        a stat of the file, so one must be provided via stat_result else one is
        performed. The uid and gid of the user must be set in the object as well,
        usually by having performed RabbitMQConsumer.set_ids() beforehand.

        """
        if self.uid is None or self.gids is None or not isinstance(self.gids, list):
            raise ValueError("uid and gid not set properly.")

        if not isinstance(path, pth.Path):
            raise ValueError("No valid path object was given.")

        try:
            if not path.exists():
                # Can't access or stat something that doesn't exist
                check_path = False
            else:
                # If no stat result is passed through then get our own
                if stat_result is None:
                    stat_result = path.stat()
                check_path = check_permissions(
                    self.uid, self.gids, access=access, stat_result=stat_result
                )
        except PermissionError:
            check_path = False
        return check_path

    def check_path_exists(self, path: pth.Path) -> bool:
        if self.uid is None or self.gids is None or not isinstance(self.gids, list):
            raise ValueError("uid and gid not set properly.")

        if not isinstance(path, pth.Path):
            raise ValueError("No valid path object was given.")

        try:
            return path.exists()
        except PermissionError:
            # return True as the path exists but the check_path_access should follow
            # this call and return False
            return True
