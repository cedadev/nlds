from typing import Dict, Union, List, NamedTuple
import grp
import pwd
import pathlib as pth
import os

from nlds.rabbit.consumer import RabbitMQConsumer as RMQC
from nlds.rabbit.consumer import FilelistType
import nlds.rabbit.message_keys as MSG
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.delays as DLY
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
    (complete, retry, failed) to split them more finely based on the result of
    the stat result (total message file size etc.). As such several refining
    functions are refactored here as well.
    """

    # The consumer configuration variables required for this consumer
    _FILELIST_MAX_LENGTH = "filelist_max_length"
    _MESSAGE_MAX_SIZE = "message_threshold"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    _MAX_RETRIES = "max_retries"
    _CHECK_PERMISSIONS = "check_permissions_fl"
    _CHECK_FILESIZE = "check_filesize_fl"

    # The corresponding default values
    DEFAULT_CONSUMER_CONFIG = {
        _FILELIST_MAX_LENGTH: 1000,
        _MESSAGE_MAX_SIZE: 1000,  # in kB
        _CHECK_PERMISSIONS: True,
        _CHECK_FILESIZE: True,
        DLY.RETRY_DELAYS: DLY.DEFAULT_RETRY_DELAYS,
    }

    def __init__(self, queue: str = None, setup_logging_fl: bool = False):
        super().__init__(queue=queue, setup_logging_fl=setup_logging_fl)

        # Member variables to temporarily hold user- and group-id of a message
        self.gids = None
        self.uid = None

        # Memeber variable to keep track of the total filesize of a message's
        # filelist
        self.completelist_size = 0

    def reset(self) -> None:
        super().reset()
        self.gids = None
        self.uid = None
        self.completelist_size = 0

    def _choose_list(
        self, list_type: FilelistType = FilelistType.processed
    ) -> List[PathDetails]:
        """Choose the correct pathlist for a given mode of operation. This
        requires that the appropriate member variable be instantiated in the
        consumer class.
        """
        if list_type == FilelistType.processed:
            return self.completelist
        elif list_type == FilelistType.retry:
            return self.retrylist
        elif list_type == FilelistType.failed:
            return self.failedlist
        else:
            raise ValueError(f"Invalid list type provided ({list_type})")

    def append_and_send(
        self,
        path_details: PathDetails,
        routing_key: str,
        body_json: Dict[str, str],
        filesize: int = None,
        list_type: Union[FilelistType, str] = FilelistType.processed,
    ) -> None:
        """Append a path details item to an existing PathDetails list and then
        determine if said list requires sending to the exchange due to size
        limits (can be either file size or list length). Which pathlist is to
        be used is determined by the list_type passed, which can be either a
        FilelistType enum or an appropriate string that can be cast into one.

        The list will by default be capped by list-length, with the maximum
        determined by a filelist_max_length config variable (defaults to 1000).
        This behaviour is overriden by specifying the filesize kwarg, which is
        in kilobytes and must be an integer, at consumption time.

        The message body and destination should be specified through body_json
        and routing_key respectively.

        NOTE: This was refactored here from the index/transfer processors.
        Might make more sense to put it somewhere else given there are specific
        config variables required (filelist_max_length, message_max_size) which
        could fail with an AttributeError?

        """
        # If list_type given as a string then attempt to cast it into an
        # appropriate enum
        if not isinstance(list_type, FilelistType):
            try:
                list_type = FilelistType[list_type]
            except KeyError:
                raise ValueError(
                    "list_type value invalid, must be a "
                    "FilelistType enum or a string capabale of "
                    f"being cast to such (list_type={list_type})"
                )

        # Select correct pathlist and append the given PathDetails object
        pathlist = self._choose_list(list_type)
        pathlist.append(path_details)

        # If filesize has been passed then use total list size as message cap
        if filesize:
            # NOTE: This references a general pathlist but a specific list size,
            # perhaps these two should be combined together into a single
            # pathlist object? Might not be necessary for just this small code
            # snippet.
            self.completelist_size += filesize

            # Send directly to exchange and reset filelist
            if self.completelist_size >= self.message_max_size:
                self.send_pathlist(pathlist, routing_key, body_json, mode=list_type)
                pathlist.clear()
                self.completelist_size = 0

        # The default message cap is the length of the pathlist. This applies
        # to failed or problem lists by default
        elif len(pathlist) >= self.filelist_max_len:
            # Send directly to exchange and reset filelist
            self.send_pathlist(pathlist, routing_key, body_json, mode=list_type)
            pathlist.clear()

    def set_ids(
        self,
        body_json: Dict[str, str],
    ) -> None:
        """Changes the real user- and group-ids stored in the class to that
        specified in the incoming message details section so that permissions
        on each file in a filelist can be checked.

        """
        # Attempt to get uid from, given username, in password db
        try:
            username = body_json[MSG.DETAILS][MSG.USER]
            pwddata = pwd.getpwnam(username)
            pwd_uid = pwddata.pw_uid
            pwd_gid = pwddata.pw_gid
        except KeyError as e:
            self.log(
                f"Problem fetching uid using username {username}", RK.LOG_ERROR
            )
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
        self,
        path: pth.Path,
        stat_result: NamedTuple = None,
        access: int = os.R_OK,
        check_permissions_fl: bool = True,
    ) -> bool:
        """Checks that the given path is accessible, either by checking for its
        existence or, if the check_permissions_fl is set, by doing a permissions
        check on the file's bitmask. This requires a stat of the file, so one
        must be provided via stat_result else one is performed. The uid and gid
        of the user must be set in the object as well, usually by having
        performed RabbitMQConsumer.set_ids() beforehand.

        """
        if check_permissions_fl and (
            self.uid is None or self.gids is None or not isinstance(self.gids, list)
        ):
            raise ValueError("uid and gid not set properly.")

        if not isinstance(path, pth.Path):
            raise ValueError("No valid path object was given.")

        if not path.exists():
            # Can't access or stat something that doesn't exist
            return False
        elif check_permissions_fl:
            # If no stat result is passed through then get our own
            if stat_result is None:
                stat_result = path.stat()
            return check_permissions(
                self.uid, self.gids, access=access, stat_result=stat_result
            )
        else:
            return True
