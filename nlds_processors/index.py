# encoding: utf-8
"""
index.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import json
import os
import pathlib
from typing import List, NamedTuple, Dict, Any

from nlds.rabbit.statting_consumer import StattingConsumer
from nlds.rabbit.consumer import State
from nlds.details import PathDetails
import nlds.rabbit.routing_keys as RK
from nlds.errors import MessageError


class IndexError(MessageError):
    pass


class IndexerConsumer(StattingConsumer):
    DEFAULT_QUEUE_NAME = "index_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}.{RK.INDEX}.{RK.WILD}"
    DEFAULT_REROUTING_INFO = f"->INDEX_Q"
    DEFAULT_STATE = State.INDEXING

    # Possible options to set in config file
    _FILELIST_MAX_LENGTH = "filelist_max_length"
    _MESSAGE_MAX_SIZE = "message_threshold"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    _CHECK_FILESIZE = "check_filesize_fl"
    _MAX_FILESIZE = "max_filesize"

    DEFAULT_CONSUMER_CONFIG = {
        _FILELIST_MAX_LENGTH: 1000,
        _MESSAGE_MAX_SIZE: 16 * 1000 * 1000,  # in kB
        _PRINT_TRACEBACKS: False,
        _CHECK_FILESIZE: True,
        _MAX_FILESIZE: (500 * 1000 * 1000),  # in kB, default=500GB
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        # Load config options or fall back to default values.
        self.filelist_max_len = self.load_config_value(self._FILELIST_MAX_LENGTH)
        self.message_max_size = self.load_config_value(self._MESSAGE_MAX_SIZE)
        self.print_tracebacks_fl = self.load_config_value(self._PRINT_TRACEBACKS)
        self.check_filesize_fl = self.load_config_value(self._CHECK_FILESIZE)
        self.max_filesize = self.load_config_value(self._MAX_FILESIZE)

        self.reset()

    def _split(
        self, filelist: List[PathDetails], rk_origin: str, body_json: Dict[str, Any]
    ) -> None:
        """Split the given filelist into batches of some configurable max
        length and resubmit each to exchange for indexing proper.

        """
        rk_index = ".".join([rk_origin, RK.INDEX, RK.START])

        # Checking the length shouldn't fail as it's already been tested
        # earlier in the callback
        filelist_len = len(filelist)

        if filelist_len > self.filelist_max_len:
            self.log(
                f"Filelist longer than allowed maximum length, splitting into "
                "batches of {self.filelist_max_len}",
                RK.LOG_DEBUG,
            )

        # For each 1000 files in the list resubmit with index as the action
        # in the routing key
        for i in range(0, filelist_len, self.filelist_max_len):
            slc = slice(i, min(i + self.filelist_max_len, filelist_len))
            self.send_pathlist(
                filelist[slc],
                rk_index,
                body_json,
                state=State.SPLITTING,
            )

    def _scan(
        self,
        filelist: List[PathDetails],
        rk_parts: List[str],
        body_json: Dict[str, Any],
    ) -> None:
        # First change user and group so file permissions can be checked
        self.set_ids(body_json)

        # Append routing info and then run the index
        body_json = self.append_route_info(body_json)
        self.log("Starting index scan", RK.LOG_INFO)

        # Index the entirety of the passed filelist and check for permissions. The size
        # of the packet will also be evaluated and used to send lists of roughly equal
        # size.
        self.index(filelist, rk_parts[0], body_json)
        self.log(f"Scan finished.", RK.LOG_INFO)

    def callback(self, ch, method, properties, body, connection):
        self.reset()
        # Convert body from bytes to string for ease of manipulation
        body_json = json.loads(body)

        self.log(
            f"Received from {self.queues[0].name} ({method.routing_key})",
            RK.LOG_DEBUG,
            body_json=body_json,
        )

        # Check for system status
        if self._is_system_status_check(body_json=body_json, properties=properties):
            return

        # Verify routing key is appropriate
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError as e:
            self.log(
                "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
            )
            return

        # parse the input filelist from the JSON passed in via the HTTP body
        filelist = self.parse_filelist(body_json)
        filelist_len = len(filelist)

        # Upon initiation, split the filelist into manageable chunks
        if rk_parts[2] == RK.INITIATE:
            self._split(filelist, rk_parts[0], body_json)
        # If for some reason a list which is too long has been submitted for
        # indexing, split it and resubmit it.
        elif rk_parts[2] == RK.START:
            if filelist_len > self.filelist_max_len:
                self._split(filelist, rk_parts[0], body_json)
            else:
                self._scan(filelist, rk_parts, body_json)

    def _index_r(
        self,
        item_path: PathDetails,
        rk_complete: str,
        rk_failed: str,
        body_json: Dict[str, str],
    ):
        """Recursively index the item_path"""
        # check that there is sufficient permission to access the item_path
        try:
            if not self.check_path_access(item_path.path):
                error_reason = f"Path:{item_path.path} is inaccessible."
                raise IndexError(message=error_reason)
            if item_path.path.is_dir():
                # item is a directory - list what is in the directory
                sub_file_list = os.listdir(item_path.path)
                # process and send via recursion
                for sf in sub_file_list:
                    root_path = pathlib.Path(item_path.path)
                    new_item_path = PathDetails(original_path=str(root_path / sf))
                    self._index_r(
                        new_item_path,
                        rk_complete=rk_complete,
                        rk_failed=rk_failed,
                        body_json=body_json,
                    )

            elif item_path.path.is_file():
                # item is a file - stat it
                item_path.stat()
                # check the filesize
                if self.check_filesize_fl and item_path.size > self.max_filesize:
                    error_reason = (
                        f"Filesize: {item_path.size / 1000}MB for path: {item_path.path}"
                        f"is too big for the NLDS tape system."
                        f" The max allowed file size is {self.max_filesize / 1000}MB."
                    )
                    raise IndexError(message=error_reason)
                # add to the complete list - use append_and_send to subdivide if
                # necessary
                self.append_and_send(
                    self.completelist,  # the list to add to
                    item_path,
                    routing_key=rk_complete,
                    body_json=body_json,
                    state=State.INDEXING,
                )
            else:
                # item is unknown
                error_reason = f"Path:{item_path.path} is of unknown type."
                raise IndexError(message=error_reason)
        except IndexError as ie:
            # add to the failed list - use append_and_send to subdivide if necessary
            item_path.failure_reason = ie.message
            self.append_and_send(
                self.failedlist,
                item_path,
                routing_key=rk_failed,
                body_json=body_json,
                state=State.FAILED,
            )

    def index(
        self, raw_filelist: List[PathDetails], rk_origin: str, body_json: Dict[str, Any]
    ) -> None:
        """Indexes a list of PathDetails.
            :param List[PathDetails] raw_filelist:  List of PathDetails containing
                paths to files or indexable directories and the number of times each
                has been attempted to be indexed.
            :param str rk_origin:   The first section of the received message's
                routing key which designates its origin.
            :param dict body_json:  The message body in dict form.
        This function checks if each item exists, fully walking any directories and
        subdirectories in the process, and then checks permissions on each
        available file. All accessible files are added to an 'indexed' list and
        sent back to the exchange for transfer once that list has reached a
        pre-configured size (default 1000MB) or the end of PathDetails list has
        been reached, whichever comes first.

        If any item cannot be found, indexed or accessed then it is added to a
        'failed' list to inform the user that it failed.
        """
        rk_complete = ".".join([rk_origin, RK.INDEX, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.INDEX, RK.FAILED])

        for item_path in raw_filelist:
            # all errors will now be handled by raising an IndexError in the _index_r
            # function
            self._index_r(item_path, rk_complete, rk_failed, body_json=body_json)

        # finalise the pathlists - anything left in the completed and failed lists
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body_json,
                state=State.INDEXING,
            )

        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body_json,
                state=State.FAILED,
            )


def main():
    consumer = IndexerConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
