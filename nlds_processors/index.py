import json
import os
import pathlib as pth
from typing import List, NamedTuple, Dict

from nlds.rabbit.statting_consumer import StattingConsumer
from nlds.rabbit.publisher import RabbitMQPublisher as RMQP
from nlds.rabbit.consumer import State, FilelistType
from nlds.details import PathDetails

class IndexerConsumer(StattingConsumer):
    DEFAULT_QUEUE_NAME = "index_q"
    DEFAULT_ROUTING_KEY = (
        f"{RMQP.RK_ROOT}.{RMQP.RK_INDEX}.{RMQP.RK_WILD}"
    )
    DEFAULT_REROUTING_INFO = f"->INDEX_Q"
    DEFAULT_STATE = State.INDEXING

    # Possible options to set in config file
    _FILELIST_MAX_LENGTH = "filelist_max_length"
    _MESSAGE_MAX_SIZE = "message_threshold"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    _MAX_RETRIES = "max_retries"
    _CHECK_PERMISSIONS = "check_permissions_fl"
    _CHECK_FILESIZE = "check_filesize_fl"
    
    DEFAULT_CONSUMER_CONFIG = {
        _FILELIST_MAX_LENGTH: 1000,
        _MESSAGE_MAX_SIZE: 1000,    # in kB
        _PRINT_TRACEBACKS: False,
        _MAX_RETRIES: 5,
        _CHECK_PERMISSIONS: True,
        _CHECK_FILESIZE: True,
        RMQP.RETRY_DELAYS: RMQP.DEFAULT_RETRY_DELAYS,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        # Load config options or fall back to default values.
        self.filelist_max_len = self.load_config_value(
            self._FILELIST_MAX_LENGTH
        )
        self.message_max_size = self.load_config_value(
            self._MESSAGE_MAX_SIZE
        )
        self.print_tracebacks_fl = self.load_config_value(
            self._PRINT_TRACEBACKS
        )
        self.max_retries = self.load_config_value(
            self._MAX_RETRIES
        )
        self.check_permissions_fl = self.load_config_value(
            self._CHECK_PERMISSIONS
        )
        self.check_filesize_fl = self.load_config_value(self._CHECK_FILESIZE)
        self.retry_delays = self.load_config_value(self.RETRY_DELAYS)

        self.reset()
    
    def callback(self, ch, method, properties, body, connection):
        self.reset() 
        # Convert body from bytes to string for ease of manipulation
        body_json = json.loads(body)

        self.log(
            f"Received {json.dumps(body_json, indent=4)} from "
            f"{self.queues[0].name} ({method.routing_key})",
            self.RK_LOG_DEBUG
        )

        # Verify routing key is appropriate
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError as e:
            self.log("Routing key inappropriate length, exiting callback.", 
                     self.RK_LOG_ERROR)
            return
        
        filelist = self.parse_filelist(body_json)
        filelist_len = len(filelist)

        # Upon initiation, split the filelist into manageable chunks
        if rk_parts[2] == self.RK_INITIATE:
            self.split(filelist, rk_parts[0], body_json)
        # If for some reason a list which is too long has been submitted for
        # indexing, split it and resubmit it.             
        elif rk_parts[2] == self.RK_START:
            if filelist_len > self.filelist_max_len:
                self.split(filelist, rk_parts[0], body_json)    
            else:
                # First change user and group so file permissions can be 
                # checked. This should be deactivated when testing locally. 
                if self.check_permissions_fl:
                    self.set_ids(body_json)
            
                # Append routing info and then run the index
                body_json = self.append_route_info(body_json)
                self.log("Starting index scan", self.RK_LOG_INFO)

                # Index the entirety of the passed filelist and check for 
                # permissions. The size of the packet will also be evaluated
                # and used to send lists of roughly equal size.
                self.index(filelist, rk_parts[0], body_json)
                self.log(f"Scan finished.", self.RK_LOG_INFO)

    def split(self, filelist: List[PathDetails], rk_origin: str, 
              body_json: Dict[str, str]) -> None:
        """ Split the given filelist into batches of some configurable max 
        length and resubmit each to exchange for indexing proper.

        """
        rk_index = ".".join([rk_origin, self.RK_INDEX, self.RK_START])
        
        # Checking the length shouldn't fail as it's already been tested 
        # earlier in the callback
        filelist_len = len(filelist)

        if filelist_len > self.filelist_max_len:
            self.log(
                f"Filelist longer than allowed maximum length, splitting into "
                 "batches of {self.filelist_max_len}",
                self.RK_LOG_DEBUG
            )
        
        # For each 1000 files in the list resubmit with index as the action 
        # in the routing key
        for i in range(0, filelist_len, self.filelist_max_len):
            slc = slice(i, min(i + self.filelist_max_len, filelist_len))
            self.send_pathlist(
                filelist[slc], rk_index, body_json, mode=FilelistType.processed,
                state=State.SPLITTING
            )
        
    def index(self, raw_filelist: List[NamedTuple], rk_origin: str, 
              body_json: Dict[str, str]):
        """Indexes a list of PathDetails. 
        
        Each IndexItem is a named tuple consisting of an item (a file or 
        directory) and an associated number of attempted accesses. This function
        checks if each item exists, fully walking any directories and 
        subdirectories in the process, and then checks permissions on each 
        available file. All accessible files are added to an 'indexed' list and 
        sent back to the exchange for transfer once that list has reached a 
        pre-configured size (default 1000MB) or the end of IndexItem list has 
        been reached, whichever comes first. 
        
        If any item cannot be found, indexed or accessed then it is added to a 
        'problem' list for another attempt at indexing. If a maximum number of 
        retries is reached and the item has still not been indexed then it is 
        added to a final 'failed' list which is sent back to the exchange so the
        user can be informed via monitoring.

        :param List[NamedTuple] raw_filelist:  List of IndexItems containing 
            paths to files or indexable directories and the number of times each 
            has been attempted to be indexed. 
        :param str rk_origin:   The first section of the received message's 
            routing key which designates its origin.
        :param dict body_json:  The message body in dict form.

        """
        rk_complete = ".".join([rk_origin, self.RK_INDEX, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_INDEX, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_INDEX, self.RK_FAILED])
        
        # Checking the lengths of file- and reset- lists is no longer necessary

        for path_details in raw_filelist:
            item_p = path_details.path

            # If any items has exceeded the maximum number of retries we add it 
            # to the dead-end failed list
            if path_details.retries.count > self.max_retries:
                # Append to failed list (in self) and send back to exchange if 
                # the appropriate size. 
                self.log(f"{path_details.path} has exceeded max retry count, "
                         "adding to failed list.", self.RK_LOG_DEBUG)
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="failed"
                )
                
                # Skip to next item and avoid access logic
                continue

            # Check if item is (a) fully resolved, and (b) exists
            # TODO: I think this is, at best, redundant and, at worst, 
            # dangerous. Should be removed in a future commit.
            root = pth.Path("/")
            if root not in item_p.parents:
                item_p = item_p.resolve()

            # If item does not exist, or is not accessible, add to problem list
            if not self.check_path_access(item_p):
                # Increment retry counter and add to retry list
                reason = (f"Path:{path_details.path} is inaccessible.")
                self.log(reason, self.RK_LOG_DEBUG)
                path_details.retries.increment(reason=reason)
                self.append_and_send(
                    path_details, rk_retry, body_json, list_type="retry"
                )

            elif item_p.is_dir():
                # Index directories by walking them
                for directory, dirs, subfiles in os.walk(item_p):
                    # Loop through dirs and remove from walk if not accessible
                    directory_path = pth.Path(directory)
                    dirs[:] = [d for d in dirs 
                               if self.check_path_access(directory_path / d)]

                    # Loop through subfiles and append each to appropriate 
                    # output filelist
                    for f in subfiles:
                        f_path = directory_path / f

                        # We create a new PathDetails for each walked file 
                        # with a zeroed retry counter.
                        walk_path_details = PathDetails(
                            original_path=str(f_path)
                        )

                        # Grab stat early - we need it later if checking file
                        # sizes
                        stat_result = None
                        if self.check_filesize_fl:
                            stat_result = f_path.stat()
                            walk_path_details.stat(stat_result=stat_result)

                        # Check if given user has read or write access 
                        if self.check_path_access(f_path, 
                                                  stat_result=stat_result):
                            # Use the stat_results to check for filesize size 
                            # (in kilobytes)
                            filesize = None
                            if self.check_filesize_fl:
                                filesize = stat_result.st_size / 1000

                            # Pass the size through to ensure maximum size is 
                            # used as the partitioning metric (if checking file
                            # size)
                            self.append_and_send(walk_path_details, rk_complete, 
                                                 body_json, list_type="indexed", 
                                                 filesize=filesize)

                        else:
                            # If file is not valid, not accessible with uid and 
                            # gid if checking permissions, and not existing 
                            # otherwise, then add to problem list. Note that we 
                            # can't check the size of the problem list as files 
                            # may not exist
                            reason = (
                                f"Path:{walk_path_details.path} is inaccessible."
                            )
                            self.log(reason, self.RK_LOG_DEBUG)
                            walk_path_details.retries.increment(reason=reason)
                            self.append_and_send(
                                walk_path_details, rk_retry, body_json, 
                                list_type="retry"
                            )
            
            # Index files directly in exactly the same way as above
            elif item_p.is_file(): 
                # Stat the file to check for size, if checking (in kilobytes)
                filesize = None
                if self.check_filesize_fl:
                    path_details.stat()
                    filesize = path_details.size

                # Pass the size through to ensure maximum size is 
                # used as the partitioning metric
                self.append_and_send(path_details, rk_complete, 
                                     body_json, list_type="indexed", 
                                     filesize=filesize)
            else:
                reason = f"Path:{path_details.path} is of unknown type."
                self.log(reason, self.RK_LOG_DEBUG)
                path_details.retries.increment(reason=reason)
                self.append_and_send(
                    path_details, rk_retry, body_json, list_type="retry"
                )
        
        # Send whatever remains after all directories have been walked
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist, rk_complete, body_json, mode="indexed"
            )
        if len(self.retrylist) > 0:
            self.send_pathlist(
                self.retrylist, rk_retry, body_json, mode="retry"
            )
        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist, rk_failed, body_json, mode="failed"
            )

    def check_path_access(self, path: pth.Path, stat_result: NamedTuple = None, 
                          access: int = os.R_OK) -> bool:
        return super().check_path_access(
            path, 
            stat_result, 
            access, 
            self.check_permissions_fl
        )


def main():
    consumer = IndexerConsumer()
    consumer.run()

if __name__ == "__main__":
    main()