import json
import os
import pwd
import re
import stat
import pathlib as pth
from typing import List, Tuple
import traceback

from nlds.rabbit.consumer import RabbitMQConsumer
from nlds.rabbit.publisher import RabbitMQPublisher

class IndexerConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "index_q"
    DEFAULT_ROUTING_KEY = (
        f"{RabbitMQPublisher.RK_ROOT}.{RabbitMQPublisher.RK_INDEX}."
        f"{RabbitMQPublisher.RK_WILD}"
    )
    DEFAULT_REROUTING_INFO = f"->INDEX_Q"

    # Possible options to set in config file
    _FILELIST_MAX_LENGTH = "filelist_max_length"
    _MESSAGE_MAX_SIZE = "message_threshold"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    _MAX_RETRIES = "max_retries"
    
    DEFAULT_CONSUMER_CONFIG = {
        _FILELIST_MAX_LENGTH: 1000,
        _MESSAGE_MAX_SIZE: 1000,
        _PRINT_TRACEBACKS: False,
        _MAX_RETRIES: 5,
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
        self.print_tracebacks = self.load_config_value(
            self._PRINT_TRACEBACKS
        )
        self.max_retries = self.load_config_value(
            self._MAX_RETRIES
        )

        print(f"@__init__ - uid: {os.getuid()}, gid: {os.getgid()}")

    def load_config_value(self, config_option: str, 
                          path_listify_fl: bool = False):
        """
        Function for verification and loading of options from the indexer 
        section of the .server_config file. Attempts to load from the config 
        section and reverts to hardcoded default value if an error is 
        encountered. Will not attempt to load an option if no default value is 
        available. 

        :param config_option:   (str) The option in the indexer section of the 
                                .server_config file to be verified and loaded.
        :param path_listify:    (boolean) Optional argument to control whether 
                                value should be treated as a list and each item 
                                converted to a pathlib.Path() object. 
        :returns:   The value at config_option, otherwise the default value as 
                    defined in IndexerConsumer.DEFAULT_CONSUMER_CONFIG

        """
        # Check if the given config option is valid (i.e. whether there is an 
        # available default option)
        if config_option not in self.DEFAULT_CONSUMER_CONFIG:
            raise ValueError(
                f"Configuration option {config_option} not valid.\n"
                f"Must be one of {list(self.DEFAULT_CONSUMER_CONFIG.keys())}"
            )
        else:
            return_val = self.DEFAULT_CONSUMER_CONFIG[config_option]

        if config_option in self.consumer_config:
            try:
                return_val = self.consumer_config[config_option]
                if path_listify_fl:
                    # TODO: (2022-02-17) This is very specific to the use-case 
                    # here, could potentially be divided up into listify and 
                    # convert functions, but that's probably only necessary if 
                    # we refactor this into Consumer – which is probably a good 
                    # idea when we start fleshing out other consumers
                    return_val_list = self.consumer_config[config_option]
                    # Make sure returned value is a list and not a string
                    # Note: it can't be any other iterable because it's loaded 
                    # from a json
                    assert isinstance(return_val_list, list)
                    return_val = [pth.Path(item) for item in return_val_list] 
            except KeyError:
                self.log(f"Invalid value for {config_option} in config file. "
                         f"Using default value instead.", self.RK_LOG_WARNING) 

        return return_val
    
    def callback(self, ch, method, properties, body, connection):
        try:
            print(f"@callback.start - uid: {os.getuid()}, gid: {os.getgid()}")

            # Convert body from bytes to string for ease of manipulation
            body_json = json.loads(body)

            self.log(
                f"Received {body} from {self.queues[0].name} "
                f"({method.routing_key})",
                self.RK_LOG_DEBUG
            )

            # Verify routing key is appropriate
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
                self.log(
                    "Routing key inappropriate length, exiting callback.", 
                    self.RK_LOG_ERROR
                )
                return
            
            # Verify filelist is, in fact, a list
            filelist = list(body_json[self.MSG_DATA][self.MSG_FILELIST])
            try:
                filelist_len = len(filelist)
            except TypeError as e:
                self.log(
                    "Filelist cannot be split into sublist, incorrect format "
                    "given.", 
                    self.RK_LOG_ERROR
                )
                raise e
            
            # Verify retrylist is, in fact, a list
            retrylist = list(
                body_json[self.MSG_DATA][self.MSG_FILELIST_RETRIES]
            )
            try:
                retrylist_len = len(retrylist)
            except TypeError as e:
                self.log(
                    "Retrylist does not appear to be a list", 
                    self.RK_LOG_ERROR
                )
                raise e
            
            if retrylist_len != filelist_len:
                self.log(
                    "Lengths of filelist and retrylist do not match, retrylist "
                    "will be reset", 
                    self.RK_LOG_WARNING
                )

            # Upon initiation, split the filelist into manageable chunks
            if rk_parts[2] == self.RK_INITIATE:
                self.split(filelist, retrylist, rk_parts[0], body_json)
            # If for some reason a list which is too long has been submitted for
            # indexing, split it and resubmit it.             
            elif (rk_parts[2] == self.RK_INDEX and 
                  filelist_len > self.filelist_max_len):
                self.split(filelist, retrylist, rk_parts[0], body_json)    
            # Otherwise index the filelist
            elif rk_parts[2] == self.RK_INDEX:
                # First change user and group so file permissions can be checked
                # self.change_user(body_json)
                
                # Append routing info and then run the index
                body_json = self.append_route_info(body_json)
                self.log("Starting index scan", self.RK_LOG_INFO)

                # Index the entirety of the passed filelist and check for 
                # permissions. The size of the packet will also be evaluated and
                # used to send lists of roughly equal size.
                self.index(filelist, retrylist, rk_parts[0], body_json)

            self.log(f"Scan finished.", self.RK_LOG_INFO)
            print(f"@callback.end - uid: {os.getuid()}, gid: {os.getgid()}")

        except (ValueError, TypeError, KeyError, PermissionError) as e:
            if self.print_tracebacks:
                tb = traceback.format_exc()
                self.log(tb, self.RK_LOG_DEBUG)
            self.log(
                f"Encountered error ({e}), sending to logger.", 
                self.RK_LOG_ERROR, exc_info=e
            )
            body_json[self.MSG_DATA][self.MSG_ERROR] = str(e)
            new_routing_key = ".".join(
                [self.RK_ROOT, self.RK_LOG, self.RK_LOG_INFO]
            )
            self.publish_message(new_routing_key, json.dumps(body_json))

    def change_user(self, body_json):
        """Changes the real user- and group-ids to that specified in the 
        incoming message details section so that permissions on each file can 
        be checked.

        """
        # Attempt to get group id and user id
        try:
            username = body_json[self.MSG_DETAILS][self.MSG_USER]
            pwddata = pwd.getpwnam(username)
            req_uid = pwddata.pw_uid
            req_gid = pwddata.pw_gid
        except KeyError as e:
            self.log(
                f"Problem fetching user and group id using username "
                 "{username}", self.RK_LOG_ERROR
            )
            raise e

        # Set real user & group ids so os.access can be used
        try:
            os.setuid(req_uid)
            os.setgid(req_gid)
        except PermissionError as e:
            self.log(
                f"Attempted to use uid or gid outside of permission scope "
                f"({req_uid}, {req_gid})", self.RK_LOG_ERROR
            )
            raise e
        
    def split(self, filelist: List[str], retrylist: List[int], rk_origin: str, 
              body_json: dict[str]) -> None:
        """ Split the given filelist into batches of 1000 and resubmit each to 
        exchange for indexing proper.

        """
        rk_index = ".".join([rk_origin, self.RK_INDEX, self.RK_INDEX])
        if retrylist is None:
            retrylist = body_json[self.MSG_DATA][self.MSG_FILELIST_RETRIES]
        
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
            self.send_list(
                filelist[slc], retrylist[slc], rk_index, 
                body_json, mode="split"
            )

    def index(self, filelist: List[str], retrylist: List[int], rk_origin: str, 
              body_json: dict[str, str]):
        """
        Iterates through a filelist, checking if each exists, walking any 
        directories and then checking permissions on each available file. All 
        accessible files are added to an indexed list and sent once that list 
        has reached a set size (default 1000MB) or the end of filelist has been 
        reached, whichever comes first. 
        
        If any item cannot be found, indexed or accessed then it is added to a 
        'problem' list for another attempt at indexing. If a maximum number of 
        retries is reached and the item has still not been indexed then it is 
        added to a final 'failed' list which is sent back to the exchange so the
        user can be informed via monitoring.

        :param List[str] filelist:  List of paths to files or indexable 
                                    directories
        :param List[int] retrylist: List of the number of times each item from 
                                    filelist has been retried. 
        :param str rk_origin:   The first section of the received message's 
                                routing key which designates its origin.
        :param dict body_json:  The message body in dict form.

        """
        indexed_filelist = []
        indexed_retrylist = []
        indexed_size = 0
        problem_filelist = []
        problem_retrylist = []
        failed_filelist = []
        failed_retrylist = []
        
        print(f"@index.start - uid: {os.getuid()}, gid: {os.getgid()}")

        rk_complete = ".".join([rk_origin, self.RK_INDEX, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_INDEX, self.RK_INDEX])
        rk_failed = ".".join([rk_origin, self.RK_INDEX, self.RK_FAILED])
        
        # If retry list is not set then create one
        if retrylist is None or len(retrylist) != len(filelist):
            self.log("Passed retrylist is either not set or malformed, "
                     "resetting...", 
                     self.RK_LOG_WARNING)
            self.log(f"retrylist is {retrylist}", self.RK_LOG_DEBUG)
            retrylist = [0 for _ in filelist]

        for i, item in enumerate(filelist):
            item_p = pth.Path(item)

            # If any items has exceeded the maximum number of retries we add it 
            # to the dead-end failed list
            if retrylist[i] > self.max_retries:
                failed_filelist.append(item)
                failed_retrylist.append(retrylist[i])

                # If failed list exceeds max list length then we send it to 
                # the exchange
                if len(failed_filelist) >= self.filelist_max_len:
                    self.send_list(
                        failed_filelist, failed_retrylist,
                        rk_failed, body_json, mode="failed"
                    )
                    failed_filelist = []
                    failed_retrylist = []
                continue

            # Check if item is (a) fully resolved, and (b) exists
            root = pth.Path("/")
            if root not in item_p.parents:
                item_p = item_p.resolve()
            # If item does not exist, add to problem list
            if not item_p.exists():
                # Add to problem lists
                problem_filelist.append(item)
                problem_retrylist.append(retrylist[i] + 1)

                # We don't check the size of the problem list as files may not 
                # exist
                if len(problem_filelist) >= self.filelist_max_len:
                    self.send_list(
                        problem_filelist, problem_retrylist, 
                        rk_retry, body_json, mode="problem"
                    )
                    problem_filelist = []
                    problem_retrylist = []
                continue

            if item_p.is_dir():
                # Index directories by walking them
                for directory, _, subfiles in os.walk(item_p):
                    # Loop through subfiles and append each to output filelist, 
                    # checking at each appension whether the message list 
                    # length 
                    # threshold is breached and yielding appropriately
                    for f in subfiles:
                        # TODO: (2022-04-06) Calling both os.stat and os.access 
                        # here, probably a more efficient way of doing this but 
                        # access does checks that stat does not... 
                        # Check if given user has read or write access 
                        if os.access(f, os.R_OK):
                            # Add the file to the list and then check for 
                            # message size 
                            indexed_filelist.append(os.path.join(directory, f))
                            indexed_retrylist.append(retrylist[i])
                            
                            # Stat the file to check for size
                            indexed_size += f.stat().st_size
                            if indexed_size >= self.message_max_size:
                                # Send directly to exchange and reset filelist
                                self.send_list(
                                    indexed_filelist, indexed_retrylist, 
                                    rk_complete, body_json
                                )
                                indexed_filelist = []
                                indexed_retrylist = []
                                indexed_size = 0
                        else:
                            # if not accessible with uid and gid then add to 
                            # problem list
                            problem_filelist.append(item)
                            problem_retrylist.append(retrylist[i] + 1)

                            # We don't check the size of the problem list as 
                            # files may not exist
                            if len(problem_filelist) >= self.filelist_max_len:
                                self.send_list(
                                    problem_filelist, problem_retrylist, 
                                    rk_retry, body_json, mode="problem"
                                )
                                problem_filelist = []
                                problem_retrylist = []
            
            # Index files directly in exactly the same way as above
            elif item_p.is_file(): 
                # Check if given user has read or write access 
                if os.access(f, os.R_OK):
                    # Add the file to the list and then check for message size 
                    indexed_filelist.append(item)
                    indexed_retrylist.append(retrylist[i])
                    
                    # Stat the file to check for size
                    indexed_size += f.stat().st_size
                    if indexed_size >= self.message_max_size:
                        # Send directly to exchange and reset filelist
                        self.send_list(
                            indexed_filelist, indexed_retrylist, 
                            rk_complete, body_json
                        )
                        indexed_filelist = []
                        indexed_retrylist = []
                        indexed_size = 0
                else:
                    # if not accessible with uid and gid then add to problem 
                    # list
                    problem_filelist.append(item)
                    problem_retrylist.append(retrylist[i] + 1)

                    # We don't check the size of the problem list as files may 
                    # not exist
                    if len(problem_filelist) >= self.filelist_max_len:
                        self.send_list(
                            problem_filelist, problem_retrylist, rk_retry, 
                            body_json, mode="problem"
                        )
                        problem_filelist = []
                        problem_retrylist = []
        
        # Send whatever remains after all directories have been walked
        print(f"@index.start - uid: {os.getuid()}, gid: {os.getgid()}")

        if len(indexed_filelist) > 0:
            self.send_list(
                indexed_filelist, indexed_retrylist, rk_complete, body_json
            )
        if len(problem_filelist) > 0:
            self.send_list(
                problem_filelist, problem_retrylist, rk_retry, body_json, 
                mode="problem"
            )
        if len(failed_filelist) > 0:
            self.send_list(
                failed_filelist, failed_retrylist, rk_failed, body_json, 
                mode="failed"
            )
    
    def send_list(self, filelist: List[str], retrylist: List[str], 
                  routing_key: str, body_json: dict[str, str], 
                  mode: str = "indexed"):
        """ Convenience function which sends the given filelist and retry list 
        to the exchange with the given routing key and message body. Mode simply
        specifies what to put into the log message.

        """
        self.log(f"Sending {mode} list back to exchange", self.RK_LOG_INFO)
        body_json[self.MSG_DATA][self.MSG_FILELIST] = filelist
        body_json[self.MSG_DATA][self.MSG_FILELIST_RETRIES] = retrylist
        self.publish_message(routing_key, json.dumps(body_json))

def main():
    consumer = IndexerConsumer()
    consumer.run()

if __name__ == "__main__":
    main()