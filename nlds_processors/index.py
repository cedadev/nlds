import json
import os
import pathlib as pth
from typing import List
import traceback

from nlds.rabbit.consumer import RabbitMQConsumer
from nlds.rabbit.publisher import RabbitMQPublisher

class IndexerConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "index_q"
    DEFAULT_ROUTING_KEY = f"{RabbitMQPublisher.RK_ROOT}.{RabbitMQPublisher.RK_INDEX}.{RabbitMQPublisher.RK_WILD}"
    DEFAULT_REROUTING_INFO = f"->INDEX_Q"

    # Possible options to set in config file
    _FILELIST_THRESHOLD = "filelist_threshold"
    _MESSAGE_THRESHOLD = "message_threshold"
    _PRINT_TRACEBACKS = "print_tracebacks_fl"
    _SCANNABLE_ROOT_DIRS = "scannable_root_dirs"
    
    DEFAULT_ROOT_DIRS = (
        pth.Path('/gws'),
        pth.Path('/group_workspaces'),
    )
    DEFAULT_CONSUMER_CONFIG = {
        _FILELIST_THRESHOLD: 1000,
        _MESSAGE_THRESHOLD: 1000,
        _PRINT_TRACEBACKS: False,
        _SCANNABLE_ROOT_DIRS: DEFAULT_ROOT_DIRS,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        # Load config options or fall back to default values.
        self.filelist_threshold = self.load_config_value(self._FILELIST_THRESHOLD)
        self.message_threshold = self.load_config_value(self._MESSAGE_THRESHOLD)
        self.print_tracebacks = self.load_config_value(self._PRINT_TRACEBACKS)
        self.scannable_root_dirs = self.load_config_value(self._SCANNABLE_ROOT_DIRS, path_listify_fl=True)

    def load_config_value(self, config_option: str, path_listify_fl: bool = False):
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
            raise ValueError(f"Configuration option {config_option} not valid.\n"
                             f"Must be one of {list(self.DEFAULT_CONSUMER_CONFIG.keys())}")
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
            except:
                print(f"Invalid value for {config_option} in config file.\n"
                      f"Using default value instead.") 

        return return_val
    
    def callback(self, ch, method, properties, body, connection):
        try:
            # Convert body from bytes to string for ease of manipulation
            body_json = json.loads(body)

            self.log(f"Received {body} from {self.queues[0].name}({method.routing_key})",
                     self.RK_LOG_INFO)

            # Verify routing key is appropriate
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError:
                print(" [XXX] Routing key inappropriate length, exiting callback.")
                return
            
            # Verify filelist is, in fact, a list
            filelist = list(body_json[self.MSG_DATA][self.MSG_FILELIST])
            try:
                filelist_len = len(filelist)
            except TypeError as e:
                self.log("Filelist cannot be split into sublist, incorrect format given.", 
                         self.RK_LOG_ERROR)
                raise e
            
            if rk_parts[2] == self.RK_INITIATE:
                # Split the filelist into batches of 1000 and resubmit
                new_routing_key = ".".join([rk_parts[0], self.RK_INDEX, self.RK_INDEX])
                
                if filelist_len > self.filelist_threshold:
                    for filesublist in filelist[::self.filelist_threshold]:
                        body_json[self.MSG_FILELIST][self.MSG_FILELIST] = filesublist
                        self.publish_message(new_routing_key, json.dumps(body_json))
                else:
                    # Resubmit list as is for indexing
                    self.publish_message(new_routing_key, json.dumps(body_json))

            if rk_parts[2] == self.RK_INDEX:
                if filelist_len > 1000:
                    # TODO: Perhaps allow some dispensation/configuration to 
                    # allow the filelist to be broken down if this does happen?
                    raise ValueError(f"List with larger than allowed length "
                                     f"submitted for indexing ({self.filelist_threshold})")

                # Append routing info and then run the index
                body_json = self.append_route_info(body_json)
                new_routing_key = ".".join([rk_parts[0], self.RK_INDEX, self.RK_COMPLETE])
                self.log("Starting scan", self.RK_LOG_INFO)

                for indexed_filelist in self.index(filelist):
                    # Change message filelist info to new indexed list and send 
                    # that back to the exchange.
                    self.log("Sending indexed list back to exchange", self.RK_LOG_INFO)
                    body_json[self.MSG_DATA][self.MSG_FILELIST] = indexed_filelist
                    self.publish_message(new_routing_key, json.dumps(body_json))

            self.log(f"Scan finished.", self.RK_LOG_INFO)

        except Exception as e:
            if self.print_tracebacks:
                tb = traceback.format_exc()
                self.log(tb, self.RK_LOG_DEBUG)
            self.log(f"Encountered error ({e}), sending to logger.", self.RK_LOG_ERROR)
            body_json[self.MSG_DATA][self.MSG_ERROR] = str(e)
            new_routing_key = ".".join([self.RK_ROOT, self.RK_LOG, self.RK_LOG_INFO])
            self.publish_message(new_routing_key, json.dumps(body_json))

    def index(self, filelist: List[str], max_depth: int = -1):
        """
        Iterates through a filelist, yielding an 'indexed' filelist whereby 
        directories in the passed filelist are walked and properly walked.
        Is a generator, and so yields an indexed_filelist of maximum length 
        self.message_threshold, set through .server_config (defaults to 1000).
        """
        indexed_filelist = []

        for item in filelist:
            item_p = pth.Path(item)

            # Check if item is (a) fully resolved, and (b) in one of the allowed 
            # root directories - e.g. where the group workspaces are mounted
            root = pth.Path('/')
            if root in item_p.parents:
                # TODO: This may not work as intended, a requirement should be 
                # that all paths are resolved client-side otherwise unintended 
                # files could be indexed/transferred
                item_p = item_p.resolve()
            # If item is not in any of the allowed root dirs, skip
            if not any([item_p.is_relative_to(root_dir) for root_dir in self.scannable_root_dirs]):
                continue

            if item_p.is_dir():
                # Index directories by walking them
                for directory, _, subfiles in os.walk(item_p):
                    # Check how deep this iteration has come from starting dir
                    # and skip if greater than allowed maximum depth
                    depth = len(pth.Path(directory).relative_to(item_p).parts)
                    if max_depth >= 0 and depth >= max_depth:
                        continue
                    
                    # Loop through subfiles and append each to output filelist, 
                    # checking at each appension whether the message list length 
                    # threshold is breached and yielding appropriately
                    for f in subfiles:
                        indexed_filelist.append(os.path.join(directory, f))
                        if len(indexed_filelist) >= self.message_threshold:
                            # Yield and reset filelist
                            yield indexed_filelist
                            indexed_filelist = []
            else:
                # Index files directly
                indexed_filelist.append(item)
                if len(indexed_filelist) >= self.message_threshold:
                    # Yield and reset filelist
                    yield indexed_filelist
                    indexed_filelist = []
        
        # Yield whatever has been indexed after all directories have been walked
        yield indexed_filelist


def main():
    consumer = IndexerConsumer()
    consumer.run()

if __name__ == "__main__":
    main()