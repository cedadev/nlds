"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '15 Sep 2022'
__copyright__ = 'Copyright 2022 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

"""
Requires these settings in the /etc/nlds/server_config file:

    "catalog_q":{
        "db_engine": "sqlite",
        "db_options": {
            "db_name" : "/nlds_catalog.db",
            "db_user" : "",
            "db_passwd" : "",
            "echo" : true
        },
        "logging":{
            "enable": true
        }
"""

import json
from typing import Dict, Tuple, List
from hashlib import shake_256

# Typing imports
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

from datetime import datetime, timezone

from nlds.rabbit.consumer import RabbitMQConsumer as RMQC
from nlds.rabbit.consumer import State
from nlds.errors import CallbackError

from nlds_processors.catalog.catalog import Catalog, CatalogError
from nlds_processors.catalog.catalog_models import Storage, File
from nlds.details import PathDetails
from nlds_processors.db_mixin import DBError


class Metadata():
    """Container class for the meta section of the message body."""
    label: str
    holding_id: int
    transaction_id: str
    tags: Dict

    def __init__(self, body: Dict):
        # Get the label from the metadata section of the message
        try:
            self.label = body[RMQC.MSG_META][RMQC.MSG_LABEL]    
        except KeyError:
            self.label = None

        # Get the holding_id from the metadata section of the message
        try:
            self.holding_id = body[RMQC.MSG_META][RMQC.MSG_HOLDING_ID]
        except KeyError:
            self.holding_id = None

        # Get any tags that exist
        try:
            self.tags = body[RMQC.MSG_META][RMQC.MSG_TAG]
        except KeyError:
            self.tags = None

        # get the transaction id from the metadata section of the message
        try: 
            self.transaction_id = body[RMQC.MSG_META][RMQC.MSG_TRANSACT_ID]
        except KeyError:
            self.transaction_id = None

    @property
    def unpack(self) -> Tuple:
        return self.label, self.holding_id, self.tags, self.transaction_id
    

class CatalogConsumer(RMQC):
    DEFAULT_QUEUE_NAME = "catalog_q"
    DEFAULT_ROUTING_KEY = (f"{RMQC.RK_ROOT}.{RMQC.RK_CATALOG}.{RMQC.RK_WILD}")
    DEFAULT_STATE = State.CATALOG_PUTTING

    # Possible options to set in config file
    _DB_ENGINE = "db_engine"
    _DB_OPTIONS = "db_options"
    _DB_OPTIONS_DB_NAME = "db_name"
    _DB_OPTIONS_USER = "db_user"
    _DB_OPTIONS_PASSWD = "db_passwd"
    _DB_ECHO = "echo"
    _MAX_RETRIES = "max_retries"
    _DEFAULT_TENANCY = "default_tenancy"
    _DEFAULT_TAPE_URL = "default_tape_url"
    _TARGET_AGGREGATION_SIZE = "target_aggregation_size"

    DEFAULT_CONSUMER_CONFIG = {
        _DB_ENGINE: "sqlite",
        _DB_OPTIONS: {
            _DB_OPTIONS_DB_NAME: "/nlds_catalog.db",
            _DB_OPTIONS_USER: "",
            _DB_OPTIONS_PASSWD: "",
            _DB_ECHO: True,
        },
        _MAX_RETRIES: 5,
        _DEFAULT_TENANCY: None,
        _DEFAULT_TAPE_URL: None,
        _TARGET_AGGREGATION_SIZE: 5 * (1024**3), # Default to 5 GB
        RMQC.RETRY_DELAYS: RMQC.DEFAULT_RETRY_DELAYS,
    }


    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.max_retries = self.load_config_value(
            self._MAX_RETRIES
        )
        self.retry_delays = self.load_config_value(
            self.RETRY_DELAYS
        )
        self.default_tape_url = self.load_config_value(
            self._DEFAULT_TAPE_URL
        )
        self.default_tenancy = self.load_config_value(
            self._DEFAULT_TENANCY
        )
        self.target_aggregation_size = self.load_config_value(
            self._TARGET_AGGREGATION_SIZE
        )

        self.catalog = None
        self.reroutelist = []


    @property
    def database(self):
        return self.catalog
    

    def reset(self):
        super().reset()

        self.completelist = []
        self.retrylist = []
        self.failedlist = []
        # New list for rerouting to archive if not found on object store
        self.reroutelist = []


    def _parse_required_vars(self, body: Dict) -> Tuple:
        """Refactored out of each catalog function, this is merely a collection 
        of try-except statments to extract required variables from the message 
        body. Returns a tuple of filelist (a list of PathDetails objects), user 
        and group if successful, None if not.
        """
        filelist = self._parse_filelist(body)
        if filelist is None:
            # No need to log as it would have been logged in the parse function
            return

        user_vars = self._parse_user_vars(body)
        if user_vars is None:
            # No need to log as it would have been logged above
            return 
        else: 
            # Unpack assuming nothing bad has happened
            user, group = user_vars
        
        return filelist, user, group
    

    def _parse_filelist(self, body: Dict):
        # get the filelist from the data section of the message
        try:
            filelist = body[self.MSG_DATA][self.MSG_FILELIST]
        except KeyError as e:
            self.log(f"Invalid message contents, filelist should be in the data "
                     f"section of the message body.",
                     self.RK_LOG_ERROR)
            return 

        # check filelist is indexable
        try:
            _ = filelist[0]
        except TypeError as e:
            self.log(f"filelist field must contain a list", self.RK_LOG_ERROR)
            return

        return filelist


    def _parse_user_vars(self, body: Dict) -> Tuple:
        # get the user id from the details section of the message
        try:
            user = body[self.MSG_DETAILS][self.MSG_USER]
        except KeyError:
            self.log("User not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return

        # get the group from the details section of the message
        try:
            group = body[self.MSG_DETAILS][self.MSG_GROUP]
        except KeyError:
            self.log("Group not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return
        
        return user, group
    

    def _parse_metadata_vars(self, body: Dict) -> Tuple:
        """Convenience function to prevent unnecessary code replication for 
        extraction of metadata variables from the message body. This is 
        specifically for requesting particular holdings/transactions/labels/tags 
        in a given catalog function. Returns a tuple of label, holding_id and 
        tags, with each being None if not found.
        """
        md = Metadata(body)
        return md.unpack


    def _catalog_put(self, body: Dict, rk_origin: str) -> None:
        """Put a file record into the catalog - end of a put transaction"""
        # Parse the message body for required variables
        message_vars = self._parse_required_vars(body)
        if message_vars is None:
            # Check if any problems have occurred in the parsing of the message 
            # body and exit if necessary 
            self.log("Could not parse one or more mandatory variables, exiting "
                     "callback.", self.RK_LOG_ERROR)
            return
        else:
            # Unpack if no problems found in parsing
            filelist, user, group = message_vars
        
        # get the transaction id from the details section of the message. It is 
        # a mandatory variable for the PUT workflow
        try: 
            transaction_id = body[self.MSG_DETAILS][self.MSG_TRANSACT_ID]
        except KeyError:
            self.log("Transaction id not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return
        
        tenancy = None
        # Get the tenancy from message, if none found then use the configured 
        # default
        try: 
            tenancy = body[self.MSG_DETAILS][self.MSG_TENANCY]
        except KeyError:
            pass
        if tenancy is None:
            tenancy = self.default_tenancy

        # Extract variables from the metadata section of the message body
        md = Metadata(body)
        label, holding_id, tags, _ = md.unpack
        if label is None:
            # No label given so if holding_id not given the subset of 
            # transaction id is used as new label
            # TODO: (2023-07-21) should loop here to make sure we get a unique 
            # label
            new_label = transaction_id[0:8] 
        else: 
            # If holding id not given then new_label used to create holding
            new_label = label    
        
        # Start the database transactions
        self.catalog.start_session()

        # determine the search label
        if label:
            search_label = label
        elif holding_id:
            search_label = ".*"     # match all labels if holding id given
        else:
            search_label = "^$"     # match nothing if no label or holding id
                                    # this will produce a new holding

        # try to get the holding to see if it already exists and can be added to
        try:
            # don't use tags to search - they are strictly for adding to the
            # holding
            holding = self.catalog.get_holding(
                user, group, search_label, holding_id
            )
        except (KeyError, CatalogError):
            holding = None

        if holding is None:
            # if the holding_id is not None then raise an error as the user is
            # trying to add to a holding that doesn't exist, but creating a new
            # holding won't have a holding_id that matches the one they passed in
            if (holding_id is not None):
                message = (f"Could not add files to holding with holding_id: "
                           f"{holding_id}.  holding_id does not exist.")
                self.log(message, self.RK_LOG_DEBUG)
                raise CallbackError(message)
            try:
                holding = self.catalog.create_holding(
                    user, group, new_label
                )
            except CatalogError as e:
                self.log(e.message, RMQC.RK_LOG_ERROR)
                return
        else:
            holding = holding[0]

        # try to get the transaction to see if it already exists and can be 
        # added to
        try:
            transaction = self.catalog.get_transaction(
                transaction_id=transaction_id
            )
        except (KeyError, CatalogError):
            transaction = None

        # create the transaction within the  holding if it doesn't exist
        if transaction is None:
            try:
                transaction = self.catalog.create_transaction(
                    holding, 
                    transaction_id
                )
            except CatalogError as e:
                self.log(e.message, RMQC.RK_LOG_ERROR)
                return

        # loop over the filelist
        for f in filelist:
            # convert to PathDetails class
            pd = PathDetails.from_dict(f)
            try:
                # Search first for file existence within holding, retry/fail if 
                # present
                try:
                    files = self.catalog.get_files(
                        user, 
                        group, 
                        holding_id=holding.id,
                        original_path=pd.original_path, 
                    )
                except CatalogError:
                    pass # should throw a catalog error if file(s) not found
                else:
                    raise CatalogError("File already exists in holding")
                
                file_ = self.catalog.create_file(
                    transaction,
                    pd.user, 
                    pd.group, 
                    pd.original_path, 
                    pd.path_type, 
                    pd.link_path,
                    pd.size, 
                    pd.permissions
                )

                # NOTE: This is an approximation/quick hack to get a value for 
                # object_name before we have created the actual object. This is 
                # fine as-is for now as the object name will always be 
                # equivalent to original path, but this may change in the future 
                # and require an extra step in the workflow to update the 
                # Location with the appropriate object name. 
                if pd.object_name is None:
                    object_name = pd.original_path
                else:
                    object_name = pd.object_name
                location = self.catalog.create_location(
                    file_,
                    Storage.OBJECT_STORAGE,
                    url_scheme="http",
                    url_netloc=tenancy,
                    root=transaction.transaction_id,
                    path=object_name, 
                    # access time is passed in the file details
                    access_time = datetime.fromtimestamp(
                        pd.access_time, tz=timezone.utc
                    )
                )
                self.completelist.append(pd)
            except CatalogError as e:
                if pd.retries.count > self.max_retries:
                    self.failedlist.append(pd)
                else:
                    pd.retries.increment(
                        reason=f"{e.message}"
                    )
                    self.retrylist.append(pd)
                self.log(e.message, RMQC.RK_LOG_ERROR)
                continue

        # add the tags - if the tag already exists then don't add it or modify
        # it, with the reasoning that the user can change it with the `meta`
        # command.
        warnings = []
        if tags:
            for k in tags:
                try:
                    tag = self.catalog.get_tag(holding, k)
                except CatalogError: # tag's key not found so create
                    self.catalog.create_tag(holding, k, tags[k])
                else:
                    # append a warning that the tag could not be added to the holding
                    warnings.append(
                        f"Tag with key:{k} could not be added to holding with label"
                        f":{label} as that tag already exists.  Tags can be modified"
                        f" using the meta command"
                    )

        # stop db transitions and commit
        self.catalog.save()
        self.catalog.end_session()

        # log the successful and non-successful catalog puts
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin,
                                    self.RK_CATALOG_PUT, 
                                    self.RK_COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_PUT {self.completelist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.completelist, rk_complete, body, 
                               state=State.CATALOG_PUTTING,
                               warning=warnings)
        # RETRY
        if len(self.retrylist) > 0:
            rk_retry = ".".join([rk_origin,
                                 self.RK_CATALOG_PUT, 
                                 self.RK_START])
            self.log(
                f"Sending retry PathList from CATALOG_PUT {self.retrylist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.retrylist, rk_retry, body, mode="retry",
                               state=State.CATALOG_PUTTING,
                               warning=warnings)
        # FAILED
        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin,
                                  self.RK_CATALOG_PUT, 
                                  self.RK_FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_PUT {self.failedlist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.failedlist, rk_failed, body, 
                               mode="failed",
                               warning=warnings)


    def _catalog_get(self, body: Dict, rk_origin: str) -> None:
        """Get the details for each file in a filelist and send it to the 
        exchange to be processed by the transfer processor. If any file is only 
        found on tape then it will be first rerouted to the archive processor 
        for retrieval to object store cache."""
        # Parse the message body for required variables
        message_vars = self._parse_required_vars(body)
        if message_vars is None:
            # Check if any problems have occurred in the parsing of the message 
            # body and exit if necessary 
            self.log("Could not parse one or more mandatory variables, exiting "
                     "callback.", self.RK_LOG_ERROR)
            return
        else:
            # Unpack if no problems found in parsing
            filelist, user, group = message_vars

        tenancy = None
        # Get the tenancy from message, if none found then use the configured 
        # default
        try: 
            tenancy = body[self.MSG_DETAILS][self.MSG_TENANCY]
        except KeyError:
            pass
        if tenancy is None:
            tenancy = self.default_tenancy

        # Extract variables from the metadata section of the message body
        md = Metadata(body)
        holding_label, holding_id, holding_tag, transaction_id = md.unpack

        # Start a set of the aggregations we need to retrieve
        aggs_to_retrieve: Dict[int, List] = dict()

        # start the database transactions
        self.catalog.start_session()
        for f in filelist:
            file_details = PathDetails.from_dict(f)
            try:
                # get the files first
                files = self.catalog.get_files(
                    user, group, holding_label, holding_id, 
                    transaction_id, file_details.original_path, holding_tag
                )
                if len(files) == 0:
                    raise CatalogError(
                        f"Could not find file(s) with original path "
                        f"{file_details.original_path}"
                    )
                # now get the location so we can get where it is stored
                for file_ in files:
                    try:
                        # For files stored in object store
                        in_object_store_fl = True
                        location = self.catalog.get_location(
                            file_, Storage.OBJECT_STORAGE
                        )
                        # If not in object store then look for it in tape 
                        if location is None:
                            in_object_store_fl = False
                            self.log("Searching for copy of file in archive", 
                                     self.RK_LOG_INFO)
                            location = self.catalog.get_location(
                                file_, Storage.TAPE
                            )
                            
                        # If still None then file doesn't exist within the NLDS
                        if location is None:
                            raise CatalogError(
                                f"Could not find location for file with "
                                f"original path {file_details.original_path}."
                            )
                    except CatalogError as e:
                        if file_details.retries.count > self.max_retries:
                            self.failedlist.append(file_details)
                        else:
                            self.retrylist.append(file_details)
                            file_details.retries.increment(
                                reason=f"{e.message}"
                            )
                        self.log(e.message, RMQC.RK_LOG_ERROR)
                        continue
                    # Make the object name. The format of location.root here 
                    # will vary a little depending on the workflow: if the file 
                    # has never been restored from tape before it will still be 
                    # the transaction_id, but if it has been restored this will 
                    # be a combination of holding and aggregation details. This 
                    # reflects the change in bucket required by tape retrieval.
                    object_name = (
                        f"nlds.{location.root}:{location.path}"
                    )
                    access_time = location.access_time.timestamp()
                    # create a new PathDetails with all the info from the DB
                    new_file = PathDetails(
                        original_path = file_.original_path,
                        object_name = object_name,
                        size = file_.size,
                        user = file_.user,
                        group = file_.group,
                        permissions = file_.file_permissions,
                        access_time = access_time,
                        path_type = file_.path_type,
                        link_path = file_.link_path
                    )
                    # Assign to the appropriate queue for retrieval from archive 
                    # or object store. Additional information is required 
                    # depending on where it's going
                    if in_object_store_fl:
                        # Need to include the tenancy information for the object
                        # store transfer
                        # TODO: this isn't actually used yet, the tenancy in the 
                        # message body is used instead in TransferGetConsumer()
                        new_file.tenancy = location.url_netloc
                        self.completelist.append(new_file)
                    else: 
                        # Need the new tenancy info as well as the stored tape 
                        # location info
                        new_file.tenancy = tenancy
                        new_file.tape_url = location.url_netloc
                        new_file.tape_path = object_name

                        # Save the aggregation id for later so we can add 
                        # objectstore aggregations for all of the files 
                        # retrieved from archive. 
                        # NOTE: We attempt to get the aggregation here so the 
                        # file can be failed at the earliest opportunity if 
                        # there is a problem.
                        agg = self.catalog.get_aggregation(
                            aggregation_id=location.aggregation_id
                        )
                        if agg in aggs_to_retrieve:
                            aggs_to_retrieve[agg].append(new_file)
                        else:
                            aggs_to_retrieve[agg] = [new_file, ]

            except CatalogError as e:
                if file_details.retries.count > self.max_retries:
                    self.failedlist.append(file_details)
                else:
                    file_details.retries.increment(
                        reason=f"{e.message}"
                    )
                    self.retrylist.append(file_details)
                self.log(e.message, RMQC.RK_LOG_ERROR)
                continue
        
        for aggregation, details_list in aggs_to_retrieve.items():
            # Make a session checkpoint here so we can rollback in the event of 
            # a failed Location creation
            # TODO: We need a way to mark a file as being a problem
            checkpoint = self.catalog.session.begin_nested()
            try:
                # Create a fake file class with just an id so we can pass it to 
                # the create_location method
                class FakeFile:
                    id: int
                # Add the new objectstore location to the catalog now, 
                # to be removed in the event of an s3 get failure. 
                for tape_location in aggregation.locations():
                    objstr_location = self.catalog.create_location(
                        FakeFile(id=tape_location.file_id),
                        storage_type=Storage.OBJECT_STORAGE,
                        url_scheme="http",
                        url_netloc=tenancy,
                        root=tape_location.root,
                        path=tape_location.path, 
                        access_time = tape_location.access_time.timestamp()
                    )
                # Also need to make locations for each of the files 
                # within the same aggregation
            except CatalogError as e:
                # In the event of a failure we rollback all the added locations 
                # and add the original file_details to the retry/fail list
                checkpoint.rollback()
                for file_details in details_list:
                    if file_details.retries.count > self.max_retries:
                        self.failedlist.append(file_details)
                    else:
                        file_details.retries.increment(
                            reason=f"{e.message}"
                        )
                        self.retrylist.append(file_details)
                self.log(e.message, RMQC.RK_LOG_ERROR)
                continue
            else: 
                # Add the file_details to the reroute list for archive retrieval 
                for file_details in details_list:
                    self.reroutelist.append(file_details)

        # log the successful and non-successful catalog puts
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin,
                                    self.RK_CATALOG_GET, 
                                    self.RK_COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_GET {self.completelist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.completelist, rk_complete, body, 
                               state=State.CATALOG_GETTING)
        # REROUTE
        if len(self.reroutelist) > 0:
            rk_reroute = ".".join([rk_origin,
                                   self.RK_CATALOG_GET, 
                                   self.RK_REROUTE])
            # Ensure the holding_id is present as we'll need it during retrieval  
            body[self.MSG_META][self.MSG_HOLDING_ID] = holding_id # Ensure this is actually populated
            self.log(
                f"Rerouting PathList from CATALOG_GET for archive retrieval "
                f"{self.reroutelist}", self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.reroutelist, rk_reroute, body, 
                               state=State.CATALOG_RESTORING)
        # RETRY
        if len(self.retrylist) > 0:
            rk_retry = ".".join([rk_origin,
                                 self.RK_CATALOG_GET, 
                                 self.RK_START])
            self.log(
                f"Sending retry PathList from CATALOG_GET {self.retrylist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.retrylist, rk_retry, body, mode="retry",
                               state=State.CATALOG_GETTING)
        # FAILED
        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin,
                                  self.RK_CATALOG_GET, 
                                  self.RK_FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_GET {self.failedlist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.failedlist, rk_failed, body, 
                               mode="failed")

        # stop db transistions and commit
        self.catalog.end_session()


    def aggregate_files(
            self, 
            filelist: List[File], 
            target_agg_count: int = None
        ) -> List[List[File]]:
        """Creates a list of suitable aggregations from a given list of files. A
        few algorithms for this were explored, with some suiting different 
        distributions of file sizes better, so the most generic solution is 
        provided. This implementation calculates a target value for aggregation 
        size, configurable through server-config, and then iterates through the 
        Files and sorts each into the smallest aggregation available at that 
        iteration. """
        # Calculate a target aggregation count if one is not given
        if target_agg_count is None:
            filesizes = (f.size for f in filelist)
            total_size = sum(filesizes)
            count = len(filesizes)
            mean_size = total_size / count
            if total_size < self.target_aggregation_size:
                # If it's less that a single target agg size then just do a single 
                # aggregation
                return [filelist, ]
            # TODO: Need to think this conditional through a bit more. This is 
            # the condition for if all the files are about the size of the 
            # target_aggregation_size, in which case we could end up with 
            # len(filelist) aggregations each with one file in them, which is 
            # maximally inefficient for the tape? 
            elif mean_size > self.target_aggregation_size:
                # For now we'll just set it to 5 in this particular case.
                target_agg_count = 5
            else:
                # Otherwise we're going for the number of target sizes that fit 
                # into our total size. 
                target_agg_count = total_size / self.target_aggregation_size

        assert target_agg_count is not None

        # Make 2 lists, one being a list of lists dictating the aggregates, the 
        # other being their sizes, so we're not continually recalculating it
        aggregates = [[] for _ in range(count)]
        sizes = [0 for _ in range(count)]
        filelist_sorted = sorted(filelist, reverse=True, key=lambda f: f.size)
        for fs in filelist_sorted:
            # Get the index of the smallest aggregate
            agg_index = min(range(len(aggregates)), key=lambda i: sizes[i])
            aggregates[agg_index].append(fs)
            sizes[agg_index] += fs.size

        return aggregates


    def _catalog_archive_put(self, body: Dict, rk_origin: str) -> None:
        """Get the next holding for archiving, create a new location and 
        aggregation for it and pass to for writing to tape."""   
        # Get the tape_url from message, if none found then use the configured 
        # default
        tape_url = None
        try: 
            tape_url = body[self.MSG_DETAILS][self.MSG_TAPE_URL]
        except KeyError:
            pass
        if tape_url is None:
            tape_url = self.default_tape_url

        # start the database transactions
        self.catalog.start_session()

        # Get the next holding in the catalog, by id, which has any unarchived 
        # Files, i.e. any files which don't have a tape location
        next_holding = self.catalog.get_next_holding()
        # We need a new root as we can't rely on the transaction_id any more. 
        # Create a slug from the uneditable holding information, this will be 
        # the directory on the tape that contains each of the tar files. 
        holding_slug = (f"{next_holding.id}.{next_holding.user}"
                        f".{next_holding.group}")
        
        # Get the unarchived files and make suitable aggregates of them. Here we
        # make a distinction between aggregates, being just groups of files, and 
        # aggregations, being the database object / table 
        filelist = self.catalog.get_unarchived_files(next_holding)
        aggregates = self.aggregate_files(filelist)

        # Create the aggragation and respective locations for each file in each 
        # aggregate
        for aggregate in aggregates:
            self.reset()
            # Generate a name for the tarfile by hashing the combined filelist. 
            # Length of the hash will be 16
            filenames = [f.original_path for f in aggregate]
            filelist_hash = shake_256("".join(filenames).encode()).hexdigest(8) 
            tar_filename = f"{filelist_hash}.tar"
            # Make the tape_root here, which will be stored in the Location. 
            # This will act as the bucket name if transferred to object store 
            # later on 
            tape_root = f"{holding_slug}_{filelist_hash}"
            # Make the aggregation first
            try:
                aggregation = self.catalog.create_aggregation(
                    tarname=tar_filename
                )
            except CatalogError as e:
                # If for some reason we fail to make the aggregation then 
                # continue and the remaining unarchived files will get picked up 
                # on the next call to get_next_holding()
                self.log(e.message, RMQC.RK_LOG_ERROR)
                continue
            # Now create the tape locations and assign them to the aggregation
            for f in aggregate:
                try:
                    # Get the object_store location so we can pass the object 
                    # information easily to the archiver
                    objstr_location = self.catalog.get_location(
                        f, Storage.OBJECT_STORAGE
                    )
                    # We need to pass the tenancy too, which could be different 
                    # for each location
                    file_details = PathDetails(
                        original_path = f.original_path,
                        object_name = objstr_location.object_name,
                        tenancy = objstr_location.url_netloc,
                        tape_path = tape_root,
                        tape_url = tape_url,
                        size = f.size,
                        user = f.user,
                        group = f.group,
                        permissions = f.file_permissions,                    
                        access_time = f.access_time,
                        path_type = f.path_type,
                        link_path = f.link_path
                    )
                    self.catalog.create_location(
                        file_=f, 
                        storage_type=Storage.TAPE,
                        url_scheme="root:",
                        url_netloc=tape_url,
                        root=tape_root,
                        path=f.original_path, 
                        # access time is passed in the file details
                        access_time = datetime.fromtimestamp(
                            f.access_time, tz=timezone.utc
                        ),
                        aggregation=aggregation
                    )
                except CatalogError as e:
                    # In the case of failure, we can just carry on adding things 
                    # to the aggregation and then the next call to 
                    # get_next_holding() should handle the rest. 
                    self.log(e.message, RMQC.RK_LOG_ERROR)
                    # Keep note of the failure (we're not sending it anywhere)
                    self.failedlist.append(file_details)
                    continue

                self.completelist.append(file_details)

            if len(self.failedlist) == len(aggregate):
                # In the unlikely event that all of the Locations failed to 
                # create then delete the Aggregation
                self.catalog.delete_aggregation(aggregation)
                continue

            # Forward successful file details to archiver for tape write
            rk_complete = ".".join([rk_origin,
                                    self.RK_CATALOG_ARCHIVE_PUT, 
                                    self.RK_COMPLETE])
            
            body[self.MSG_DETAILS][self.MSG_USER] = next_holding.user
            body[self.MSG_DETAILS][self.MSG_GROUP] = next_holding.group
            body[self.MSG_META][self.MSG_HOLDING_ID] = next_holding.id
            body[self.MSG_META][self.MSG_AGGREGATION_ID] = aggregation.id
            self.log(
                f"Sending completed PathList from CATALOG_ARCHIVE_PUT "
                f"{self.completelist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.completelist, rk_complete, body, 
                               state=State.CATALOG_BACKUP)
        
        # stop db transactions and commit
        self.catalog.save()
        self.catalog.end_session()


    def _catalog_archive_update(self, body: Dict) -> None:
        """Update the aggregation record following successful archive write to 
        fill in the missing checksum information."""
        # Parse the message body for required variables
        message_vars = self._parse_required_vars(body)
        if message_vars is None:
            # Check if any problems have occurred in the parsing of the message 
            # body and exit if necessary 
            self.log("Could not parse one or more mandatory variables, exiting "
                     "callback.", self.RK_LOG_ERROR)
            return
        else:
            # Unpack if no problems found in parsing
            filelist, user, group = message_vars
        
        # Extract variables from the metadata section of the message body
        md = Metadata(body)
        _, holding_id, _, _ = md.unpack

        # Parse aggregation and checksum info from message.
        try: 
            aggregation_id = body[self.MSG_META][self.MSG_AGGREGATION_ID]
        except KeyError:
            self.log("Aggregation id not in message, continuing without.", 
                     self.RK_LOG_WARNING)
        try: 
            checksum = body[self.MSG_DATA][self.MSG_CHECKSUM]
        except KeyError:
            self.log("Checksum not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return
        
        if holding_id is None and aggregation_id is None:
            self.log("No method for identifying an aggregation provided, forced"
                     " to exit callback.", self.RK_LOG_ERROR)
            return

        self.catalog.start_session()

        try:
            if aggregation_id is not None:
                aggregation = self.catalog.get_aggregation()
            elif holding_id is not None:
                # get first file in list, that is all we should need, and turn it 
                # into a PathDetails object
                f = filelist[0] 
                file_details = PathDetails.from_dict(f)
                # Using that and the holding_id, we can get the 
                files = self.catalog.get_files(
                    user, group, 
                    holding_id=holding_id, 
                    original_path=file_details.original_path
                )
                file_ = files[0]
                aggregation = self.catalog.get_aggregation_by_file(file_)
            
            self.catalog.update_aggregation(aggregation, checksum=checksum, 
                                            algorithm="ADLER32")
        except CatalogError as e:
            self.log(f"Error encountered during _catalog_archive_update(): {e}", 
                     self.RK_LOG_ERROR)
            raise CallbackError("Encountered error during aggregation update, "
                                "submitting for retry")
        
        # stop db transactions and commit
        self.catalog.save()
        self.catalog.end_session() 


    def _catalog_del(self, body: Dict, rk_origin: str) -> None:
        """Remove a given list of files from the catalog if the transfer 
        fails"""
        # Parse the message body for required variables
        message_vars = self._parse_required_vars(body)
        if message_vars is None:
            # Check if any problems have occurred in the parsing of the message 
            # body and exit if necessary 
            self.log("Could not parse one or more mandatory variables, exiting "
                     "callback.", self.RK_LOG_ERROR)
            return
        else:
            # Unpack if no problems found in parsing
            filelist, user, group = message_vars

        # Extract variables from the metadata section of the message body
        md = Metadata(body)
        holding_label, holding_id, holding_tag, _ = md.unpack

        # start the database transactions
        self.catalog.start_session()

        # get the holding from the database
        if holding_label is None and holding_id is None and holding_tag is None:
            self.log("No method for identifying a holding or transaction "
                     "provided, will continue without.",
                     self.RK_LOG_WARNING)
            # TODO: what happens in this event?

        for f in filelist:
            file_details = PathDetails.from_dict(f)
            try:
                # outsource deleting to the catalog itself
                self.catalog.delete_files(
                    user, group, holding_label=holding_label, 
                    holding_id=holding_id, path=file_details.original_path,
                    tag=holding_tag
                )
            except CatalogError as e:
                if file_details.retries.count > self.max_retries:
                    self.failedlist.append(file_details)
                else:
                    file_details.retries.increment(
                        reason=f"{e.message}"
                    )
                    self.retrylist.append(file_details)
                self.log(e.message, RMQC.RK_LOG_ERROR)
                continue

        # log the successful and non-successful catalog puts
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin,
                                    self.RK_CATALOG_DEL, 
                                    self.RK_COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_DEL {self.completelist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.completelist, rk_complete, body, 
                               state=State.CATALOG_ROLLBACK)
        # RETRY
        if len(self.retrylist) > 0:
            rk_retry = ".".join([rk_origin,
                                 self.RK_CATALOG_DEL, 
                                 self.RK_START])
            self.log(
                f"Sending retry PathList from CATALOG_DEL {self.retrylist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.retrylist, rk_retry, body, mode="retry",
                               state=State.CATALOG_ROLLBACK)
        # FAILED
        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin,
                                  self.RK_CATALOG_DEL, 
                                  self.RK_FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_DEL {self.failedlist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.failedlist, rk_failed, body, 
                               mode="failed")

        # stop db transactions and commit
        self.catalog.save()
        self.catalog.end_session() 


    def _catalog_location_del(
            self, 
            body: Dict, 
            rk_origin: str, 
            location_type: Storage = Storage.OBJECT_STORAGE
        ) -> None:
        """Remove a given list of locations from the catalog if the transfer, 
        archive-put or archive-get fails."""
        # Parse the message body for required variables
        message_vars = self._parse_required_vars(body)
        if message_vars is None:
            # Check if any problems have occurred in the parsing of the message 
            # body and exit if necessary 
            self.log("Could not parse one or more mandatory variables, exiting "
                     "callback.", self.RK_LOG_ERROR)
            return
        else:
            # Unpack if no problems found in parsing
            filelist, user, group = message_vars

        # Extract variables from the metadata section of the message body
        md = Metadata(body)
        holding_label, holding_id, tag, transaction_id = md.unpack

        # start the database transactions
        self.catalog.start_session()

        # get the holding from the database
        if holding_label is None and holding_id is None and tag is None:
            self.log("No method for identifying a holding or transaction "
                     "provided, will continue without.",
                     self.RK_LOG_WARNING)

        for f in filelist:
            file_details = PathDetails.from_dict(f)
            try:
                # get the files first
                files = self.catalog.get_files(
                    user, group, holding_label, holding_id, 
                    transaction_id, file_details.original_path, tag
                )
                if len(files) == 0:
                    raise CatalogError(
                        f"Could not find file(s) with original path "
                        f"{file_details.original_path}"
                    )
                # now get the location for the storage type requested so we can 
                # delete it
                for file in files:
                    try:
                        self.catalog.delete_location(
                            file, location_type
                        )
                    except CatalogError as e:
                        if file_details.retries.count > self.max_retries:
                            self.failedlist.append(file_details)
                        else:
                            file_details.retries.increment(
                                reason=f"{e.message}"
                            )
                            self.retrylist.append(file_details)
                        self.log(e.message, RMQC.RK_LOG_ERROR)
                        continue
            except CatalogError as e:
                if file_details.retries.count > self.max_retries:
                    self.failedlist.append(file_details)
                else:
                    file_details.retries.increment(
                        reason=f"{e.message}"
                    )
                    self.retrylist.append(file_details)
                self.log(e.message, RMQC.RK_LOG_ERROR)
                continue

        # log the successful and non-successful catalog puts
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin,
                                    self.RK_CATALOG_DEL, 
                                    self.RK_COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_DEL {self.completelist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.completelist, rk_complete, body, 
                               state=State.CATALOG_ROLLBACK)
        # RETRY
        if len(self.retrylist) > 0:
            rk_retry = ".".join([rk_origin,
                                 self.RK_CATALOG_DEL, 
                                 self.RK_START])
            self.log(
                f"Sending retry PathList from CATALOG_DEL {self.retrylist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.retrylist, rk_retry, body, mode="retry",
                               state=State.CATALOG_ROLLBACK)
        # FAILED
        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin,
                                  self.RK_CATALOG_DEL, 
                                  self.RK_FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_DEL {self.failedlist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.failedlist, rk_failed, body, 
                               mode="failed")

        # stop db transactions and commit
        self.catalog.save()
        self.catalog.end_session() 
        

    def _catalog_list(self, body: Dict, properties: Header) -> None:
        """List the users holdings"""
        # Parse the message body for required variables
        message_vars = self._parse_user_vars(body)
        if message_vars is None:
            # Check if any problems have occurred in the parsing of the message 
            # body and exit if necessary 
            self.log("Could not parse one or more mandatory variables, exiting "
                     "callback.", self.RK_LOG_ERROR)
            return
        else:
            # Unpack if no problems found in parsing
            user, group = message_vars

        # Extract variables from the metadata section of the message body
        md = Metadata(body)
        holding_label, holding_id, tag, transaction_id = md.unpack

        self.catalog.start_session()

        # holding_label and holding_id is None means that more than one
        # holding wil be returned
        try:
            holdings = self.catalog.get_holding(
                user, group, holding_label, holding_id, transaction_id, tag
            )
        except CatalogError as e:
            # failed to get the holdings - send a return message saying so
            self.log(e.message, self.RK_LOG_ERROR)
            body[self.MSG_DETAILS][self.MSG_FAILURE] = e.message
            body[self.MSG_DATA][self.MSG_HOLDING_LIST] = []
        else:
            # fill the dictionary to generate JSON for the response
            ret_list = []
            for h in holdings:
                # get the first transaction
                t = h.transactions[0]
                ret_dict = {
                    "id": h.id,
                    "label": h.label,
                    "user": h.user,
                    "group": h.group,
                    "tags": h.get_tags(),
                    "transactions": h.get_transaction_ids(),
                    "date": t.ingest_time.isoformat()
                }
                ret_list.append(ret_dict)
            # add the return list to successfully completed holding listings
            body[self.MSG_DATA][self.MSG_HOLDING_LIST] = ret_list
            self.log(
                f"Listing holdings from CATALOG_LIST {ret_list}",
                self.RK_LOG_DEBUG
            )
        self.catalog.end_session()

        # send the rpc return message for failed or success
        self.publish_message(
            properties.reply_to,
            msg_dict=body,
            exchange={'name': ''},
            correlation_id=properties.correlation_id
        )
    

    def _catalog_stat(self, body: Dict, properties: Header) -> None:
        """Get the labels for a list of transaction ids"""
        # Parse the message body for required variables
        message_vars = self._parse_user_vars(body)
        if message_vars is None:
            # Check if any problems have occurred in the parsing of the message 
            # body and exit if necessary 
            self.log("Could not parse one or more mandatory variables, exiting "
                     "callback.", self.RK_LOG_ERROR)
            return
        else:
            # Unpack if no problems found in parsing
            user, group = message_vars

        # get the transaction_ids from the metadata section of the message
        try:
            transaction_records = body[self.MSG_DATA][self.MSG_RECORD_LIST]
        except KeyError:
            self.log("Transaction ids not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return
        
        # Verify transaction list
        if transaction_records is None or len(transaction_records) <= 0:
            self.log("Passed list of transactions is not valid, check message " 
                     "contents. Exiting callback", self.RK_LOG_ERROR)
            return

        self.catalog.start_session()

        # Get transactions from catalog using transaction_ids from monitoring
        ret_dict = {}
        try:
            # Get the transaction and holding for each transaction_record
            for tr in transaction_records:
                transaction_id = tr["transaction_id"]
                t = self.catalog.get_transaction(
                    transaction_id=transaction_id
                )
                # A transaction_id might not have an associated transaction in
                # the catalog if the transaction FAILED or has not COMPLETED
                # yet.  We allow for this and return an empty string instead.
                if t is None:
                    label = ""
                else:
                    h = self.catalog.get_holding(
                        user, group, holding_id=t.holding_id
                    )[0] # should only be one!
                    label = h.label
                    ret_dict[t.transaction_id] = label

                # Add label to the transaction_record dict
                tr[self.MSG_LABEL] = label
        except CatalogError as e:
            # failed to get the transactions - send a return message saying so
            self.log(e.message, self.RK_LOG_ERROR)
            body[self.MSG_DETAILS][self.MSG_FAILURE] = e.message
            body[self.MSG_DATA][self.MSG_TRANSACTIONS] = {}
        else:            
            # add the return list to successfully completed holding listings
            body[self.MSG_DATA][self.MSG_TRANSACTIONS] = ret_dict
            body[self.MSG_DATA][self.MSG_RECORD_LIST] = transaction_records
            self.log(
                f"Getting holding labels from CATALOG_STAT {ret_dict}",
                self.RK_LOG_DEBUG
            )
        self.catalog.end_session()

        # send the rpc return message for failed or success
        self.publish_message(
            properties.reply_to,
            msg_dict=body,
            exchange={'name': ''},
            correlation_id=properties.correlation_id
        )


    def _catalog_find(self, body: Dict, properties: Header) -> None:
        """List the user's files"""
        # Parse the message body for required variables
        message_vars = self._parse_user_vars(body)
        if message_vars is None:
            # Check if any problems have occurred in the parsing of the message 
            # body and exit if necessary 
            self.log("Could not parse one or more mandatory variables, exiting "
                     "callback.", self.RK_LOG_ERROR)
            return
        else:
            # Unpack if no problems found in parsing
            user, group = message_vars

        # Extract variables from the metadata section of the message body
        md = Metadata(body)
        holding_label, holding_id, tag, transaction_id = md.unpack

        # get the path from the detaisl section of the message
        try:
            path = body[self.MSG_META][self.MSG_PATH]
        except KeyError:
            path = None

        self.catalog.start_session()
        ret_dict = {}
        try:
            files = self.catalog.get_files(
                user, group, holding_label, holding_id, 
                transaction_id, path, tag
            )
            for f in files:
                # get the transaction and the holding:
                t = self.catalog.get_transaction(
                    id = f.transaction_id
                ) # should only be one!
                h = self.catalog.get_holding(
                    user, group, holding_id=t.holding_id
                )[0] # should only be one!
                # create a holding dictionary if it doesn't exists
                if h.label in ret_dict:
                    h_rec = ret_dict[h.label]
                else:
                    h_rec = {
                        self.MSG_TRANSACTIONS: {},
                        self.MSG_LABEL: h.label,
                        self.MSG_HOLDING_ID: h.id,
                        self.MSG_USER: h.user,
                        self.MSG_GROUP: h.group
                    }
                    ret_dict[h.label] = h_rec                    
                # create a transaction dictionary if it doesn't exist
                if t.transaction_id in ret_dict[h.label][self.MSG_TRANSACTIONS]:
                    t_rec = ret_dict[h.label][self.MSG_TRANSACTIONS][t.transaction_id]
                else:
                    t_rec = {
                        self.MSG_FILELIST: [],
                        self.MSG_TRANSACT_ID: t.transaction_id,
                        "ingest_time": t.ingest_time.isoformat()
                    }
                    ret_dict[h.label][self.MSG_TRANSACTIONS][t.transaction_id] = t_rec
                # get the locations
                locations = []
                for l in f.location:
                    l_rec = {
                        "storage_type" : l.storage_type,
                        "root": l.root,
                        "path": l.path,
                        "access_time": l.access_time.isoformat(),
                    }
                    locations.append(l_rec)
                # build the file record
                f_rec = {
                    "original_path" : f.original_path,
                    "path_type" : str(f.path_type),
                    "link_path" : f.link_path,
                    "size" : f.size,
                    self.MSG_USER : f.user,
                    self.MSG_GROUP : f.group,
                    "permissions": f.file_permissions,
                    "locations" : locations
                }
                t_rec[self.MSG_FILELIST].append(f_rec)

        except CatalogError as e:
            # failed to get the holdings - send a return message saying so
            self.log(e.message, self.RK_LOG_ERROR)
            body[self.MSG_DETAILS][self.MSG_FAILURE] = e.message
            body[self.MSG_DATA][self.MSG_HOLDING_LIST] = []
        else:
            # add the return list to successfully completed holding listings
            body[self.MSG_DATA][self.MSG_HOLDING_LIST] = ret_dict
            self.log(
                f"Listing files from CATALOG_FIND {ret_dict}",
                self.RK_LOG_DEBUG
            )

        self.catalog.end_session()

        self.publish_message(
            properties.reply_to,
            msg_dict=body,
            exchange={'name': ''},
            correlation_id=properties.correlation_id
        )


    def _catalog_meta(self, body: Dict, properties: Header) -> None:
        """Change metadata for a user's holding"""
                # Parse the message body for required variables
        message_vars = self._parse_user_vars(body)
        if message_vars is None:
            # Check if any problems have occurred in the parsing of the message 
            # body and exit if necessary 
            self.log("Could not parse one or more mandatory variables, exiting "
                     "callback.", self.RK_LOG_ERROR)
            return
        else:
            # Unpack if no problems found in parsing
            user, group = message_vars

        # Extract variables from the metadata section of the message body
        md = Metadata(body)
        holding_label, holding_id, tag, _ = md.unpack

        # get the new label from the new meta section of the message
        try:
            new_label = body[self.MSG_META][self.MSG_NEW_META][self.MSG_LABEL]
        except KeyError:
            new_label = None

        # get the new tag(s) from the new meta section of the message
        try:
            new_tag = body[self.MSG_META][self.MSG_NEW_META][self.MSG_TAG]
        except KeyError:
            new_tag = None

        # get the deleted tag(s) from the new_meta section of the message
        try:
            del_tag = body[self.MSG_META][self.MSG_NEW_META][self.MSG_DEL_TAG]
        except KeyError:
            del_tag = None

        self.catalog.start_session()

        # if there is the holding label or holding id then get the holding
        try:
            if not holding_label and not holding_id and not tag:
                raise CatalogError(
                    "Holding not found: holding_id or label or tag(s) not specified."
                )
            holdings = self.catalog.get_holding(
                user, group, holding_label, holding_id, tag=tag
            )
            ret_list = []
            for holding in holdings:
                # get the old metadata so we can record it, then modify
                old_meta = {
                    "label": holding.label,
                    "tags" : holding.get_tags()
                }
                holding = self.catalog.modify_holding(
                    holding, new_label, new_tag, del_tag
                )
                # record the new metadata
                new_meta = {
                    "label": holding.label,
                    "tags":  holding.get_tags()
                }
                # build the return dictionary and append it to the list of
                # holdings that have been modified
                ret_dict = {
                    "id": holding.id,
                    "user": holding.user,
                    "group": holding.group,
                    "old_meta" : old_meta,
                    "new_meta" : new_meta,
                }
                ret_list.append(ret_dict)

            self.catalog.save()
        except CatalogError as e:
            # failed to get the holdings - send a return message saying so
            self.log(e.message, self.RK_LOG_ERROR)
            body[self.MSG_DETAILS][self.MSG_FAILURE] = e.message
            body[self.MSG_DATA][self.MSG_HOLDING_LIST] = []
        else:
            # fill the return message with a dictionary of the holding(s)
            body[self.MSG_DATA][self.MSG_HOLDING_LIST] = ret_list
            self.log(
                f"Modified metadata from CATALOG_META {ret_list}",
                self.RK_LOG_DEBUG
            )

        self.catalog.end_session()

        # return message to complete RPC
        self.publish_message(
            properties.reply_to,
            msg_dict=body,
            exchange={'name': ''},
            correlation_id=properties.correlation_id
        )       


    def attach_database(self, create_db_fl: bool = True):
        """Attach the Catalog to the consumer"""
        # Load config options or fall back to default values.
        db_engine = self.load_config_value(self._DB_ENGINE)
        db_options = self.load_config_value(self._DB_OPTIONS)
        self.catalog = Catalog(db_engine, db_options)

        try:
            db_connect = self.catalog.connect(create_db_fl=create_db_fl)
            if create_db_fl:
                self.log(f"db_connect string is {db_connect}", RMQC.RK_LOG_DEBUG)
        except DBError as e:
            self.log(e.message, RMQC.RK_LOG_CRITICAL)


    def get_engine(self):
        # Method for making the db_engine available to alembic
        return self.database.db_engine
    

    def get_url(self):
        """ Method for making the sqlalchemy url available to alembic"""
        # Create a minimum version of the catalog to put together a url
        if self.catalog is None:
            db_engine = self.load_config_value(self._DB_ENGINE)
            db_options = self.load_config_value(self._DB_OPTIONS)
            self.catalog = Catalog(db_engine, db_options)
        return self.catalog.get_db_string()


    def callback(self, ch: Channel, method: Method, properties: Header, 
                 body: bytes, connection: Connection) -> None:
        # Reset member variables
        self.reset()

        # Connect to database if not connected yet                
        # Convert body from bytes to json for ease of manipulation
        body = json.loads(body)

        self.log(f"Received {json.dumps(body, indent=4)} from "
                 f"{self.queues[0].name} ({method.routing_key})", 
                 self.RK_LOG_INFO)

        # Get the API method and decide what to do with it
        try:
            api_method = body[RMQC.MSG_DETAILS][RMQC.MSG_API_ACTION]
        except KeyError:
            self.log(f"Message did not contain appropriate API method", 
                     self.RK_LOG_ERROR)
            return

        # check whether this is a GET or a PUT
        if (api_method == self.RK_GETLIST) or (api_method == self.RK_GET):
            # split the routing key
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
                self.log("Routing key inappropriate length, exiting callback.",
                         self.RK_LOG_ERROR)
                return 
            if (rk_parts[1] == self.RK_CATALOG_GET):
                self._catalog_get(body, rk_parts[0], api_method=self.RK_GET)
            elif (rk_parts[1] == self.RK_CATALOG_DEL):
                # If part of a GET transaction but received via the del topic 
                # then delete the new object storage locations added to the 
                # catalog.
                self._catalog_location_del(body, rk_parts[0], 
                                           location_type=Storage.OBJECT_STORAGE)
                
                
        
            
        # If received system test message, reply to it (this is for system status check)
        elif api_method == "system_stat":
            if properties.correlation_id is not None and properties.correlation_id != self.channel.consumer_tags[0]:
                return False
            if (body["details"]["ignore_message"]) == True:
                return
            else:
                self.publish_message(
                    properties.reply_to,
                    msg_dict=body,
                    exchange={'name': ''},
                    correlation_id=properties.correlation_id
                )
            return
            
        

        elif (api_method == self.RK_PUTLIST) or (api_method == self.RK_PUT):
            # split the routing key
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
                self.log("Routing key inappropriate length, exiting callback.",
                        self.RK_LOG_ERROR)
                return             
            if (rk_parts[2] == self.RK_START):
                # Check the routing key worker section to determine which method 
                # to call, as a del could be being called from a failed 
                # transfer_put
                if (rk_parts[1] == self.RK_CATALOG_PUT):
                    self._catalog_put(body, rk_parts[0])
                elif (rk_parts[1] == self.RK_CATALOG_DEL):
                    self._catalog_del(body, rk_parts[0])

        # Archive put requires getting from the catalog
        elif (api_method == self.RK_ARCHIVE_PUT):
            # split the routing key
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
                self.log("Routing key inappropriate length, exiting callback.",
                         self.RK_LOG_ERROR)
            
            if (rk_parts[1] == self.RK_CATALOG_GET):
                self._catalog_get(body, rk_parts[0], api_method=self.RK_ARCHIVE)
            elif (rk_parts[1] == self.RK_CATALOG_DEL):
                self._catalog_location_del(body, rk_parts[0], 
                                           location_type=Storage.TAPE)

        elif (api_method == self.RK_LIST):
            # don't need to split any routing key for an RPC method
            self._catalog_list(body, properties)

        elif (api_method == self.RK_FIND):
            # don't need to split any routing key for an RPC method
            self._catalog_find(body, properties)

        elif (api_method == self.RK_META):
            # don't need to split any routing key for an RPC method
            self._catalog_meta(body, properties)

        elif (api_method == self.RK_STAT):
            self._catalog_stat(body, properties)


def main():
    consumer = CatalogConsumer()
    # connect to message queue early so that we can send logging messages about
    # connecting to the database
    consumer.get_connection()
    consumer.attach_database()
    # run the loop
    consumer.run()


if __name__ == "__main__":
    main()