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
from nlds_processors.catalog.catalog_models import Storage
from nlds.details import PathDetails
from nlds_processors.db_mixin import DBError

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

    DEFAULT_CONSUMER_CONFIG = {
        _DB_ENGINE: "sqlite",
        _DB_OPTIONS: {
            _DB_OPTIONS_DB_NAME: "/nlds_catalog.db",
            _DB_OPTIONS_USER: "",
            _DB_OPTIONS_PASSWD: "",
            _DB_ECHO: True,
        },
        _MAX_RETRIES: 5,
        RMQC.RETRY_DELAYS: RMQC.DEFAULT_RETRY_DELAYS
    }


    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.max_retries = self.load_config_value(
            self._MAX_RETRIES
        )
        self.retry_delays = self.load_config_value(
            self.RETRY_DELAYS
        )

        self.catalog = None


    def reset(self):
        super().reset()

        self.completelist = []
        self.retrylist = []
        self.failedlist = []


    def _catalog_put(self, body: dict, rk_origin: str) -> None:
        """Put a file record into the catalog - end of a put transaction"""
        # get the filelist from the data section of the message
        try:
            filelist = body[self.MSG_DATA][self.MSG_FILELIST]
        except KeyError as e:
            self.log(f"Invalid message contents, filelist should be in the data "
                     f"section of the message body.",
                     self.RK_LOG_ERROR)
            return

        # check filelist is a list
        try:
            f = filelist[0]
        except TypeError as e:
            self.log(f"filelist field must contain a list", self.RK_LOG_ERROR)
            return

        # get the transaction id from the details section of the message
        try: 
            transaction_id = body[self.MSG_DETAILS][self.MSG_TRANSACT_ID]
        except KeyError:
            self.log("Transaction id not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the user id from the details section of the message
        try:
            user = body[self.MSG_DETAILS][self.MSG_USER]
        except KeyError:
            self.log("User not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the group from the details section of the message
        try:
            group = body[self.MSG_DETAILS][self.MSG_GROUP]
        except KeyError:
            self.log("Group not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the label from the metadata section of the message
        try:
            label = body[self.MSG_META][self.MSG_LABEL]
            # if holding id not given then new_label used to create holding
            new_label = label        
        except KeyError:
            # generate the label from the UUID - should loop here to make sure 
            # we get a unique label
            label = None
            # no label given so if holding_id not given the subset of 
            # transaction id is used as new label
            new_label = transaction_id[0:8] 

        # get the holding_id from the metadata section of the message
        try:
            holding_id = body[self.MSG_META][self.MSG_HOLDING_ID]
        except KeyError:
            holding_id = None

        # get any tags that exist
        try:
            tags = body[self.MSG_META][self.MSG_TAG]
        except KeyError:
            tags = None

        # start the database transactions
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
                
                file = self.catalog.create_file(
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
                    file,
                    Storage.OBJECT_STORAGE,
                    transaction.transaction_id,
                    object_name, 
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


    def _catalog_get(self, body: dict, rk_origin: str) -> None:
        """Get the details for each file in a filelist and send it to the 
        exchange to be processed by the transfer processor"""
        # get the filelist from the data section of the message
        try:
            filelist = body[self.MSG_DATA][self.MSG_FILELIST]
        except KeyError as e:
            self.log(f"Invalid message contents, filelist should be in the data"
                     f"section of the message body.",
                     self.RK_LOG_ERROR)
            return

        # get the user id from the details section of the message
        try:
            user = body[self.MSG_DETAILS][self.MSG_USER]
        except KeyError:
            self.log("User not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the group from the details section of the message
        try:
            group = body[self.MSG_DETAILS][self.MSG_GROUP]
        except KeyError:
            self.log("Group not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the holding_id from the metadata section of the message
        try:
            holding_id = body[self.MSG_META][self.MSG_HOLDING_ID]
        except KeyError:
            holding_id = None

        # get the holding label from the details section of the message
        # could (legit) be None
        try: 
            holding_label = body[self.MSG_META][self.MSG_LABEL]
        except KeyError:
            holding_label = None

        # get the transaction id from the details section of the message
        try: 
            transaction_id = body[self.MSG_META][self.MSG_TRANSACT_ID]
        except KeyError:
            transaction_id = None

        # get the holding tag from the details section of the message
        try:
            holding_tag = body[self.MSG_META][self.MSG_TAG]
        except KeyError:
            holding_tag = None

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
                for file in files:
                    try:
                        location = self.catalog.get_location(
                            file, Storage.OBJECT_STORAGE
                        )
                        if location is None:
                            raise CatalogError(
                                f"Could not find location for file with original path "
                                f"{file_details.original_path}"
                            )         
                        object_name = ("nlds." +
                                    location.root + ":" + 
                                    location.path)
                        access_time = location.access_time.timestamp()
                        # create a new PathDetails with all the info from the DB
                        new_file = PathDetails(
                            original_path = file.original_path,
                            object_name = object_name,
                            size = file.size,
                            user = file.user,
                            group = file.group,
                            permissions = file.file_permissions,                    
                            access_time = access_time,
                            path_type = file.path_type,
                            link_path = file.link_path
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
                    self.completelist.append(new_file)

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
                                    self.RK_CATALOG_GET, 
                                    self.RK_COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_GET {self.completelist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.completelist, rk_complete, body, 
                               state=State.CATALOG_GETTING)
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

    def _catalog_del(self, body: dict, rk_origin: str) -> None:
        """Remove a given list of files from the catalog if the transfer 
        fails"""
        # get the filelist from the data section of the message
        try:
            filelist = body[self.MSG_DATA][self.MSG_FILELIST]
        except KeyError as e:
            self.log(f"Invalid message contents, filelist should be in the data"
                     f"section of the message body.",
                     self.RK_LOG_ERROR)
            return
        
        # check filelist is a list
        try:
            f = filelist[0]
        except TypeError as e:
            self.log(f"filelist field must contain a list", self.RK_LOG_ERROR)
            return

        # get the user id from the details section of the message
        try:
            user = body[self.MSG_DETAILS][self.MSG_USER]
        except KeyError:
            self.log("User not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the group from the details section of the message
        try:
            group = body[self.MSG_DETAILS][self.MSG_GROUP]
        except KeyError:
            self.log("Group not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the holding_id from the metadata section of the message
        try:
            holding_id = body[self.MSG_META][self.MSG_HOLDING_ID]
        except KeyError:
            holding_id = None

        # get the holding label from the details section of the message
        # could (legit) be None
        try: 
            holding_label = body[self.MSG_META][self.MSG_LABEL]
        except KeyError:
            holding_label = None

        # get the tags from the details sections of the message
        try:
            tag = body[self.MSG_META][self.MSG_TAG]
        except KeyError:
            tag = None

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
                # outsource deleting to the catalog itself
                self.catalog.delete_files(
                    user, group, holding_label=holding_label, 
                    holding_id=holding_id, path=file_details.original_path,
                    tag=tag
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


    def _catalog_list(self, body: dict, properties: Header) -> None:
        """List the users holdings"""
        # get the user id from the details section of the message
        try:
            user = body[self.MSG_DETAILS][self.MSG_USER]
        except KeyError:
            self.log("User not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the group from the details section of the message
        try:
            group = body[self.MSG_DETAILS][self.MSG_GROUP]
        except KeyError:
            self.log("Group not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the holding_id from the metadata section of the message
        try:
            holding_id = body[self.MSG_META][self.MSG_HOLDING_ID]
        except KeyError:
            holding_id = None

        # get the transaction id if it exists
        try:
            transaction_id = body[self.MSG_META][self.MSG_TRANSACT_ID]
        except KeyError:
            transaction_id = None

        # get the holding label from the details section of the message
        # could (legit) be None
        try: 
            holding_label = body[self.MSG_META][self.MSG_LABEL]
        except KeyError:
            holding_label = None

        # get the tags from the details sections of the message
        try:
            tag = body[self.MSG_META][self.MSG_TAG]
        except KeyError:
            tag = None

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
    

    def _catalog_stat(self, body: dict, properties: Header) -> None:
        """Get the labels for a list of transaction ids"""
        # get the user id from the details section of the message
        try:
            user = body[self.MSG_DETAILS][self.MSG_USER]
        except KeyError:
            self.log("User not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the group from the details section of the message
        try:
            group = body[self.MSG_DETAILS][self.MSG_GROUP]
        except KeyError:
            self.log("Group not in message, exiting callback.", self.RK_LOG_ERROR)
            return

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


    def _catalog_find(self, body: dict, properties: Header) -> None:
        """List the user's files"""
        # get the user id from the details section of the message
        try:
            user = body[self.MSG_DETAILS][self.MSG_USER]
        except KeyError:
            self.log("User not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the group from the details section of the message
        try:
            group = body[self.MSG_DETAILS][self.MSG_GROUP]
        except KeyError:
            self.log("Group not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the holding_id from the metadata section of the message
        try:
            holding_id = body[self.MSG_META][self.MSG_HOLDING_ID]
        except KeyError:
            holding_id = None

        # get the holding label from the details section of the message
        # could (legit) be None
        try: 
            holding_label = body[self.MSG_META][self.MSG_LABEL]
        except KeyError:
            holding_label = None

        # get the transaction id from the details section of the message
        try: 
            transaction_id = body[self.MSG_META][self.MSG_TRANSACT_ID]
        except KeyError:
            transaction_id = None

        # get the path from the detaisl section of the message
        try:
            path = body[self.MSG_META][self.MSG_PATH]
        except KeyError:
            path = None

        # get the tags from the details sections of the message
        try:
            tag = body[self.MSG_META][self.MSG_TAG]
        except KeyError:
            tag = None

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


    def _catalog_meta(self, body: dict, properties: Header) -> None:
        """Change metadata for a user's holding"""
        # get the user id from the details section of the message
        try:
            user = body[self.MSG_DETAILS][self.MSG_USER]
        except KeyError:
            self.log("User not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the group from the details section of the message
        try:
            group = body[self.MSG_DETAILS][self.MSG_GROUP]
        except KeyError:
            self.log("Group not in message, exiting callback.", self.RK_LOG_ERROR)
            return

        # get the holding_id from the metadata section of the message
        try:
            holding_id = body[self.MSG_META][self.MSG_HOLDING_ID]
        except KeyError:
            holding_id = None

        # get the holding label from the details section of the message
        # could (legit) be None
        try: 
            holding_label = body[self.MSG_META][self.MSG_LABEL]
        except KeyError:
            holding_label = None

        # get the tags from the details sections of the message
        try:
            tag = body[self.MSG_META][self.MSG_TAG]
        except KeyError:
            tag = None

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


    def attach_catalog(self):
        """Attach the Catalog to the consumer"""
        # Load config options or fall back to default values.
        db_engine = self.load_config_value(self._DB_ENGINE)
        db_options = self.load_config_value(self._DB_OPTIONS)
        self.catalog = Catalog(db_engine, db_options)

        try:
            db_connect = self.catalog.connect()
            self.log(f"db_connect string is {db_connect}", RMQC.RK_LOG_DEBUG)
        except DBError as e:
            self.log(e.message, RMQC.RK_LOG_CRITICAL)


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
            if (rk_parts[2] == self.RK_START):
                self._catalog_get(body, rk_parts[0])
                
                
        
            
        # If recieved system test message, reply to it (this is for system status check)
        elif api_method == "system_stat":
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
    consumer.attach_catalog()
    # run the loop
    consumer.run()


if __name__ == "__main__":
    main()