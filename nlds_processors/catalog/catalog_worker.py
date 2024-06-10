"""

"""

__author__ = "Neil Massey and Jack Leland"
__date__ = "15 Sep 2022"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

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
from datetime import datetime

from urllib.parse import urlunsplit

# Typing imports
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

from nlds.rabbit.consumer import RabbitMQConsumer as RMQC
from nlds.rabbit.consumer import State
from nlds.errors import CallbackError

from nlds_processors.catalog.catalog import Catalog
from nlds_processors.catalog.catalog_error import CatalogError
from nlds_processors.catalog.catalog_models import Storage, Location
from nlds.details import PathDetails
from nlds_processors.db_mixin import DBError
from nlds_processors.utils.aggregations import aggregate_files

import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


class Metadata:
    """Container class for the meta section of the message body."""

    label: str
    holding_id: int
    transaction_id: str
    tags: Dict
    groupall: bool

    def __init__(self, body: Dict):
        # Get the label from the metadata section of the message
        try:
            self.label = body[MSG.META][MSG.LABEL]
        except KeyError:
            self.label = None

        # Get the holding_id from the metadata section of the message
        try:
            self.holding_id = body[MSG.META][MSG.HOLDING_ID]
        except KeyError:
            self.holding_id = None

        # Get any tags that exist
        try:
            self.tags = body[MSG.META][MSG.TAG]
        except KeyError:
            self.tags = None

        # get the transaction id from the metadata section of the message
        try:
            self.transaction_id = body[MSG.META][MSG.TRANSACT_ID]
        except KeyError:
            self.transaction_id = None

    @property
    def unpack(self) -> Tuple:
        return (self.label, self.holding_id, self.tags, self.transaction_id)


def format_datetime(date: datetime):
    try:
        datetime_str = date.isoformat()
    except AttributeError:
        datetime_str = "0000-01-01T00:00:00"
    return datetime_str


class CatalogConsumer(RMQC):
    DEFAULT_QUEUE_NAME = "catalog_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}.{RK.CATALOG}.{RK.WILD}"
    DEFAULT_REROUTING_INFO = "->CATALOG_Q"
    DEFAULT_STATE = State.CATALOG_PUTTING

    # Possible options to set in config file
    _DB_ENGINE = "db_engine"
    _DB_OPTIONS = "db_options"
    _DB_OPTIONS_DB_NAME = "db_name"
    _DB_OPTIONS_USER = "db_user"
    _DB_OPTIONS_PASSWD = "db_passwd"
    _DB_ECHO = "echo"
    _DEFAULT_TENANCY = "default_tenancy"
    _DEFAULT_TAPE_URL = "default_tape_url"
    _TARGET_AGGREGATION_SIZE = "target_aggregation_size"
    _FULLY_UNPACK_TAR = "fully_unpack_tar_fl"

    DEFAULT_CONSUMER_CONFIG = {
        _DB_ENGINE: "sqlite",
        _DB_OPTIONS: {
            _DB_OPTIONS_DB_NAME: "/nlds_catalog.db",
            _DB_OPTIONS_USER: "",
            _DB_OPTIONS_PASSWD: "",
            _DB_ECHO: True,
        },
        _DEFAULT_TENANCY: None,
        _DEFAULT_TAPE_URL: None,
        _TARGET_AGGREGATION_SIZE: 5 * (1024**3),  # Default to 5 GB
        _FULLY_UNPACK_TAR: False,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.default_tape_url = self.load_config_value(self._DEFAULT_TAPE_URL)
        self.default_tenancy = self.load_config_value(self._DEFAULT_TENANCY)
        self.target_aggregation_size = self.load_config_value(
            self._TARGET_AGGREGATION_SIZE
        )
        self.fully_unpack_tar_fl = self.load_config_value(self._FULLY_UNPACK_TAR)

        self.catalog = None
        self.reroutelist = []
        self.retrievedict = {}

    @property
    def database(self):
        return self.catalog

    def reset(self):
        super().reset()

        self.completelist.clear()
        self.failedlist.clear()
        # New list for rerouting to archive if not found on object store
        self.reroutelist.clear()
        self.retrievedict.clear()

    def _parse_filelist(self, body: Dict) -> list[str]:
        # get the filelist from the data section of the message
        try:
            filelist = body[MSG.DATA][MSG.FILELIST]
        except KeyError as e:
            self.log(
                f"Invalid message contents, filelist should be in the data "
                f"section of the message body.",
                RK.LOG_ERROR,
            )
            return

        # check filelist is indexable
        try:
            _ = filelist[0]
        except TypeError as e:
            msg = f"filelist field must contain a list"
            self.log(msg, RK.LOG_ERROR)
            raise CatalogError(message=msg)

        return filelist

    def _parse_user_vars(self, body: Dict) -> Tuple:
        # get the user id from the details section of the message
        try:
            user = body[MSG.DETAILS][MSG.USER]
        except KeyError:
            msg = "User not in message, exiting callback."
            self.log(msg, RK.LOG_ERROR)
            raise CatalogError(message=msg)

        # get the group from the details section of the message
        try:
            group = body[MSG.DETAILS][MSG.GROUP]
        except KeyError:
            msg = "Group not in message, exiting callback."
            self.log(msg, RK.LOG_ERROR)
            raise CatalogError(message=msg)

        return user, group

    def _parse_transaction_id(self, body: Dict, mandatory: bool = False) -> str:
        # get the transaction id from the details section of the message. It is
        # a mandatory variable for the PUT workflow
        try:
            transaction_id = body[MSG.DETAILS][MSG.TRANSACT_ID]
        except KeyError:
            if mandatory:
                msg = "Transaction id not in message, exiting callback."
                self.log(msg, RK.LOG_ERROR)
                raise CatalogError(message=msg)
            else:
                transaction_id = None
        return transaction_id

    def _parse_tenancy(self, body: Dict) -> str:
        # Get the tenancy from message, if none found then use the configured default
        tenancy = self.default_tenancy
        try:
            tenancy = body[MSG.DETAILS][MSG.TENANCY]
        except KeyError:
            pass
        return tenancy

    def _parse_metadata_vars(self, body: Dict) -> Tuple:
        """Convenience function to prevent unnecessary code replication for
        extraction of metadata variables from the message body. This is
        specifically for requesting particular holdings/transactions/labels/tags
        in a given catalog function. Returns a tuple of label, holding_id and
        tags, with each being None if not found.
        """
        md = Metadata(body)
        return md.label, md.holding_id, md.tags, md.transaction_id

    def _parse_groupall(self, body: Dict) -> str:
        try:
            groupall = body[MSG.DETAILS][MSG.GROUPALL]
        except KeyError:
            groupall = False
        return groupall

    def _parse_tape_url(self, body: Dict) -> str:
        # Get the tape_url from message, if none found then use the configured default
        tape_url = self.default_tape_url
        try:
            tape_url = body[MSG.DETAILS][MSG.TAPE_URL]
        except KeyError:
            pass
        return tape_url

    def _parse_aggregation_id(self, body: Dict) -> str:
        # Parse aggregation and checksum info from message.
        try:
            aggregation_id = body[MSG.META][MSG.AGGREGATION_ID]
        except KeyError:
            self.log(
                "Aggregation id not in message, continuing without.", RK.LOG_WARNING
            )
            aggregation_id = None
        return aggregation_id

    def _parse_transaction_records(self, body: dict) -> list[str]:
        # get the transaction_ids from the metadata section of the message
        try:
            transaction_records = body[MSG.DATA][MSG.RECORD_LIST]
        except KeyError:
            msg = "Transaction ids not in message, exiting callback."
            self.log(msg, RK.LOG_ERROR)
            raise CatalogError(message=msg)

        # Verify transaction list
        if transaction_records is None or len(transaction_records) <= 0:
            msg = (
                "Passed list of transactions is not valid, check message "
                "contents. Exiting callback"
            )
            self.log(msg, RK.LOG_ERROR)
            raise CatalogError(message=msg)
        return transaction_records

    def _parse_path(self, body: dict) -> str:
        # get the path from the details section of the message
        try:
            path = body[MSG.META][MSG.PATH]
        except KeyError:
            path = None
        return path

    def _parse_new_metadata_variables(self, body: dict) -> tuple:
        # get the new label from the new meta section of the message
        try:
            new_label = body[MSG.META][MSG.NEW_META][MSG.LABEL]
        except KeyError:
            new_label = None

        # get the new tag(s) from the new meta section of the message
        try:
            new_tag = body[MSG.META][MSG.NEW_META][MSG.TAG]
        except KeyError:
            new_tag = None

        # get the deleted tag(s) from the new_meta section of the message
        try:
            del_tag = body[MSG.META][MSG.NEW_META][MSG.DEL_TAG]
        except KeyError:
            del_tag = None

        return new_label, new_tag, del_tag

    def _get_search_label(self, holding_label, holding_id):
        """Determine the search label, this is a regex and depends on whether the
        holding_label and/or holding_id has been supplied"""
        if holding_label:
            search_label = holding_label
        elif holding_id:
            search_label = ".*"  # match all labels if holding id given
        else:
            search_label = "^$"  # match nothing if no label or holding id
            # this will produce a new holding
        return search_label

    def _get_or_create_holding(self, user, group, label, holding_id, new_label):
        """Get a holding via label or holding_id.
        If the holding doesn't already exist then create it."""
        # try to get the holding to see if it already exists and can be added to
        try:
            # don't use tags to search - they are strictly for adding to the holding
            holding = self.catalog.get_holding(
                user, group, label=label, holding_id=holding_id
            )
        except (KeyError, CatalogError):
            holding = None

        if holding is None:
            # if the holding_id is not None then raise an error as the user is
            # trying to add to a holding that doesn't exist, but creating a new
            # holding won't have a holding_id that matches the one they passed in
            if holding_id is not None:
                message = (
                    f"Could not add files to holding with holding_id: "
                    f"{holding_id}.  holding_id does not exist."
                )
                self.log(message, RK.LOG_DEBUG)
                raise CallbackError(message)
            try:
                holding = self.catalog.create_holding(user, group, new_label)
            except CatalogError as e:
                self.log(e.message, RK.LOG_ERROR)
                raise e
        else:
            holding = holding[0]
        return holding

    def _get_or_create_transaction(self, transaction_id, holding):
        # try to get the transaction to see if it already exists and can be
        # added to
        try:
            transaction = self.catalog.get_transaction(transaction_id=transaction_id)
        except (KeyError, CatalogError):
            transaction = None

        # create the transaction within the  holding if it doesn't exist
        if transaction is None:
            try:
                transaction = self.catalog.create_transaction(holding, transaction_id)
            except CatalogError as e:
                self.log(e.message, RK.LOG_ERROR)
                raise e
        return transaction

    def _create_tags(self, tags, holding, label):
        # add the tags - if the tag already exists then don't add it or modify
        # it, with the reasoning that the user can change it with the `meta`
        # command.
        warnings = []
        if tags:
            for k in tags:
                try:
                    tag = self.catalog.get_tag(holding, k)
                except CatalogError:  # tag's key not found so create
                    self.catalog.create_tag(holding, k, tags[k])
                else:
                    # append a warning that the tag could not be added to the holding
                    warnings.append(
                        f"Tag with key:{k} could not be added to holding with label"
                        f":{label} as that tag already exists.  Tags can be modified"
                        f" using the meta command"
                    )
        return warnings

    def _catalog_put(self, body: Dict, rk_origin: str) -> None:
        """Put a file record into the catalog - end of a put transaction"""
        # Parse the message body for required variables
        try:
            filelist = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
            transaction_id = self._parse_transaction_id(body, mandatory=True)
            tenancy = self._parse_tenancy(body)
            label, holding_id, tags, _ = self._parse_metadata_vars(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        if label is None:
            # No label given so if holding_id not given the subset of
            # transaction id is used as new label
            new_label = transaction_id[0:8]
        else:
            # If holding id not given then new_label used to create holding
            new_label = label

        # Start the database transactions
        self.catalog.start_session()

        # get the (regex) search label
        search_label = self._get_search_label(label, holding_id)

        # get, or create the holding
        holding = self._get_or_create_holding(
            user, group, label=search_label, holding_id=holding_id, new_label=new_label
        )

        # get or create the transaction
        transaction = self._get_or_create_transaction(transaction_id, holding)

        # loop over the filelist
        for f in filelist:
            # convert to PathDetails class
            pd = PathDetails.from_dict(f)
            try:
                # Search first for file existence within holding, fail if present
                try:
                    files = self.catalog.get_files(
                        user,
                        group,
                        holding_id=holding.id,
                        original_path=pd.original_path,
                    )
                except CatalogError:
                    pass  # should throw a catalog error if file(s) not found
                else:
                    raise CatalogError("File already exists in holding")

                # create the file
                file_ = self.catalog.create_file(
                    transaction,
                    pd.user,
                    pd.group,
                    pd.original_path,
                    pd.path_type,
                    pd.link_path,
                    pd.size,
                    pd.permissions,
                )
                self.completelist.append(pd)
            except CatalogError as e:
                pd.failure_reason = e.message
                self.failedlist.append(pd)
                self.log(e.message, RK.LOG_ERROR)
                continue

        # Add any user tags to the holding
        tag_warnings = self._create_tags(tags, holding, label)

        # stop db transitions and commit
        self.catalog.save()
        self.catalog.end_session()

        # log the successful and non-successful catalog puts
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin, RK.CATALOG_PUT, RK.COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_PUT {self.completelist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.CATALOG_PUTTING,
                warning=tag_warnings,
            )
        # FAILED
        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin, RK.CATALOG_PUT, RK.FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_PUT {self.failedlist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body,
                state=State.FAILED,
                warning=tag_warnings,
            )

    def _catalog_amend(self, body: Dict, rk_origin: str) -> None:
        """Upon completion of a TRANSFER_PUT, the list of completed files is returned
        back to the NLDS worker, but with location on Object Storage of the files
        appended to each PathDetails JSON object.
        The NLDS worker then passes this message to the Catalog, and this function
        processes the PathDetails and amends each file's record in the database to
        contain the Object Storage location."""
        # Parse the message body for required variables
        try:
            filelist = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
            transaction_id = self._parse_transaction_id(body, mandatory=True)
        except CatalogError as e:
            # functions above handled message logging, here we just return
            raise e

        self.catalog.start_session()
        self.completelist.clear()
        self.failedlist.clear()

        # loop over the filelist
        for f in filelist:
            # convert to PathDetails class
            pd = PathDetails.from_dict(f)
            # need to
            #   1. find the file,
            #   2. create the object storage location,
            #   3. add the details to the object storage location from the PathDetails
            pl = pd.get_object_store()  # this returns a PathLocation object
            # get the file
            try:
                file = self.catalog.get_files(
                    user,
                    group,
                    transaction_id=transaction_id,
                    original_path=pd.original_path,
                )[
                    0
                ]  # just one file
                location = self.catalog.create_location(
                    file,
                    storage_type=Storage.from_str(pl.storage_type),
                    url_scheme=pl.url_scheme,
                    url_netloc=pl.url_netloc,
                    root=pl.root,
                    path=pl.path,
                    access_time=pl.access_time,
                )
                self.completelist.append(pd)
            except CatalogError as e:
                # the file wasn't found or the location couldn't be created
                pd.failure_reason = e.message
                self.failedlist.append(pd)
                self.log(e.message, RK.LOG_ERROR)
                continue

        # stop db transitions and commit
        self.catalog.save()
        self.catalog.end_session()

        # log the successful and non-successful catalog amends
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin, RK.CATALOG_AMEND, RK.COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_AMEND {self.completelist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.CATALOG_AMEND,
            )
        # FAILED
        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin, RK.CATALOG_AMEND, RK.FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_AMEND {self.failedlist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body,
                state=State.FAILED,
            )

    def _catalog_get(self, body: Dict, rk_origin: str) -> None:
        """Get the details for each file in a filelist and send it to the
        exchange to be processed by the transfer processor. If any file is only
        found on tape then it will be first rerouted to the archive processor
        for retrieval to object store cache."""
        # Parse the message body for required variables
        try:
            filelist = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
            transaction_id = self._parse_transaction_id(body)
            tenancy = self._parse_tenancy(body)
            holding_label, holding_id, holding_tag, _ = self._parse_metadata_vars(body)
            groupall = self._parse_groupall(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        # Start a set of the aggregations we need to retrieve
        aggs_to_retrieve: Dict[int, List] = dict()

        # start the database transactions
        self.catalog.start_session()

        for f in filelist:
            file_details = PathDetails.from_dict(f)
            try:
                # get the files first
                files = self.catalog.get_files(
                    user,
                    group,
                    groupall=groupall,
                    holding_label=holding_label,
                    holding_id=holding_id,
                    transaction_id=transaction_id,
                    original_path=file_details.original_path,
                    tag=holding_tag,
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
                            self.log(
                                "Searching for copy of file in archive", RK.LOG_INFO
                            )
                            location = self.catalog.get_location(file_, Storage.TAPE)
                            transaction = self.catalog.get_location_transaction(
                                location
                            )
                            root = transaction.transaction_id
                        else:
                            root = location.root

                        # If still None then file doesn't exist within the NLDS
                        if location is None:
                            raise CatalogError(
                                f"Could not find location for file with "
                                f"original path {file_details.original_path}."
                            )
                    except CatalogError as e:
                        file_details.failure_reason = e.message
                        self.failedlist.append(file_details)
                        self.log(e.message, RK.LOG_ERROR)
                        continue
                    # NRM - TODO - this needs sorting out
                    # Make the object name. (2023-09) As of now the root will
                    # always be the transaction id, so files are retrieved from
                    # tape into the same original bucket name (for URIs).
                    object_name = f"nlds.{root}:{location.path}"
                    access_time = location.access_time.timestamp()
                    # create a new PathDetails with all the info from the DB
                    new_file = PathDetails(
                        original_path=file_.original_path,
                        path_type=file_.path_type,
                        size=file_.size,
                        user=file_.user,
                        group=file_.group,
                        permissions=file_.file_permissions,
                        access_time=access_time,
                        link_path=file_.link_path,
                    )
                    # NRM - TODO - this all needs revisiting
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
                        tape_object_name = f"nlds.{location.root}:{location.path}"
                        new_file.tenancy = tenancy
                        new_file.tape_url = location.url_netloc
                        new_file.tape_path = tape_object_name

                        # Get the aggregation so we can group by tarname in
                        # retrieval_dict
                        agg = self.catalog.get_aggregation(
                            aggregation_id=location.aggregation_id
                        )
                        if self.fully_unpack_tar_fl:
                            # Save the aggregation for later so we can add
                            # objectstore location for all of the files needed
                            # to be retrieved from tape.
                            if agg in aggs_to_retrieve:
                                aggs_to_retrieve[agg].append(new_file)
                            else:
                                aggs_to_retrieve[agg] = [
                                    new_file,
                                ]
                        else:
                            # If we just want the files requested from the
                            # archive then make the necessary Location and
                            # message data now.
                            self.create_objectstore_location(location, tenancy)
                            self.reroutelist.append(new_file)
                            if agg.tarname in self.retrievedict:
                                self.retrievedict[agg.tarname].append(new_file)
                            else:
                                self.retrievedict[agg.tarname] = [
                                    new_file,
                                ]

            except CatalogError as e:
                file_details.failure_reason = e.message
                self.failedlist.append(file_details)
                self.log(e.message, RK.LOG_ERROR)
                continue

        # Create object store Locations for each member of an aggregation (if
        # unpacking the whole tar file)
        for aggregation, details_list in aggs_to_retrieve.items():
            # Make a session checkpoint here so we can rollback in the event of
            # a failed Location creation
            # TODO: We need a way to mark a file as being a problem
            checkpoint = self.catalog.session.begin_nested()
            try:
                retrievelist = []
                # Add the new objectstore location to the catalog now,
                # to be removed in the event of an s3 get failure.
                for tape_location in aggregation.locations:
                    path_details = self.create_objectstore_location(
                        tape_location, tenancy
                    )
                    if path_details is None:
                        continue
                    retrievelist.append(path_details)
            except CatalogError as e:
                # In the event of a failure we rollback all the added locations
                # and add the original file_details to the fail list
                checkpoint.rollback()
                for file_details in details_list:
                    file_details.failure_reason = e.message
                    self.failedlist.append(file_details)
                self.log(e.message, RK.LOG_ERROR)
                continue
            else:
                # Add the file_details to the reroute list for archive retrieval
                for file_details in details_list:
                    self.reroutelist.append(file_details)
                self.retrievedict[aggregation.tarname] = retrievelist

        # log and route the successful and non-successful catalog gets
        # we could split up using the code:
        # j=5 # size of each transfer
        # [x[i*j:i*j+j] for i in range(0,int(len(x)/j)+1)]
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin, RK.CATALOG_GET, RK.COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_GET {self.completelist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.CATALOG_GETTING,
            )
        # REROUTE
        if len(self.reroutelist) > 0:
            rk_reroute = ".".join([rk_origin, RK.CATALOG_GET, RK.REROUTE])
            # TODO (2024-03-14): This probably needs to be split, definitely
            # into cache-appropriate chunks of less than 500GB
            # Ensure the holding_id is present as we'll need it during retrieval
            body[MSG.META][
                MSG.HOLDING_ID
            ] = holding_id  # Ensure this is actually populated
            # Include the original files requested in the message body so they
            # can be moved to disk after retrieval
            body[MSG.DATA][MSG.RETRIEVAL_FILELIST] = self.retrievedict
            self.log(
                f"Rerouting PathList from CATALOG_GET to ARCHIVE_GET for "
                f"archive retrieval ({self.reroutelist})",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.reroutelist,
                pouting_key=rk_reroute,
                body_json=body,
                state=State.CATALOG_GETTING,
            )
        # FAILED
        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin, RK.CATALOG_GET, RK.FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_GET {self.failedlist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(self.failedlist, rk_failed, body, state=State.FAILED)

        # stop db transistions and commit
        self.catalog.save()
        self.catalog.end_session()

    def create_objectstore_location(
        self, tape_location: Location, tenancy: str
    ) -> PathDetails:
        # Need to query catalog for file and transaction info to
        # create the same object_store location that existed before
        # archive.
        file_ = self.catalog.get_location_file(tape_location)

        # Check if objectstore location already exists and skip if so
        if self.catalog.get_location(file_, Storage.OBJECT_STORAGE):
            return

        transaction = self.catalog.get_location_transaction(tape_location)
        access_time = tape_location.access_time.timestamp()
        # Create the object store location with the old bucket name
        objstr_location = self.catalog.create_location(
            file_=file_,
            storage_type=Storage.OBJECT_STORAGE,
            url_scheme="http",
            url_netloc=tenancy,
            root=transaction.transaction_id,
            path=file_.original_path,
            access_time=tape_location.access_time,
        )
        # Also need to make a path_details object for each of the
        # files within the same aggregation and add it to the
        # retrieve list.
        # NRM - TODO this needs changing to new structure - and don't munge the names, # urgh!
        tape_object_name = f"nlds.{tape_location.root}:{tape_location.path}"
        objstr_object_name = f"nlds.{transaction.transaction_id}:{tape_location.path}"
        path_details = PathDetails(
            original_path=file_.original_path,
            object_name=objstr_object_name,
            tape_url=tape_location.url_netloc,
            tape_path=tape_object_name,
            tenancy=tenancy,
            size=file_.size,
            user=file_.user,
            group=file_.group,
            permissions=file_.file_permissions,
            access_time=access_time,
            path_type=file_.path_type,
            link_path=file_.link_path,
        )
        return path_details

    def _catalog_archive_put(self, body: Dict, rk_origin: str) -> None:
        """Get the next holding for archiving, create a new location and
        aggregation for it and pass to for writing to tape."""
        # get the tape url
        tape_url = self._parse_tape_url(body)
        # start the database transactions
        self.catalog.start_session()

        # Get the next holding in the catalog, by id, which has any unarchived
        # Files, i.e. any files which don't have a tape location
        next_holding = self.catalog.get_next_holding()

        # If no holdings left to archive then end the callback
        if not next_holding:
            self.log("No holdings found to archive, exiting callback.", RK.LOG_INFO)
            self.catalog.end_session()
            return

        # We need a new root as we can't rely on the transaction_id any more.
        # Create a slug from the uneditable holding information, this will be
        # the directory on the tape that contains each of the tar files.
        holding_slug = f"{next_holding.id}.{next_holding.user}" f".{next_holding.group}"

        # Get the unarchived files and make suitable aggregates of them. Here we
        # make a distinction between aggregates, being just groups of files, and
        # aggregations, being the database object / table
        filelist = self.catalog.get_unarchived_files(next_holding)
        aggregates = aggregate_files(
            filelist, target_agg_size=self.target_aggregation_size
        )

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
                aggregation = self.catalog.create_aggregation(tarname=tar_filename)
            except CatalogError as e:
                # If for some reason we fail to make the aggregation then
                # continue and the remaining unarchived files will get picked up
                # on the next call to get_next_holding()
                self.log(e.message, RK.LOG_ERROR)
                continue
            # Now create the tape locations and assign them to the aggregation
            for f in aggregate:
                try:
                    # Get the object_store location so we can pass the object
                    # information easily to the archiver
                    objstr_location = self.catalog.get_location(
                        f, Storage.OBJECT_STORAGE
                    )
                    object_name = (
                        f"nlds.{objstr_location.root}:" f"{objstr_location.path}"
                    )
                    tape_path = f"nlds.{tape_root}:{f.original_path}"
                    # We need to pass the tenancy too, which could be different
                    # for each location
                    file_details = PathDetails(
                        original_path=f.original_path,
                        object_name=object_name,
                        tenancy=objstr_location.url_netloc,
                        tape_path=tape_path,
                        tape_url=tape_url,
                        size=f.size,
                        user=f.user,
                        group=f.group,
                        permissions=f.file_permissions,
                        access_time=objstr_location.access_time.timestamp(),
                        path_type=f.path_type,
                        link_path=f.link_path,
                    )
                    self.catalog.create_location(
                        file_=f,
                        storage_type=Storage.TAPE,
                        url_scheme="root",
                        url_netloc=tape_url,
                        root=tape_root,
                        path=f.original_path,
                        access_time=objstr_location.access_time,
                        aggregation=aggregation,
                    )
                except CatalogError as e:
                    # In the case of failure, we can just carry on adding things
                    # to the aggregation and then the next call to
                    # get_next_holding() should handle the rest.
                    self.log(e.message, RK.LOG_ERROR)
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
            rk_complete = ".".join([rk_origin, RK.CATALOG_ARCHIVE_NEXT, RK.COMPLETE])

            body[MSG.DETAILS][MSG.USER] = next_holding.user
            body[MSG.DETAILS][MSG.GROUP] = next_holding.group
            body[MSG.META][MSG.HOLDING_ID] = next_holding.id
            body[MSG.META][MSG.AGGREGATION_ID] = aggregation.id
            self.log(
                f"Sending completed PathList from CATALOG_ARCHIVE_PUT "
                f"{self.completelist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.CATALOG_ARCHIVE_AGGREGATING,
            )

        # stop db transactions and commit
        self.catalog.save()
        self.catalog.end_session()

    def _catalog_archive_update(
        self, body: Dict, rk_origin: str, rollback_fl: bool = False
    ) -> None:
        """Update the aggregation record following successful archive write to
        fill in the missing checksum information. Alternative mode is rollback,
        to be used on CATALOG_ARCHIVE_ROLLBACK, which simply marks the
        aggregation as failed upon a failed write attempt.
        """
        # Parse the message body for required variables
        try:
            filelist = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
            _, holding_id, _, _ = self._parse_metadata_vars(body)
            aggregation_id = self._parse_aggregation_id(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        try:
            checksum = body[MSG.DATA][MSG.CHECKSUM]
        except KeyError:
            if not rollback_fl:
                # Checksum required if we're finishing the put workflow
                self.log("Checksum not in message, exiting callback.", RK.LOG_ERROR)
                return
            else:
                # Checksum not required if we're just failing the agg
                self.log("Checksum not in message, continuing without.", RK.LOG_INFO)

        if holding_id is None and aggregation_id is None:
            self.log(
                "No method for identifying an aggregation provided, forced"
                " to exit callback.",
                RK.LOG_ERROR,
            )
            return

        try:
            new_tarname = body[MSG.DATA][MSG.NEW_TARNAME]
        except KeyError as e:
            self.log(
                "Optional parameter new_tarname not found, continuing " "without",
                RK.LOG_INFO,
            )
            new_tarname = None

        self.catalog.start_session()

        try:
            if aggregation_id is not None:
                aggregation = self.catalog.get_aggregation(aggregation_id)
            elif holding_id is not None:
                # get first file in list (that is all we should need) and turn
                # it into a PathDetails object
                f = filelist[0]
                file_details = PathDetails.from_dict(f)
                # Using that and the holding_id, we can get the
                files = self.catalog.get_files(
                    user,
                    group,
                    holding_id=holding_id,
                    original_path=file_details.original_path,
                )
                file_ = files[0]
                aggregation = self.catalog.get_aggregation_by_file(file_)
            if rollback_fl:
                self.catalog.fail_aggregation(aggregation)
            else:
                self.catalog.update_aggregation(
                    aggregation,
                    checksum=checksum,
                    algorithm="ADLER32",
                    tarname=new_tarname,
                )
        except CatalogError as e:
            self.log(
                f"Error encountered during _catalog_archive_update(): {e}", RK.LOG_ERROR
            )
            raise CallbackError("Encountered error during aggregation update.")

        # Only need to update the monitor after a successful tape-write
        if not rollback_fl:
            # Compile back into a list of PathDetails objects for finishing
            # things up
            self.completelist = [PathDetails.from_dict(f) for f in filelist]
            # Send confirmation on to monitor/worker
            rk_complete = ".".join([rk_origin, RK.CATALOG_ARCHIVE_UPDATE, RK.COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_ARCHIVE_UPDATE", RK.LOG_DEBUG
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.CATALOG_ARCHIVE_UPDATING,
            )

        # stop db transactions and commit
        self.catalog.save()
        self.catalog.end_session()

    def _catalog_del(self, body: Dict, rk_origin: str) -> None:
        """Remove a given list of files from the catalog if the transfer
        fails"""
        # Parse the message body for required variables
        try:
            filelist = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
            holding_label, holding_id, holding_tag, _ = self._parse_metadata_vars(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        # start the database transactions
        self.catalog.start_session()

        # get the holding from the database
        if holding_label is None and holding_id is None and holding_tag is None:
            self.log(
                "No method for identifying a holding or transaction "
                "provided, will continue without.",
                RK.LOG_WARNING,
            )
            # TODO: what happens in this event?

        for f in filelist:
            file_details = PathDetails.from_dict(f)
            try:
                # outsource deleting to the catalog itself
                self.catalog.delete_files(
                    user,
                    group,
                    holding_label=holding_label,
                    holding_id=holding_id,
                    path=file_details.original_path,
                    tag=holding_tag,
                )
            except CatalogError as e:
                file_details.failure_reason = e.message
                self.failedlist.append(file_details)
                self.log(e.message, RK.LOG_ERROR)
                continue

        # log the successful and non-successful catalog dels
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin, RK.CATALOG_DEL, RK.COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_DEL {self.completelist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.CATALOG_ROLLBACK,
            )
        # FAILED
        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin, RK.CATALOG_DEL, RK.FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_DEL {self.failedlist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body,
                state=State.FAILED,
            )

        # stop db transactions and commit
        self.catalog.save()
        self.catalog.end_session()

    def _catalog_location_del(
        self,
        body: Dict,
        rk_origin: str,
        location_type: Storage = Storage.OBJECT_STORAGE,
    ) -> None:
        """Remove a given list of locations from the catalog if the transfer,
        archive-put or archive-get fails."""
        # Parse the message body for required variables
        try:
            filelist = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
            (holding_label, holding_id, tag, transaction_id) = (
                self._parse_metadata_vars(body)
            )
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        # start the database transactions
        self.catalog.start_session()

        # get the holding from the database
        if holding_label is None and holding_id is None and tag is None:
            self.log(
                "No method for identifying a holding or transaction "
                "provided, will continue without.",
                RK.LOG_WARNING,
            )

        for f in filelist:
            file_details = PathDetails.from_dict(f)
            try:
                # get the files first
                files = self.catalog.get_files(
                    user,
                    group,
                    holding_label=holding_label,
                    holding_id=holding_id,
                    transaction_id=transaction_id,
                    original_path=file_details.original_path,
                    tag=tag,
                )
                if len(files) == 0:
                    raise CatalogError(
                        f"Could not find file(s) with original path "
                        f"{file_details.original_path}"
                    )
                # now get the location for the storage type requested so we can
                # delete it
                for file_ in files:
                    try:
                        self.catalog.delete_location(file_, location_type)
                    except CatalogError as e:
                        file_details.failure_reason = e.message
                        self.failedlist.append(file_details)
                        self.log(e.message, RK.LOG_ERROR)
                        continue
            except CatalogError as e:
                file_details.failure_reason = e.message
                self.failedlist.append(file_details)
                self.log(e.message, RK.LOG_ERROR)
                continue

        # log the successful and non-successful catalog puts
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin, RK.CATALOG_ARCHIVE_DEL, RK.COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_DEL {self.completelist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.CATALOG_ROLLBACK,
            )
        # FAILED
        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin, RK.CATALOG_ARCHIVE_DEL, RK.FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_DEL {self.failedlist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body,
                state=State.FAILED,
            )

        # stop db transactions and commit
        self.catalog.save()
        self.catalog.end_session()

    def _catalog_list(self, body: Dict, properties: Header) -> None:
        """List the users holdings"""
        # Parse the message body for required variables
        try:
            user, group = self._parse_user_vars(body)
            (holding_label, holding_id, tag, transaction_id) = (
                self._parse_metadata_vars(body)
            )
            groupall = self._parse_groupall(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        self.catalog.start_session()

        # holding_label and holding_id is None means that more than one
        # holding wil be returned
        try:
            holdings = self.catalog.get_holding(
                user,
                group,
                groupall=groupall,
                label=holding_label,
                holding_id=holding_id,
                transaction_id=transaction_id,
                tag=tag,
            )
        except CatalogError as e:
            # failed to get the holdings - send a return message saying so
            self.log(e.message, RK.LOG_ERROR)
            body[MSG.DETAILS][MSG.FAILURE] = e.message
            body[MSG.DATA][MSG.HOLDING_LIST] = []
        else:
            # fill the dictionary to generate JSON for the response
            ret_list = []
            for h in holdings:
                # get the first transaction
                if len(h.transactions) > 0:
                    t = h.transactions[0]
                    date_str = format_datetime(t.ingest_time)
                else:
                    date_str = ""
                ret_dict = {
                    "id": h.id,
                    "label": h.label,
                    "user": h.user,
                    "group": h.group,
                    "tags": h.get_tags(),
                    "transactions": h.get_transaction_ids(),
                    "date": date_str,
                }
                ret_list.append(ret_dict)
            # add the return list to successfully completed holding listings
            body[MSG.DATA][MSG.HOLDING_LIST] = ret_list
            self.log(f"Listing holdings from CATALOG_LIST {ret_list}", RK.LOG_DEBUG)
        self.catalog.end_session()

        # send the rpc return message for failed or success
        self.publish_message(
            properties.reply_to,
            msg_dict=body,
            exchange={"name": ""},
            correlation_id=properties.correlation_id,
        )

    def _catalog_stat(self, body: Dict, properties: Header) -> None:
        """Get the labels for a list of transaction ids"""
        # Parse the message body for required variables
        try:
            user, group = self._parse_user_vars(body)
            transaction_id = self._parse_transaction_id(body)
            label, _, _, _ = self._parse_metadata_vars(body)
            transaction_records = self._parse_transaction_records(body)
            groupall = self._parse_groupall(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        self.catalog.start_session()

        # Get transactions from catalog using transaction_ids from monitoring
        ret_dict = {}
        try:
            # Get the transaction and holding for each transaction_record
            for tr in transaction_records:
                transaction_id = tr["transaction_id"]
                t = self.catalog.get_transaction(transaction_id=transaction_id)
                # A transaction_id might not have an associated transaction in
                # the catalog if the transaction FAILED or has not COMPLETED
                # yet.  We allow for this and return an empty string instead.
                if t is None:
                    label = ""
                else:
                    h = self.catalog.get_holding(
                        user, group, groupall=groupall, holding_id=t.holding_id
                    )[
                        0
                    ]  # should only be one!
                    label = h.label
                    ret_dict[t.transaction_id] = label

                # Add label to the transaction_record dict
                tr[MSG.LABEL] = label
        except CatalogError as e:
            # failed to get the transactions - send a return message saying so
            self.log(e.message, RK.LOG_ERROR)
            body[MSG.DETAILS][MSG.FAILURE] = e.message
            body[MSG.DATA][MSG.TRANSACTIONS] = {}
        else:
            # add the return list to successfully completed holding listings
            body[MSG.DATA][MSG.TRANSACTIONS] = ret_dict
            body[MSG.DATA][MSG.RECORD_LIST] = transaction_records
            self.log(
                f"Getting holding labels from CATALOG_STAT {ret_dict}", RK.LOG_DEBUG
            )
        self.catalog.end_session()

        # send the rpc return message for failed or success
        self.publish_message(
            properties.reply_to,
            msg_dict=body,
            exchange={"name": ""},
            correlation_id=properties.correlation_id,
        )

    def _catalog_find(self, body: Dict, properties: Header) -> None:
        """List the user's files"""
        # Parse the message body for required variables
        try:
            user, group = self._parse_user_vars(body)
            transaction_id = self._parse_transaction_id(body)
            (holding_label, holding_id, tag, transaction_id) = (
                self._parse_metadata_vars(body)
            )
            groupall = self._parse_groupall(body)
            path = self._parse_path(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            raise Exception

        self.catalog.start_session()
        ret_dict = {}
        try:
            files = self.catalog.get_files(
                user,
                group,
                groupall=groupall,
                holding_label=holding_label,
                holding_id=holding_id,
                transaction_id=transaction_id,
                original_path=path,
                tag=tag,
            )
            for f in files:
                # get the transaction and the holding:
                t = self.catalog.get_transaction(
                    id=f.transaction_id
                )  # should only be one!
                h = self.catalog.get_holding(
                    user, group, groupall=groupall, holding_id=t.holding_id
                )[
                    0
                ]  # should only be one!
                # create a holding dictionary if it doesn't exists
                if h.label in ret_dict:
                    h_rec = ret_dict[h.label]
                else:
                    h_rec = {
                        MSG.TRANSACTIONS: {},
                        MSG.LABEL: h.label,
                        MSG.HOLDING_ID: h.id,
                        MSG.USER: h.user,
                        MSG.GROUP: h.group,
                    }
                    ret_dict[h.label] = h_rec
                # create a transaction dictionary if it doesn't exist
                if t.transaction_id in ret_dict[h.label][MSG.TRANSACTIONS]:
                    t_rec = ret_dict[h.label][MSG.TRANSACTIONS][t.transaction_id]
                else:
                    t_rec = {
                        MSG.FILELIST: [],
                        MSG.TRANSACT_ID: t.transaction_id,
                        "ingest_time": format_datetime(t.ingest_time)
                    }
                    ret_dict[h.label][MSG.TRANSACTIONS][t.transaction_id] = t_rec
                # get the locations
                locations = []
                for l in f.locations:
                    # build the url for object storage
                    if l.storage_type == Storage.OBJECT_STORAGE:
                        url = urlunsplit(
                            (
                                l.url_scheme,
                                l.url_netloc,
                                f"nlds.{l.root}/{l.path}",
                                "",
                                "",
                            )
                        )
                    else:
                        url = None

                    l_rec = {
                        "storage_type": l.storage_type,
                        "root": l.root,
                        "path": l.path,
                        "access_time": format_datetime(l.access_time),
                        "url": url,
                    }
                    locations.append(l_rec)
                # build the file record
                f_rec = {
                    "original_path": f.original_path,
                    "path_type": str(f.path_type),
                    "link_path": f.link_path,
                    "size": f.size,
                    MSG.USER: f.user,
                    MSG.GROUP: f.group,
                    "permissions": f.file_permissions,
                    "locations": locations,
                }
                t_rec[MSG.FILELIST].append(f_rec)

        except CatalogError as e:
            # failed to get the holdings - send a return message saying so
            self.log(e.message, RK.LOG_ERROR)
            body[MSG.DETAILS][MSG.FAILURE] = e.message
            body[MSG.DATA][MSG.HOLDING_LIST] = []
        else:
            # add the return list to successfully completed holding listings
            body[MSG.DATA][MSG.HOLDING_LIST] = ret_dict
            self.log(f"Listing files from CATALOG_FIND {ret_dict}", RK.LOG_DEBUG)

        self.catalog.end_session()

        self.publish_message(
            properties.reply_to,
            msg_dict=body,
            exchange={"name": ""},
            correlation_id=properties.correlation_id,
        )

    def _catalog_meta(self, body: Dict, properties: Header) -> None:
        """Change metadata for a user's holding"""
        # Parse the message body for required variables
        try:
            user, group = self._parse_user_vars(body)
            holding_label, holding_id, tag, _ = self._parse_metadata_vars(body)
            new_label, new_tag, del_tag = self._parse_new_metadata_variables(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        self.catalog.start_session()

        # if there is the holding label or holding id then get the holding
        try:
            if not holding_label and not holding_id and not tag:
                raise CatalogError(
                    "Holding not found: holding_id or label or tag(s) not specified."
                )
            holdings = self.catalog.get_holding(
                user, group, label=holding_label, holding_id=holding_id, tag=tag
            )
            old_meta_list = []
            ret_list = []
            for holding in holdings:
                # get the old metadata so we can record it, then modify
                old_meta_list.append(
                    {"label": holding.label, "tags": holding.get_tags()}
                )
                self.catalog.modify_holding(holding, new_label, new_tag, del_tag)
            self.catalog.save()

            for holding, old_meta in zip(holdings, old_meta_list):
                # record the new metadata
                new_meta = {"label": holding.label, "tags": holding.get_tags()}

                # build the return dictionary and append it to the list of
                # holdings that have been modified
                ret_dict = {
                    "id": holding.id,
                    "user": holding.user,
                    "group": holding.group,
                    "old_meta": old_meta,
                    "new_meta": new_meta,
                }
                ret_list.append(ret_dict)

        except CatalogError as e:
            # failed to get the holdings - send a return message saying so
            self.log(e.message, RK.LOG_ERROR)
            body[MSG.DETAILS][MSG.FAILURE] = e.message
            body[MSG.DATA][MSG.HOLDING_LIST] = []
        else:
            # fill the return message with a dictionary of the holding(s)
            body[MSG.DATA][MSG.HOLDING_LIST] = ret_list
            self.log(f"Modified metadata from CATALOG_META {ret_list}", RK.LOG_DEBUG)

        self.catalog.end_session()

        # return message to complete RPC
        self.publish_message(
            properties.reply_to,
            msg_dict=body,
            exchange={"name": ""},
            correlation_id=properties.correlation_id,
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
                self.log(f"db_connect string is {db_connect}", RK.LOG_DEBUG)
        except DBError as e:
            self.log(e.message, RK.LOG_CRITICAL)

    def get_engine(self):
        # Method for making the db_engine available to alembic
        return self.database.db_engine

    def get_url(self):
        """Method for making the sqlalchemy url available to alembic"""
        # Create a minimum version of the catalog to put together a url
        if self.catalog is None:
            db_engine = self.load_config_value(self._DB_ENGINE)
            db_options = self.load_config_value(self._DB_OPTIONS)
            self.catalog = Catalog(db_engine, db_options)
        return self.catalog.get_db_string()

    def callback(
        self,
        ch: Channel,
        method: Method,
        properties: Header,
        body: bytes,
        connection: Connection,
    ) -> None:
        # Reset member variables
        self.reset()

        # Connect to database if not connected yet
        # Convert body from bytes to json for ease of manipulation
        body = json.loads(body)

        # Get the API method and decide what to do with it
        try:
            api_method = body[MSG.DETAILS][MSG.API_ACTION]
        except KeyError:
            self.log(
                f"Message did not contain an appropriate API method, "
                "exiting callback",
                RK.LOG_ERROR,
            )
            return

        # Check for system status
        if self._is_system_status_check(body_json=body, properties=properties):
            return

        # Only print the message contents when we're not statting, the message
        # can get very long.
        if not api_method == RK.STAT:
            self.log(
                f"Received {json.dumps(body, indent=4)} from "
                f"{self.queues[0].name} ({method.routing_key})",
                RK.LOG_DEBUG,
            )

        self.log(
            f"Appending rerouting information to message: "
            f"{self.DEFAULT_REROUTING_INFO} ",
            RK.LOG_DEBUG,
        )
        body = self.append_route_info(body)

        # check whether this is a GET or a PUT
        if (api_method == RK.GETLIST) or (api_method == RK.GET):
            # split the routing key
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
                self.log(
                    "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
                )
                return
            if rk_parts[1] == RK.CATALOG_GET:
                self.log(f"Running catalog get workflow", RK.LOG_INFO)
                self._catalog_get(body, rk_parts[0])
            elif rk_parts[1] == RK.CATALOG_ARCHIVE_DEL:
                # If part of a GET transaction but received via the del topic
                # then delete the previously added object storage Locations
                self.log(
                    f"Deleting objectstore Locations as part of a failed "
                    f"archive-get workflow",
                    RK.LOG_INFO,
                )
                self._catalog_location_del(
                    body, rk_parts[0], location_type=Storage.OBJECT_STORAGE
                )

        elif (api_method == RK.PUTLIST) or (api_method == RK.PUT):
            # split the routing key
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
                self.log(
                    "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
                )
                return
            if rk_parts[2] == RK.START:
                # Check the routing key worker section to determine which method
                # to call, as a del could be being called from a failed
                # transfer_put
                if rk_parts[1] == RK.CATALOG_PUT:
                    self._catalog_put(body, rk_parts[0])
                elif rk_parts[1] == RK.CATALOG_DEL:
                    self._catalog_del(body, rk_parts[0])
                elif rk_parts[1] == RK.CATALOG_AMEND:
                    self._catalog_amend(body, rk_parts[0])

        # Archive put requires getting from the catalog
        elif api_method == RK.ARCHIVE_PUT:
            self.log("Starting an archive-put workflow", RK.LOG_DEBUG)
            # split the routing key
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
                self.log(
                    "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
                )

            if rk_parts[1] == RK.CATALOG_ARCHIVE_NEXT:
                self.log(
                    "Beginning preparation of next archive aggregation", RK.LOG_DEBUG
                )
                self._catalog_archive_put(body, rk_parts[0])
            elif rk_parts[1] == RK.CATALOG_ARCHIVE_UPDATE:
                # NOTE: retries and failures for this method are handled by TLR
                self._catalog_archive_update(body, rk_parts[0])
            elif rk_parts[1] == RK.CATALOG_ARCHIVE_DEL:
                self._catalog_archive_update(body, rk_parts[0], rollback_fl=True)

        elif api_method == RK.LIST:
            # don't need to split any routing key for an RPC method
            self._catalog_list(body, properties)

        elif api_method == RK.FIND:
            # don't need to split any routing key for an RPC method
            self._catalog_find(body, properties)

        elif api_method == RK.META:
            # don't need to split any routing key for an RPC method
            self._catalog_meta(body, properties)

        elif api_method == RK.STAT:
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
