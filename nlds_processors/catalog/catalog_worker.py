# encoding: utf-8
"""
catalog_worker.py
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
<<<<<<< HEAD

from urllib.parse import urlunsplit
=======
from datetime import datetime
>>>>>>> main

# Typing imports
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

from nlds.rabbit.consumer import RabbitMQConsumer as RMQC
from nlds.rabbit.consumer import State
from nlds.errors import CallbackError

<<<<<<< HEAD
from nlds_processors.catalog.catalog import Catalog, CatalogError
from nlds_processors.catalog.catalog_models import Storage, Location, File
from nlds.details import PathDetails, PathType
from nlds_processors.db_mixin import DBError

from nlds.authenticators.jasmin_authenticator import JasminAuthenticator as Authenticator

=======
from nlds_processors.catalog.catalog import Catalog
from nlds_processors.catalog.catalog_error import CatalogError
from nlds_processors.catalog.catalog_models import Storage, File
from nlds.details import PathDetails
from nlds_processors.db_mixin import DBError

import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG
>>>>>>> main


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


def build_retrieval_dict(filelist: List[PathDetails], fullpath: bool = False):
    """Build a retrieval dict from the filelist.  The retrieval dict contains a
    tarfile name, a holding id, and the list of files to retrieve from the tarfile.
    """
    retrieval_dict = {}
    for file in filelist:
        # get the tape location for File or FileSystem
        tarfile = file.tape_name
        # if it has not been added before then create a new record
        if not tarfile in retrieval_dict:
            retrieval_dict[tarfile] = {
                "holding_id": file.holding_id,
                "filelist": [file],
            }
        else:
            # if it has been added before then append to the filelist
            retrieval_dict[tarfile]["filelist"].append(file)
    return retrieval_dict


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
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.default_tape_url = self.load_config_value(self._DEFAULT_TAPE_URL)
        self.default_tenancy = self.load_config_value(self._DEFAULT_TENANCY)

        self.catalog = None
<<<<<<< HEAD
        self.reroutelist = []
        self.retrievedict = {}
        self.authenticator = Authenticator()

=======
        self.tapelist = []
>>>>>>> main

    @property
    def database(self):
        return self.catalog

    def reset(self):
        super().reset()

        self.completelist.clear()
        self.failedlist.clear()
        # New list for rerouting to tape archive if not found on object store
        self.tapelist.clear()

    def _parse_filelist(self, body: Dict) -> list[str]:
        # get the filelist from the data section of the message
        try:
            filelist = body[MSG.DATA][MSG.FILELIST]
        except KeyError as e:
            self.log(
                f"Invalid message contents, filelist should be in the data section of "
                f"the message body.",
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
        if (
            MSG.TENANCY in body[MSG.DETAILS]
            and body[MSG.DETAILS][MSG.TENANCY] is not None
        ):
            tenancy = body[MSG.DETAILS][MSG.TENANCY]
        else:
            tenancy = self.default_tenancy
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
        if (
            MSG.TAPE_URL in body[MSG.DETAILS]
            and body[MSG.DETAILS][MSG.TAPE_URL] is not None
        ):
            tape_url = body[MSG.DETAILS][MSG.TAPE_URL]
        else:
            tape_url = self.default_tape_url
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
        # get the path from the metadata section of the message
        try:
            path = body[MSG.META][MSG.PATH]
        except KeyError:
            path = None
        return path

    def _parse_regex(self, body: dict) -> str:
        # get the REGEX flag from the metadata section of the message
        try:
            regex = body[MSG.META][MSG.REGEX]
        except KeyError:
            regex = False
        return regex

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
            search_label = None  # match nothing if no label or holding id (was $<)
            # this will produce a new holding
        return search_label

    def _get_or_create_holding(
        self, user, group, search_label, holding_id, transaction_id, new_label
    ):
        """Get a holding via label or holding_id or transaction_id.
        If the holding doesn't already exist then create it."""
        # try to get the holding to see if it already exists and can be added to
        try:
            # don't use tags to search - they are strictly for adding to the holding
            holding = self.catalog.get_holding(
                user,
                group,
                label=search_label,
                holding_id=holding_id,
                transaction_id=transaction_id,
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
                raise CatalogError(message)
            try:
                # does the holding exist with the new_label?  This might happen if
                # the transaction is interrupted
                try:
                    holding = self.catalog.get_holding(user, group, label=new_label)
                except (KeyError, CatalogError):
                    holding = self.catalog.create_holding(user, group, new_label)
                else:
                    if len(holding) > 1:
                        raise CatalogError(
                            f"More than one holding found for label {new_label}"
                        )
                    else:
                        holding = holding[0]
            except CatalogError as e:
                self.log(e.message, RK.LOG_ERROR)
                raise e
        else:
            if len(holding) > 1:
                raise CatalogError(f"More than one holding found for label {new_label}")
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

        # blank the tag warnings
        tag_warnings = None
        # get the (regex) search label
        search_label = self._get_search_label(label, holding_id)

        # get, or create the holding
        try:
            holding = self._get_or_create_holding(
                user,
                group,
                search_label=search_label,
                holding_id=holding_id,
                transaction_id=transaction_id,
                new_label=new_label,
            )
        except CatalogError as e:
            # could not find holding so mark all files as failed and return
            holding = None
            for f in filelist:
                pd = PathDetails.from_dict(f)
                pd.failure_reason = e.message
                self.failedlist.append(pd)

        if holding:
            try:
                # get or create the transaction
                transaction = self._get_or_create_transaction(transaction_id, holding)
            except CatalogError as e:
                transaction = None
                for f in filelist:
                    pd = PathDetails.from_dict(f)
                    pd.failure_reason = e.message
                    self.failedlist.append(pd)

        if holding and transaction:
            # loop over the filelist
            for f in filelist:
                # convert to PathDetails class
                pd = PathDetails.from_dict(f)
                # add the holding id to the PathDetails
                pd.holding_id = holding.id
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

        # remove empty transactions
        try:
            if len(transaction.files) == 0:
                self.catalog.delete_transaction(transaction_id)
        except CatalogError as e:
            self.log(e.message, RMQC.RK_LOG_ERROR)
            return

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

    def _catalog_update(self, body: Dict, rk_origin: str, create: bool) -> None:
        """Upon completion of a TRANSFER_PUT, the list of completed files is returned
        back to the NLDS worker, but with location on Object Storage of the files
        appended to each PathDetails JSON object.
        The NLDS worker then passes this message to the Catalog, and this function
        processes the PathDetails and updates each file's record in the database to
        contain the Object Storage location.

        Upon completion of an ARCHIVE_GET, the list of completed files is returned
        back to the NLDS worker.  The information for each completed file is enough to  amend the OBJECT STORAGE location in the catalog database.
        """
        # Parse the message body for required variables
        try:
            filelist = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
        except CatalogError as e:
            # functions above handled message logging, here we just return
            raise e

        self.catalog.start_session()
        self.reset()

        # loop over the filelist
        for f in filelist:
            # convert to PathDetails class
            pd = PathDetails.from_dict(f)
            # need to
            #   1. find the file,
            #   2. find or create the object storage location (if create==True),
            #   3. add the details to the object storage location from the PathDetails
            # get the file
            try:
                pl = pd.get_object_store()  # this returns a PathLocation object
                if pl is None:
                    raise CatalogError(
                        f"No object store location in PathDetails for file "
                        f"{pd.original_path}"
                    )
                file = self.catalog.get_files(
                    user,
                    group,
                    holding_id=pd.holding_id,
                    original_path=pd.original_path,
                )[0]
                # access time is now if None
                if pl.access_time is None:
                    access_time = datetime.now()
                else:
                    access_time = datetime.fromtimestamp(pl.access_time)
                st = Storage.from_str(pl.storage_type)
                # get the location
                location = self.catalog.get_location(file, st)
                if location:
                    # check empty
                    if location.url_scheme != "" or location.url_netloc != "":
                        raise CatalogError(
                            f"{pl.storage_type} for file {pd.original_path} will be"
                            f" overwritten, the Storage Location should be empty."
                        )
                    # otherwise update if exists and not empty
                    location.url_scheme = pl.url_scheme
                    location.url_netloc = pl.url_netloc
                    location.root = pl.root
                    location.path = pl.path
                    location.access_time = access_time
                    self.completelist.append(pd)
                elif create:
                    # create location
                    location = self.catalog.create_location(
                        file,
                        storage_type=st,
                        url_scheme=pl.url_scheme,
                        url_netloc=pl.url_netloc,
                        root=pl.root,
                        path=pl.path,
                        access_time=access_time,
                    )
                    self.completelist.append(pd)
                else:
                    raise CatalogError(
                        f"{pl.storage_type} for file {pd.original_path} can not be "
                        "found."
                    )
            except CatalogError as e:
                # the file wasn't found or the location couldn't be created
                pd.failure_reason = e.message
                self.failedlist.append(pd)
                self.log(e.message, RK.LOG_ERROR)

        # stop db transitions and commit
        self.catalog.save()
        self.catalog.end_session()

        # log the successful and non-successful catalog updates
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin, RK.CATALOG_UPDATE, RK.COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_UPDATE {self.completelist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.CATALOG_UPDATING,
            )
        # FAILED
        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin, RK.CATALOG_UPDATE, RK.FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_UPDATE {self.failedlist}",
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
        found on tape then it will be first restored by the archive processor
        for retrieval to object store cache."""
        # Parse the message body for required variables
        try:
            filelist = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
            tenancy = self._parse_tenancy(body)
            holding_label, holding_id, holding_tag, transaction_id = (
                self._parse_metadata_vars(body)
            )
            groupall = self._parse_groupall(body)
            regex = self._parse_regex(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        # reset the lists
        self.reset()

        # start the database transactions, reset lists and
        self.catalog.start_session()

        for filepath in filelist:
            filepath_details = PathDetails.from_dict(filepath)
            try:
                # get the files first
                files = self.catalog.get_files(
                    user,
                    group,
                    groupall=groupall,
                    holding_label=holding_label,
                    holding_id=holding_id,
                    transaction_id=transaction_id,
                    original_path=filepath_details.original_path,
                    tag=holding_tag,
                    newest_only=True,
                    regex=regex,
                )

                if len(files) == 0:
                    raise CatalogError(
                        f"Could not find file(s) with original path: "
                        f"{filepath_details.original_path}"
                    )
                # If the filepath was a regex then more than one file will be returned
                for file in files:
                    # determine the storage location - None, OBJECT_STORAGE and/or TAPE
                    pd = self._filemodel_to_path_details(file)
                    if pd.locations.count == 0:
                        # empty storage location denotes that it is still in its initial
                        # transfer to OBJECT STORAGE
                        reason = (
                            f"No Storage Location found for file with original path: "
                            f"{pd.original_path}.  Has it completed transfer?"
                        )
                        raise CatalogError(reason)

                    elif pd.locations.has_storage_type(MSG.OBJECT_STORAGE):
                        # empty OBJECT_STORAGE denotes that it is restoring from tape
                        # we want to only fetch things from tape once.
                        if pd.get_object_store().url_scheme == "":
                            reason = (
                                "File is already transferring from tape to Object "
                                "Storage."
                            )
                            raise CatalogError(reason)
                        else:
                            self.completelist.append(pd)

                    elif pd.locations.has_storage_type(MSG.TAPE):
                        # get the aggregation
                        pl = pd.get_tape()
                        agg = self.catalog.get_aggregation(pl.aggregation_id)
                        tr = self.catalog.get_transaction(file.transaction_id)
                        if pl.access_time is None:
                            access_time = datetime.now()
                        else:
                            access_time = datetime.fromtimestamp(pl.access_time)

                        # create a mostly empty OBJECT STORAGE location in the database
                        # as a marker that the file is currently transferring
                        self.catalog.create_location(
                            file_=file,
                            storage_type=Storage.OBJECT_STORAGE,
                            url_scheme="",
                            url_netloc="",
                            root="",
                            path=file.original_path,
                            access_time=access_time,
                            aggregation=None,
                        )

                        # create the OBJECT STORAGE Path Location for the message (not
                        # the database)
                        pd.set_object_store(tenancy=tenancy, bucket=tr.transaction_id)
                        self.tapelist.append(pd)
                    else:
                        # this shouldn't occur but we'll trap the error anyway
                        reason = (
                            f"No compatible Storage Location found for file with "
                            f"original path: {pd.original_path}."
                        )
                        raise CatalogError(reason)

            except CatalogError as e:
                filepath_details.failure_reason = e.message
                self.failedlist.append(filepath_details)
                self.log(e.message, RK.LOG_ERROR)

        self.catalog.save()
        self.catalog.end_session()

        # COMPLETED
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

        # NEED RETRIEVING FROM TAPE
        if len(self.tapelist) > 0:
            rk_restore = ".".join([rk_origin, RK.ROUTE, RK.ARCHIVE_RESTORE])
            self.log(
                f"Rerouting PathList from CATALOG_GET to ARCHIVE_GET for archive "
                f"retrieval ({self.tapelist})",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.tapelist,
                routing_key=rk_restore,
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
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body,
                state=State.FAILED,
            )

    def _filemodel_to_path_details(self, file: File):
        pd = PathDetails.from_filemodel(file)
        t = self.catalog.get_transaction(id=file.transaction_id)
        pd.holding_id = t.holding_id
        return pd

    def _catalog_archive_put(self, body: Dict, rk_origin: str) -> None:
        """Get the next holding for archiving, create a new location for it and pass it
        for aggregating to the Archive Put process."""
        try:
            tenancy = self._parse_tenancy(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return
        # start the database transactions
        self.catalog.start_session()

        # Get the next holding in the catalog, by id, which has any unarchived
        # Files, i.e. any files which don't have a tape location
        next_holding = self.catalog.get_next_unarchived_holding(tenancy)

        # If no holdings left to archive then end the callback
        if not next_holding:
            self.log("No holdings found to archive, exiting callback.", RK.LOG_INFO)
            self.catalog.end_session()
            return

        # get the list of unarchived files from that holding
        filelist = self.catalog.get_unarchived_files(next_holding)
        # loop over the files and modify the database to have a TAPE storage location
        self.reset()
        for f in filelist:
            pd = self._filemodel_to_path_details(f)
            pl = pd.get_object_store()  # this returns a PathLocation object
            # get the access time of the object store to mirror to tape, or set to now
            # if no access_time present
            if pl.access_time is None:
                access_time = datetime.now()
            else:
                access_time = datetime.fromtimestamp(pl.access_time)

            try:
                # create a mostly empty TAPE storage location
                self.catalog.create_location(
                    file_=f,
                    storage_type=Storage.TAPE,
                    url_scheme="",
                    url_netloc="",
                    root="",
                    path=f.original_path,
                    access_time=access_time,
                    aggregation=None,
                )
                # update the pd now with new location
                pd = self._filemodel_to_path_details(f)
                # add to the completelist ready for sending
                self.completelist.append(pd)
            except CatalogError as e:
                # In the case of failure, we can just carry on adding files to the
                # message
                self.log(e.message, RK.LOG_ERROR)
                # Keep note of the failure (we're not sending it anywhere currently)
                self.failedlist.append(pd)
                continue

        # Forward successful file details to archive for tape write
        rk_complete = ".".join([rk_origin, RK.CATALOG_ARCHIVE_NEXT, RK.COMPLETE])

        # the user and group need to be set here
        body[MSG.DETAILS][MSG.USER] = next_holding.user
        body[MSG.DETAILS][MSG.GROUP] = next_holding.group
        body[MSG.META][MSG.HOLDING_ID] = next_holding.id
        if len(self.completelist) > 0:
            self.log(
                f"Sending completed PathList from CATALOG_ARCHIVE_PUT "
                f"{self.completelist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.ARCHIVE_INIT,
            )

        # stop db transactions and commit
        self.catalog.save()
        self.catalog.end_session()

    def _catalog_archive_update(
        self, body: Dict, rk_origin: str, storage_type: Storage
    ) -> None:
        """Update the aggregation record following successful archive write to fill in
        the missing checksum information.
        """
        # Parse the message body for required variables
        try:
            filelist = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
            _, holding_id, _, _ = self._parse_metadata_vars(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        try:
            tarfile_name = body[MSG.DATA][MSG.TARFILE]
        except KeyError:
            msg = "Tarfile name not in message and is required."
            self.log(msg, RK.LOG_ERROR)
            raise CatalogError(msg)

        try:
            checksum = body[MSG.DATA][MSG.CHECKSUM]
        except KeyError:
            msg = "Checksum not in message and is required."
            self.log(msg, RK.LOG_ERROR)
            raise CatalogError(msg)

        if holding_id is None:
            msg = "Holding id not in message and is required"
            self.log(msg, RK.LOG_ERROR)
            raise CatalogError(msg)

        self.catalog.start_session()
        self.completelist.clear()
        self.failedlist.clear()

        try:
            # the only route in now is to create an aggregation at this point
            aggregation = self.catalog.create_aggregation(
                tarname=tarfile_name, checksum=checksum, algorithm="ADLER32"
            )
        except CatalogError as e:
            msg = f"Could not create aggregation in _catalog_archive_update: {e}"
            self.log(msg, RK.LOG_ERROR)
            raise CallbackError(msg)

        # holding_id is not None, as confirmed by above check
        for f in filelist:
            pd = PathDetails.from_dict(f)
            pl = pd.get_tape()
            # get the file
            try:
                # recreate the path location if it was deleted
                if pl is None:
                    raise CatalogError(
                        f"No tape location in PathDetails for file {pd.original_path}"
                    )
                # just one file
                file = self.catalog.get_files(
                    user, group, holding_id=holding_id, original_path=pd.original_path
                )[0]
                # get the already created location and update with info from
                # PathLocation and assign the aggregation
                location = self.catalog.get_location(file, storage_type)
                location.url_scheme = pl.url_scheme
                location.url_netloc = pl.url_netloc
                location.root = pl.root
                location.path = pl.path
                location.aggregation_id = aggregation.id
                self.completelist.append(pd)

            except CatalogError as e:
                # the file wasn't found or the location couldn't be created
                pd.failure_reason = e.message
                self.failedlist.append(pd)
                self.log(e.message, RK.LOG_ERROR)
                continue

        # stop db transactions and commit
        self.catalog.save()
        self.catalog.end_session()

        if len(self.completelist) > 0:
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

        if len(self.failedlist) > 0:
            rk_failed = ".".join([rk_origin, RK.CATALOG_ARCHIVE_UPDATE, RK.FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_ARCHIVE_UPDATE "
                f"{self.failedlist}",
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body,
                state=State.FAILED,
            )

    def _catalog_remove(
        self, body: Dict, rk_origin: str, storage_type: Storage
    ) -> None:
        """Remove an empty storage_type storage Location if archive_put has failed."""
        # routing keys
        rk_complete = ".".join([rk_origin, RK.CATALOG_REMOVE, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.CATALOG_REMOVE, RK.FAILED])

        try:
            filelist_ = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
            _, holding_id, _, _ = self._parse_metadata_vars(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        if holding_id is None and filelist_ is None:
            self.log(
                "No method for identifying a filelist provided, exit callback.",
                RK.LOG_ERROR,
            )
            return

        self.reset()
        self.catalog.start_session()

        # convert the JSON filelist into a list of PathDetails
        filelist = [PathDetails.from_dict(f) for f in filelist_]

        try:
            holding = self.catalog.get_holding(user, group, holding_id=holding_id)
        except CatalogError as e:
            for f in filelist:
                f.failure_reason = e.message
                self.failedlist.append(f)
        else:
            for f in filelist:
                try:
                    file = self.catalog.get_files(
                        user,
                        group,
                        holding_id=holding_id,
                        original_path=f.original_path,
                    )[0]
                    loc = self.catalog.get_location(file, storage_type)
                    # delete location if all details are empty
                    if loc is not None and loc.storage_type == storage_type:
                        if (
                            loc.url_scheme == ""
                            and loc.url_netloc == ""
                            and loc.root == ""
                        ):
                            self.catalog.delete_location(file, storage_type)
                            self.completelist.append(f)
                        else:
                            f.failure_reason = (
                                f"{str(storage_type.name)} location not empty details"
                            )
                            self.failedlist.append(f)

                except (CatalogError, IndexError) as e:
                    f.failure_reason = e.message
                    self.failedlist.append(f)

        if len(self.completelist) > 0:
            self.log(
                (
                    f"Sending completed PathList from CATALOG_REMOVE "
                    f"{self.completelist}"
                ),
                RK.LOG_DEBUG,
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.CATALOG_REMOVING,
            )

        if len(self.failedlist) > 0:
            self.log(
                (f"Sending failed PathList from CATALOG_REMOVE " f"{self.failedlist}"),
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

<<<<<<< HEAD
    def __File_to_PathDetails(self, file):
        """Transfer details from a (Catalog) File object to a PathDetails object
        """
        # load the details in from the file object
        new_details = PathDetails(
            original_path = file.original_path,
            size = file.size,
            user = file.user,
            group = file.group,
            permissions = file.file_permissions,
            path_type = file.path_type,
            link_path = file.link_path,
        )

        for l in file.locations:
            if l.storage_type == Storage.OBJECT_STORAGE:
                new_details.object_name = f"nlds.{l.root}:{l.path}"
                new_details.tenancy = l.url_netloc
                new_details.access_time = l.access_time.timestamp()
            elif l.storage_type == Storage.TAPE:
                new_details.tape_url = l.url_netloc
                new_details.tape_path = f"nlds.{l.root}:{l.path}"
        return new_details
    

    def _catalog_restore(self, body: Dict, rk_origin: str) -> None:
        """Restore entries into the catalog that have been deleted in the intitial
        transfer delete phase."""
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

        # get the transaction id from the details section of the message. This is
        # needed for creating a transaction if it doesn't exist during the 
        # reconstruction
        try:
            new_transaction_id = body[self.MSG_DETAILS][self.MSG_TRANSACT_ID]
        except KeyError:
            new_transaction_id = None

        rk_complete = ".".join([rk_origin,
                                self.RK_CATALOG_RESTORE, 
                                self.RK_COMPLETE])
        self.reset()
        self.catalog.start_session()

        # get the previously created holding - if it has already been deleted
        # then we need to recreate them from the details in the message
        try:
            holding = self.catalog.get_holding(
                user, group, holding_id=holding_id, label=holding_label
            )[0]        # should only be one
        except (KeyError, CatalogError):
            holding = None

        if holding is None:
            if holding_label is None:
                holding_label = new_transaction_id[0:8]
            try:
                holding = self.catalog.create_holding(user, group, holding_label)
            except CatalogError:    # holding somehow already exists
                self.send_pathlist([], rk_complete, body, mode="failed")
                return
            
        # get the previously create transaction - if it has already been deleted
        # then we need to recreate them from the details in the message
        try:
            transaction = self.catalog.get_transaction(
                transaction_id=transaction_id
            )
        except (KeyError, CatalogError):
            transaction = None
        if transaction is None:
            try:
                transaction = self.catalog.create_transaction(
                    holding, new_transaction_id
                )
            except CatalogError:    # transaction somehow already exists
                self.send_pathlist([], rk_complete, body, mode="failed")
                return

        for f in filelist:
            pd = PathDetails.from_dict(f)
            # don't restore missing, deleted files!
            if pd.path_type == PathType.MISSING:
                continue
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
            # create the object storage location if not null
            if pd.object_name is not None and pd.tenancy is not None:
                bucket_name, object_name = pd.object_name.split(':')
                # have to remove the "nlds" prefix from the bucket_name as it is
                # added in the create_location function below
                slug_len = len("nlds")+1
                bucket_name = bucket_name[slug_len:]
                obj_loc = self.catalog.create_location(
                    file_,
                    Storage.OBJECT_STORAGE,
                    url_scheme="http",
                    url_netloc=pd.tenancy,
                    root=bucket_name,
                    path=object_name,
                    access_time=datetime.fromtimestamp(
                        pd.access_time, tz=timezone.utc
                    )
                )
            # create the tape location
            if pd.tape_path is not None and pd.tape_url is not None:
                tape_loc = self.catalog.create_location(
                    file_,
                    Storage.TAPE,
                    url_scheme="root",
                    url_netloc=pd.tape_url,
                    #### !!!! NRM - 19/12/23 - I don't know what to put in here!
                    root=pd.tape_path,
                    path=pd.tape_path,
                    access_time=datetime.fromtimestamp(
                        pd.access_time, tz=timezone.utc
                    ),
                    aggregation="What?"
                )

            self.completelist.append(pd)

        # stop db transitions and commit
        self.catalog.save()
        self.catalog.end_session()

        self.send_pathlist(self.completelist, rk_complete, body, mode="failed")


    def _catalog_del(self, body: Dict, rk_origin: str, 
                     post_state: State) -> None:
        """Remove a given list of files from the catalog if the transfer 
        fails"""
=======
    def _catalog_del(self, body: Dict, rk_origin: str) -> None:
        """Remove a given list of files from the catalog if the transfer fails"""
>>>>>>> main
        # Parse the message body for required variables
        try:
            filelist = self._parse_filelist(body)
            user, group = self._parse_user_vars(body)
            holding_label, holding_id, holding_tag, _ = self._parse_metadata_vars(body)
        except CatalogError:
            # functions above handled message logging, here we just return
            return

        # start the database transactions
        self.reset()
        self.catalog.start_session()

<<<<<<< HEAD
        # The holding_label or holding_id must be supplied.
        if holding_label is None and holding_id is None:
            self.log("No method for identifying a holding or transaction "
                     "provided, exiting callback.", self.RK_LOG_ERROR)
            return
        
        # if the holding_label is None then get it from the holding_id and put
        # it in the message so that we can reconstruct with the correct label if
        # the delete goes wrong
        if holding_label is None:
=======
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
>>>>>>> main
            try:
                holding = self.catalog.get_holding(
                    user, group, holding_id=holding_id
                )
                # should only be one
                holding_label = holding[0].label
                # add to metadata
                body[RMQC.MSG_META][RMQC.MSG_LABEL] = holding_label
            except CatalogError:
                message = (f"Could not delete files from holding with holding_id: "
                           f"{holding_id}.  holding_id does not exist.")
                self.log(message, self.RK_LOG_DEBUG)
                raise CallbackError(message)
        
        for f in filelist:
            try:
                file_details = PathDetails.from_dict(f)
                # single, quick(er) method of get_file
                file = self.catalog.get_file(
                    user, group, holding_label=holding_label,
                    holding_id=holding_id, 
                    path = f["file_details"]["original_path"],
                    )
                # get the file details
                file_details = self.__File_to_PathDetails(file)
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
            else:
                self.completelist.append(file_details)

        # log the successful and non-successful catalog dels
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin, RK.CATALOG_DEL, RK.COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_DEL {self.completelist}",
                RK.LOG_DEBUG,
            )
<<<<<<< HEAD
            self.send_pathlist(self.completelist, rk_complete, body, 
                                state=post_state)
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
                               state=post_state)
=======
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body,
                state=State.CATALOG_DELETING,
            )
>>>>>>> main
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
<<<<<<< HEAD
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
                     "provided, will continue without.", self.RK_LOG_WARNING)

        for f in filelist:
            file_details = PathDetails.from_dict(f)
            try:
                # get the files first
                files = self.catalog.get_files(
                    user, group, 
                    holding_label=holding_label, 
                    holding_id=holding_id, 
                    transaction_id=transaction_id, 
                    original_path=file_details.original_path, 
                    tag=tag
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
                        raise
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
            self.completelist.append(file_details)

        # log the successful and non-successful catalog puts
        # SUCCESS
        if len(self.completelist) > 0:
            rk_complete = ".".join([rk_origin,
                                    self.RK_CATALOG_ARCHIVE_DEL, 
                                    self.RK_COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_DEL {self.completelist}",
                self.RK_LOG_DEBUG
            )
            self.send_pathlist(self.completelist, rk_complete, body, 
                               state=State.CATALOG_ARCHIVE_ROLLBACK)
        # RETRY
        if len(self.retrylist) > 0:
            rk_retry = ".".join([rk_origin,
                                 self.RK_CATALOG_ARCHIVE_DEL, 
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
                                  self.RK_CATALOG_ARCHIVE_DEL, 
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

=======
        self.catalog.end_session()
>>>>>>> main

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
<<<<<<< HEAD
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
=======
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
>>>>>>> main
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
                    )[0]
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
            holding_label, holding_id, tag, transaction_id = self._parse_metadata_vars(
                body
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
                t = self.catalog.get_transaction(id=f.transaction_id)
                h = self.catalog.get_holding(
                    user, group, groupall=groupall, holding_id=t.holding_id
                )[0]
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
                        "ingest_time": format_datetime(t.ingest_time),
                    }
                    ret_dict[h.label][MSG.TRANSACTIONS][t.transaction_id] = t_rec
                # get the locations
                locations = []
                for l in f.locations:
                    l_rec = {
                        "storage_type": l.storage_type,
                        "root": l.root,
                        "path": l.path,
                        "access_time": format_datetime(l.access_time),
                        "url": l.url,
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
                    "holding_id": h.id,
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
<<<<<<< HEAD
            exchange={'name': ''},
            correlation_id=properties.correlation_id
        )

    def _catalog_quota(self, body: Dict, properties: Header) -> None:
        """Return the user's quota for the given group."""
        message_vars = self._parse_user_vars(body)
        if message_vars is None:
            # Check if any problems have occured in the parsing of the message
            # body and exit if necessary
            self.log("Could not parse one or more mandatory variables, exiting"
                     "callback", self.RK_LOG_ERROR)
            return
        else:
            # Unpack if no problems found in parsing
            user, group = message_vars

        try:
            group_quota = self.authenticator.get_tape_quota(service_name=group)
        except CatalogError as e:
            # failed to get the tape quota - send a return message saying so
            self.log(e.message, self.RK_LOG_ERROR)
            body[self.MSG_DETAILS][self.MSG_FAILURE] = e.message
            body[self.MSG_DATA][self.MSG_QUOTA] = None
        else:
            # fill the return message with the group quota
            body[self.MSG_DATA][self.MSG_QUOTA] = group_quota
            self.log(
                f"Quota from CATALOG_QUOTA {group_quota}",
                self.RK_LOG_DEBUG
            )

        self.catalog.start_session()

        try:
            used_diskspace = self.catalog.get_used_diskspace(user=user, group=group)
        except CatalogError as e:
            # failed to get the used diskspace - send a return message saying so
            self.log(e.message, self.RK_LOG_ERROR)
            body[self.MSG_DETAILS][self.MSG_FAILURE] = e.message
            body[self.MSG_DATA][self.MSG_DISKSPACE] = None
        else:
            # fill the return message with the used diskspace
            body[self.MSG_DATA][self.MSG_DISKSPACE] = used_diskspace
            self.log(
                f"Used diskspace from CATALOG_QUOTA {used_diskspace}",
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

=======
            exchange={"name": ""},
            correlation_id=properties.correlation_id,
        )
>>>>>>> main

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
                f"Message did not contain an appropriate api_action, "
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
                f"Received from {self.queues[0].name} ({method.routing_key})",
                RK.LOG_DEBUG,
                body_json=body,
            )

        self.log(
            f"Appending rerouting information to message: "
            f"{self.DEFAULT_REROUTING_INFO} ",
            RK.LOG_DEBUG,
        )
        body = self.append_route_info(body)

        # check whether this is a GET or a PUT
        if api_method in (RK.GETLIST, RK.GET):
            # split the routing key
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
<<<<<<< HEAD
                self.log("Routing key inappropriate length, exiting callback.",
                        self.RK_LOG_ERROR)
                return
            if (rk_parts[1] == self.RK_CATALOG_GET):
                self.log(f"Running catalog get workflow", self.RK_LOG_INFO)
                self._catalog_get(body, rk_parts[0])
            elif (rk_parts[1] == self.RK_CATALOG_ARCHIVE_DEL):
                # If part of a GET transaction but received via the del topic 
                # then delete the previously added object storage Locations
                self.log(f"Deleting objectstore Locations as part of a failed "
                         f"archive-get workflow", self.RK_LOG_INFO)
                self._catalog_location_del(body, rk_parts[0], 
                                           location_type=Storage.OBJECT_STORAGE)

        elif (api_method == self.RK_PUTLIST) or (api_method == self.RK_PUT):           
=======
                self.log(
                    "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
                )
                return
            if rk_parts[2] == RK.START:
                if rk_parts[1] == RK.CATALOG_GET:
                    self._catalog_get(body, rk_parts[0])
                elif rk_parts[1] == RK.CATALOG_REMOVE:
                    self._catalog_remove(body, rk_parts[0], Storage.OBJECT_STORAGE)
                elif rk_parts[1] == RK.CATALOG_UPDATE:
                    self._catalog_update(body, rk_parts[0], create=False)

        elif api_method in (RK.PUTLIST, RK.PUT):
>>>>>>> main
            # split the routing key
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
<<<<<<< HEAD
                self.log("Routing key inappropriate length, exiting callback.",
                        self.RK_LOG_ERROR)
                return
            if (rk_parts[2] == self.RK_START):
                # Check the routing key worker section to determine which method 
                # to call, as a del could be being called from a failed 
=======
                self.log(
                    "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
                )
                return
            if rk_parts[2] == RK.START:
                # Check the routing key worker section to determine which method
                # to call, as a del could be being called from a failed
>>>>>>> main
                # transfer_put
                if rk_parts[1] == RK.CATALOG_PUT:
                    self._catalog_put(body, rk_parts[0])
<<<<<<< HEAD
                elif (rk_parts[1] == self.RK_CATALOG_DEL):
                    self._catalog_del(body, rk_parts[0], 
                                      post_state=State.CATALOG_ROLLBACK)
=======
                elif rk_parts[1] == RK.CATALOG_DEL:
                    self._catalog_del(body, rk_parts[0])
                elif rk_parts[1] == RK.CATALOG_UPDATE:
                    self._catalog_update(body, rk_parts[0], create=True)
>>>>>>> main

        # Archive put requires getting from the catalog
        elif api_method == RK.ARCHIVE_PUT:
            self.log("Starting an archive-put workflow", RK.LOG_DEBUG)
            # split the routing key
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
<<<<<<< HEAD
                self.log("Routing key inappropriate length, exiting callback.",
                        self.RK_LOG_ERROR)
                return
            if (rk_parts[1] == self.RK_CATALOG_ARCHIVE_NEXT):
                self.log("Beginning preparation of next archive aggregation", 
                         self.RK_LOG_DEBUG)
                self._catalog_archive_put(body, rk_parts[0])    
            elif (rk_parts[1] == self.RK_CATALOG_ARCHIVE_UPDATE):
                # NOTE: retries and failures for this method are handled by TLR
                self._catalog_archive_update(body, rk_parts[0])
            elif (rk_parts[1] == self.RK_CATALOG_ARCHIVE_DEL):
                self._catalog_location_del(
                    body, rk_parts[0], location_type=Storage.TAPE
                )
                
        elif (api_method == self.RK_DEL) or (api_method == self.RK_DELLIST):
            self.log("Starting an archive-del workflow", self.RK_LOG_DEBUG)
            # split the routing key
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
                self.log("Routing key inappropriate length, exiting callback.",
                        self.RK_LOG_ERROR)
                return
            if rk_parts[1] == self.RK_CATALOG_DEL:
                self._catalog_del(body, rk_parts[0], 
                                  post_state=State.CATALOG_DELETING)
            elif rk_parts[1] == self.RK_CATALOG_RESTORE:
                self._catalog_restore(body, rk_parts[0]) 
=======
                self.log(
                    "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
                )
>>>>>>> main

            if rk_parts[1] == RK.CATALOG_ARCHIVE_NEXT:
                self.log(
                    "Beginning preparation of next archive aggregation", RK.LOG_DEBUG
                )
                self._catalog_archive_put(body, rk_parts[0])
            elif rk_parts[1] == RK.CATALOG_ARCHIVE_UPDATE:
                # NOTE: retries and failures for this method are handled by TLR
                self._catalog_archive_update(body, rk_parts[0], Storage.TAPE)
            elif rk_parts[1] == RK.CATALOG_REMOVE:
                self._catalog_remove(body, rk_parts[0], Storage.TAPE)

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

        elif (api_method == self.RK_QUOTA):
            # don't need to split any routing key for an RPC method
            self._catalog_quota(body, properties)

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
