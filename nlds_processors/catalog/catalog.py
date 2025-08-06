# encoding: utf-8
"""
catalog.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import List

# SQLalchemy imports
from sqlalchemy import func, Enum
from sqlalchemy.exc import (
    IntegrityError,
    OperationalError,
    ArgumentError,
    NoResultFound,
    DataError,
)

from nlds_processors.catalog.catalog_models import (
    CatalogBase,
    File,
    Holding,
    Location,
    Transaction,
    Aggregation,
    Storage,
    Tag,
)
from nlds_processors.db_mixin import DBMixin
from nlds_processors.catalog.catalog_error import CatalogError
from nlds.details import PathType


class Catalog(DBMixin):
    """Catalog object containing methods to manipulate the Catalog Database"""

    def __init__(self, db_engine: str, db_options: str):
        """Store the catalog engine from the config strings passed in"""
        self.db_engine = None
        self.db_engine_str = db_engine
        self.db_options = db_options
        self.base = CatalogBase
        self.session = None

    @staticmethod
    def _user_has_get_holding_permission(
        user: str, group: str, holding: Holding
    ) -> bool:
        """Check whether a user has permission to view this holding.
        When we implement ROLES this will be more complicated."""
        permitted = True
        # search of "**all**" can access everything
        if user != "**all**" and group != "**all**":
            # Users can view / get all holdings in their group
            permitted &= holding.group == group
        return permitted

    def get_holding(
        self,
        user: str,
        group: str,
        groupall: bool = False,
        label: str = None,
        holding_id: int = None,
        transaction_id: str = None,
        tag: dict = None,
        regex: bool = False,
        limit: int = None,
        descending: bool = False,
    ) -> List[Holding]:
        """Get a holding from the database"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            # build holding query bit by bit
            holding_q = self.session.query(Holding)
            if group != "**all**":
                holding_q = holding_q.filter(Holding.group == group)
            # if the groupall flag is set then don't filter on user
            if not groupall and user != "**all**":
                holding_q = holding_q.filter(Holding.user == user)

            # Note - the order of these if.elif.elif statements is very important - do
            # not change the order!
            # holding id filtering - for when the user supplies a holding id
            if holding_id:
                holding_q = holding_q.filter(
                    Holding.id == holding_id,
                )

            # transaction id filtering - for when a large upload has been split into
            # multiple uploads
            elif transaction_id:
                holding_q = holding_q.filter(
                    Transaction.holding_id == Holding.id,
                    Transaction.transaction_id == transaction_id,
                )

            # search label filtering - for when the user supplies a holding label
            elif label:
                # regex will throw exception below if invalid
                if regex:
                    holding_q = holding_q.filter(Holding.label.regexp_match(label))
                else:
                    holding_q = holding_q.filter(Holding.label == label)

            # filter the query on any tags
            if tag:
                # get the holdings that have a key that matches one or more of
                # the keys in the tag dictionary passed as a parameter
                holding_q = holding_q.join(Tag).filter(Tag.key.in_(tag.keys()))
                # check for zero
                if holding_q.count() != 0:
                    # we have now got a subset of holdings with a tag that has
                    # a key that matches the keys in the input dictionary
                    # now find the holdings where the key and value match
                    for key, item in tag.items():
                        holding_q = holding_q.filter(Tag.key == key, Tag.value == item)

            # Get the holdings, up to the limit if set
            holding_q = holding_q.join(Transaction)
            # sort the holdings if set
            if descending:
                holding_q = holding_q.order_by(Transaction.ingest_time.desc())
            else:
                holding_q = holding_q.order_by(Transaction.ingest_time)

            if holding_q.count() == 0:
                holding = []
            elif limit:  # might have to change this back to .limit if DB gets big
                holding = holding_q.all()[0:limit]
            else:
                holding = holding_q.all()

            # check if at least one holding found
            if len(holding) == 0:
                raise KeyError
            # check the user has permission to view the holding(s)
            for h in holding:
                if not self._user_has_get_holding_permission(user, group, h):
                    raise CatalogError(
                        f"User:{user} in group:{group} does not have permission "
                        f"to access the holding with label:{h.label}."
                    )
        except (IntegrityError, KeyError, ArgumentError) as e:
            msg = ""
            if holding_id:
                msg = (
                    f"Holding with holding_id:{holding_id} not found for "
                    f"user:{user} and group:{group}"
                )
            elif transaction_id:
                msg = (
                    f"Holding containing transaction_id:{transaction_id} not "
                    f"found for user:{user} and group:{group}"
                )
            elif label:
                msg = (
                    f"Holding with label:{label} not found for "
                    f"user:{user} and group:{group}"
                )
            else:
                msg = f"No holdings found for user:{user} and group:{group}"
            if tag:
                msg += f" with tags:{tag}."
            else:
                msg += "."
            raise CatalogError(msg)
        except DataError as e:
            if regex:
                msg = f"Invalid regular expression: {label}"
            else:
                msg = f"Error getting Holding: {e}"
            raise CatalogError(msg)

        except OperationalError as e:
            raise CatalogError(
                f"Error when when listing holding for user:{user} and group:{group}. "
                f"Original error {e}"
            )
        return holding

    def create_holding(self, user: str, group: str, label: str) -> Holding:
        """Create the new Holding with the label, user, group"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            holding = Holding(label=label, user=user, group=group)
            self.session.add(holding)
            self.session.flush()  # update holding.id
        except (IntegrityError, KeyError) as e:
            raise CatalogError(
                f"Holding with label:{label} could not be added to the database."
            )
        return holding

    def modify_holding(
        self,
        holding: Holding,
        new_label: str = None,
        new_tags: dict = None,
        del_tags: dict = None,
    ) -> Holding:
        """Find a holding and modify the information in it"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        if not isinstance(holding, Holding):
            raise CatalogError(
                f"Cannot modify holding, it does not appear to be a valid "
                f"Holding ({holding})."
            )
        # change the label if a new_label supplied
        if new_label:
            try:
                holding.label = new_label
                self.session.flush()
            except IntegrityError:
                # rollback so we can access the holding
                self.session.rollback()
                raise CatalogError(
                    f"Cannot change holding with label:{holding.label} and "
                    f"holding_id:{holding.id} to new label:{new_label}. New "
                    f"label:{new_label} already in use by another holding."
                )

        if new_tags:
            for k in new_tags:
                # if the tag exists then modify it, if it doesn't then create it
                try:
                    # get
                    tag = self.get_tag(holding, k)
                except CatalogError:
                    # create
                    tag = self.create_tag(holding, k, new_tags[k])
                else:
                    # modify
                    tag = self.modify_tag(holding, k, new_tags[k])
        if del_tags:
            for k in del_tags:
                # if the tag exists and the value matches then delete it
                tag = self.get_tag(holding, k)
                if tag.value == del_tags[k]:
                    self.delete_tag(holding, k)
        self.session.flush()

        return holding

    def get_transaction(
        self, id: int = None, transaction_id: str = None
    ) -> Transaction:
        """Get a transaction from the database"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            if transaction_id:
                transaction = (
                    self.session.query(Transaction)
                    .filter(Transaction.transaction_id == transaction_id)
                    .one_or_none()
                )
            else:
                transaction = (
                    self.session.query(Transaction)
                    .filter(Transaction.id == id)
                    .one_or_none()
                )
        except (IntegrityError, KeyError):
            if transaction_id:
                raise CatalogError(
                    f"Transaction with transaction_id:{transaction_id} not found."
                )
            else:
                raise CatalogError(f"Transaction with id {id} not found.")
        return transaction

    def get_location_file(self, location: Location) -> File:
        """Get a File but from the other end of the database tree, starting from
        a location.
        """
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            file_ = (
                self.session.query(File)
                .filter(File.id == location.file_id)
                .one_or_none()
            )
        except (IntegrityError, KeyError):
            raise CatalogError(f"File for location:{location.id} not retrievable.")
        return file_

    def create_transaction(self, holding: Holding, transaction_id: str) -> Transaction:
        """Create a transaction that belongs to a holding and will contain files"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            transaction = Transaction(
                holding_id=holding.id,
                transaction_id=transaction_id,
                ingest_time=func.now(),
            )
            self.session.add(transaction)
            self.session.flush()  # flush to generate transaction.id
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"Transaction with transaction_id:{transaction_id} could not "
                "be added to the database"
            )
        return transaction

    def _user_has_get_file_permission(self, user: str, group: str, file: File) -> bool:
        """Check whether a user has permission to access a file.
        Later, when we implement the ROLES this function will be a lot more
        complicated!"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        holding = (
            self.session.query(Holding)
            .filter(
                Transaction.id == file.transaction_id,
                Holding.id == Transaction.holding_id,
            )
            .all()
        )
        permitted = True
        for h in holding:
            # users have get file permission if in group
            # permitted &= h.user == user
            if user != "**all**" and group != "**all**":
                permitted &= h.group == group

        return permitted

    def get_files(
        self,
        user: str,
        group: str,
        groupall: bool = False,
        holding_label: str = None,
        holding_id: int = None,
        transaction_id: str = None,
        original_path: str = None,
        tag: dict = None,
        newest_only: bool = False,
        regex: bool = False,
        limit: int = None,
        descending: bool = False,
    ) -> list:
        """Get a multitude of file details from the database, given the user,
        group, label, holding_id, path (can be regex) or tag(s)"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        # newest only we always want to sort descending
        if newest_only:
            descending = True
        # Nones are set to .* in the regexp matching
        # get the matching holdings first, these match all but the path
        holding = self.get_holding(
            user,
            group,
            groupall=groupall,
            label=holding_label,
            holding_id=holding_id,
            transaction_id=transaction_id,
            tag=tag,
            descending=descending,
        )
        if original_path:
            search_path = original_path
        else:
            search_path = ".*"

        # (permissions have been checked by get_holding)
        file_list = []
        path_list = []
        try:
            for h in holding:
                # build the file query bit by bit
                file_q = self.session.query(File, Transaction).filter(
                    File.transaction_id == Transaction.id,
                    Transaction.holding_id == h.id,
                )
                if descending:
                    file_q = file_q.order_by(Transaction.ingest_time.desc())
                else:
                    file_q = file_q.order_by(Transaction.ingest_time)
                if regex or search_path == ".*":
                    # will throw an exception here for bad regex
                    file_q = file_q.filter(File.original_path.regexp_match(search_path))
                else:
                    file_q = file_q.filter(File.original_path == search_path)

                if file_q.count() == 0:
                    result = []
                elif limit:
                    result = file_q.limit(limit).all()
                else:
                    result = file_q.all()

                # only want one file if newest_only is set: (this is for downloading
                # when only the path is specified and no holding id or label is given)
                # the results have been ordered by the Transaction ingest time, if the
                # file already exists in the file_list then it is newer
                for r in result:
                    if r.File is None:
                        continue
                    # check user has permission to access this file
                    if r.File and not self._user_has_get_file_permission(
                        user, group, r.File
                    ):
                        raise CatalogError(
                            f"User:{user} in group:{group} does not have permission to "
                            f"access the file with original path:{r.File.original_path}."
                        )
                    if newest_only:
                        if not r.File.original_path in path_list:
                            file_list.append(r.File)
                            path_list.append(r.File.original_path)
                    else:
                        file_list.append(r.File)
                        path_list.append(r.File.original_path)
                    if limit and len(file_list) >= limit:
                        break
                if limit and len(file_list) >= limit:
                    break

            # no files found
            if len(file_list) == 0:
                raise KeyError

        except (IntegrityError, KeyError, OperationalError):
            if holding_label:
                err_msg = (
                    f"File not found in holding with holding_label:{holding_label}"
                )
            elif holding_id:
                err_msg = f"File not found in holding with holding_id:{holding_id}"
            elif transaction_id:
                err_msg = (
                    f"File not found in holding with transaction_id:{transaction_id}"
                )
            elif tag:
                err_msg = f"File not found in holding with tag:{tag}"
            else:
                err_msg = f"File with original_path:{original_path} not found "
            raise CatalogError(err_msg)

        except DataError as e:
            if regex:
                msg = f"Invalid regular expression: {search_path}"
            else:
                msg = f"Error getting Holding: {e}"
            raise CatalogError(msg)

        return file_list

    def create_file(
        self,
        transaction: Transaction,
        user: str = None,
        group: str = None,
        original_path: str = None,
        path_type: str = None,
        link_path: str = None,
        size: str = None,
        file_permissions: str = None,
    ) -> File:
        """Create a file that belongs to a transaction and will contain
        locations"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            new_file = File(
                transaction_id=transaction.id,
                original_path=original_path,
                path_type=path_type,
                link_path=link_path,
                size=int(size),
                user=user,
                group=group,
                file_permissions=file_permissions,
            )
            self.session.add(new_file)
            self.session.flush()  # flush to generate file.id
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"File with original path {original_path} could not be added to"
                " the database"
            )
        return new_file

    def delete_files(
        self,
        user: str,
        group: str,
        holding_label: str = None,
        holding_id: int = None,
        transaction_id: str = None,
        path: str = None,
        tag: dict = None,
    ) -> list:
        """Delete a given path from the catalog. If a holding is specified only
        the matching file from that holding will be deleted, otherwise all
        matching files will. Utilises get_files().

        """
        if self.session is None:
            raise RuntimeError("self.session is None")

        files = self.get_files(
            user,
            group,
            holding_label=holding_label,
            holding_id=holding_id,
            transaction_id=transaction_id,
            original_path=path,
            tag=tag,
        )
        checkpoint = self.session.begin_nested()
        try:
            for f in files:
                # First get parent transaction and holding
                transaction = self.get_transaction(f.transaction_id)
                holding = self.get_holding(
                    user, group, holding_id=transaction.holding_id
                )[0]
                self.session.delete(f)
                if len(transaction.files) == 0:
                    self.session.delete(transaction)
                if len(holding.transactions) == 0:
                    self.session.delete(holding)
        except (IntegrityError, KeyError, OperationalError):
            # This rollsback only to the checkpoint, so any successful deletes
            # done already will stay in the transaction.
            checkpoint.rollback()
            err_msg = f"File with original_path:{path} could not be deleted"
            raise CatalogError(err_msg)

    def get_location(self, file: File, storage_type: Enum) -> Location:
        """Get a storage location for a file, given the file and the storage
        type"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            location = (
                self.session.query(Location)
                .filter(
                    Location.file_id == file.id, Location.storage_type == storage_type
                )
                .one_or_none()
            )
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"Location of storage type {storage_type} not found for file "
                f"{file.original_path}"
            )
        return location

    def create_location(
        self,
        file_: File,
        storage_type: Enum,
        url_scheme: str,
        url_netloc: str,
        root: str,
        path: str,
        access_time: float,
        aggregation: Aggregation = None,
    ) -> Location:
        """Add the storage location for either object storage or tape"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        if aggregation is None:
            aggregation_id = None
        else:
            aggregation_id = aggregation.id
        try:
            location = Location(
                storage_type=storage_type,
                url_scheme=url_scheme,
                url_netloc=url_netloc,
                # root is bucket for Object Storage which is the transaction id
                # which is now stored in the Holding record
                root=root,
                # path is object_name for object storage
                path=path,
                # access time is passed in the file details
                access_time=access_time,
                file_id=file_.id,
                aggregation_id=aggregation_id,
            )
            self.session.add(location)
            self.session.flush()  # flush to generate location.id
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"Location with root {root}, path {file_.original_path} and "
                f"storage type {storage_type} could not be added to "
                "the database"
            )
        return location

    def delete_location(self, file: File, storage_type: Enum) -> None:
        """Delete the location for a given file and storage_type"""
        location = self.get_location(file, storage_type=storage_type)
        checkpoint = self.session.begin_nested()
        try:
            self.session.delete(location)
        except (IntegrityError, KeyError, OperationalError):
            # This rollsback only to the checkpoint, so any successful deletes
            # done already will stay in the transaction.
            checkpoint.rollback()
            err_msg = (
                f"Location with file.id {file.id} and storage_type "
                f"{storage_type} could not be deleted."
            )
            raise CatalogError(err_msg)

    def create_tag(self, holding: Holding, key: str, value: str):
        """Create a tag and add it to a holding"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            tag = Tag(key=key, value=value, holding_id=holding.id)
            self.session.add(tag)
            self.session.flush()  # flush to generate tag.id
        except (IntegrityError, KeyError):
            raise CatalogError(f"Tag could not be added to holding:{holding.label}")
        return tag

    def get_tag(self, holding: Holding, key: str):
        """Get the tag with a specific key"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            tag = (
                self.session.query(Tag)
                .filter(Tag.key == key, Tag.holding_id == holding.id)
                .one()
            )  # uniqueness constraint guarantees only one
        except (NoResultFound, KeyError):
            raise CatalogError(f"Tag with key:{key} not found")
        return tag

    def modify_tag(self, holding: Holding, key: str, value: str):
        """Modify a tag that has the key, with a new value.
        Tag has to exist, current value will be overwritten."""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            tag = (
                self.session.query(Tag)
                .filter(Tag.key == key, Tag.holding_id == holding.id)
                .one()
            )  # uniqueness constraint guarantees only one
            tag.value = value
        except (NoResultFound, KeyError):
            raise CatalogError(f"Tag with key:{key} not found")
        return tag

    def delete_tag(self, holding: Holding, key: str):
        """Delete a tag that has the key"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        # use a checkpoint as the tags are being deleted in an external loop and
        # using a checkpoint will ensure that any completed deletes are committed
        checkpoint = self.session.begin_nested()
        try:
            tag = (
                self.session.query(Tag)
                .filter(Tag.key == key, Tag.holding_id == holding.id)
                .one()
            )  # uniqueness constraint guarantees only one
            self.session.delete(tag)
        except (NoResultFound, KeyError):
            checkpoint.rollback()
            raise CatalogError(f"Tag with key:{key} not found")
        return None

    def create_aggregation(
        self, tarname: str, checksum: str = None, algorithm: str = None
    ) -> Aggregation:
        """Create an aggregation of files to write to tape as a tar file"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            aggregation = Aggregation(
                tarname=tarname,
                checksum=checksum,
                algorithm=algorithm,
                failed_fl=False,  # Aggregations fail before creation now
            )
            self.session.add(aggregation)
            self.session.flush()  # flush to generate aggregation.id
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"Aggregation with tarname:{tarname} could not be added to the "
                f"database"
            )
        return aggregation

    def get_aggregation(self, aggregation_id: int) -> Aggregation:
        """Simple function for getting of Aggregation from aggregation_id."""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            # Get the aggregation for a particular file via it's tape location
            aggregation = (
                self.session.query(Aggregation)
                .filter(
                    Aggregation.id == aggregation_id,
                )
                .one_or_none()
            )
            # There should only ever be one aggregation per tape location and
            # only one tape location per file
        except (NoResultFound, KeyError):
            raise CatalogError(f"Aggregation with id:{aggregation_id} not found.")
        return aggregation

    def delete_aggregation(self, aggregation: Aggregation) -> None:
        """Delete a given aggregation"""
        try:
            self.session.delete(aggregation)
        except (IntegrityError, KeyError, OperationalError):
            err_msg = (
                f"Aggregation with aggregation.id {aggregation.id} could "
                f"not be deleted."
            )
            raise CatalogError(err_msg)

    def get_next_unarchived_holding(self, tenancy: str) -> Holding:
        """The principal function for getting the next unarchived holding to
        archive aggregate.
        A tenancy is passed in so that the only holdings attempted to be backed up are
        those that can be accessed via the object store keys also passed in the body.
        Otherwise, when the archive_put process tries to stream the files from the
        object store to the tape, the keys don't match the tenancy and an access denied
        error is produced.
        """
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            # To get unarchived Holdings we need to find Transactions in a holding that
            # contains files that do not have a Tape location
            # however, they do have to have a Object Storage location, as this shows
            # that the file was successfully transferred to Object Storage.
            # There are four cases:
            # 1. Files without either a Object Storage or Tape location are mid transfer
            #    to the Object Store
            # 2. Files with an Object Storage, but no Tape location are on the Object
            #    Storage but require backing up to tape
            # 3. Files with an Object Storage and Tape location are on the Object
            #    Storage and have already been backed up to Tape
            # 4. Files with a Tape location, but no Object Storage location have been
            #    removed from Object Storage due to space constraints, and will need to
            #    be fetched from Tape on a user GET

            next_holding = (
                self.session.query(Holding)
                .filter(
                    Transaction.holding_id == Holding.id,
                    File.transaction_id == Transaction.id,
                    File.path_type == PathType.FILE,
                    ~File.locations.any(Location.storage_type == Storage.TAPE),
                    File.locations.any(Location.storage_type == Storage.OBJECT_STORAGE),
                    # tenancy is stored in url_netloc part of Location
                    File.locations.any(Location.url_netloc == tenancy),
                )
                # Order randomly so that if one archive fails, it won't prevent the
                # others from archiving
                .order_by(func.random())
                .first()
            )

        except (NoResultFound, KeyError):
            raise CatalogError(f"Couldn't get unarchived holdings")
        return next_holding

    def get_unarchived_files(self, holding: Holding) -> List[File]:
        """The principal function for getting unarchived files to aggregate and
        send to archive put."""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            # Get all files for the given holding. Again we have to ensure that the
            # transfer to object storage has completed and the files are not
            # mid-transfer
            unarchived_files = (
                self.session.query(File)
                .filter(
                    Transaction.holding_id == holding.id,
                    File.transaction_id == Transaction.id,
                    File.path_type == PathType.FILE,
                    ~File.locations.any(Location.storage_type == Storage.TAPE),
                    File.locations.any(Location.storage_type == Storage.OBJECT_STORAGE),
                )
                .all()
            )
        except (NoResultFound, KeyError):
            raise CatalogError(
                f"Couldn't find unarchived files for holding with id:{holding.id}"
            )
        return unarchived_files
