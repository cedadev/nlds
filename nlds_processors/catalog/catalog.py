# SQLalchemy imports
from sqlalchemy import func, Enum
from sqlalchemy.exc import IntegrityError, OperationalError, ArgumentError

from nlds_processors.catalog.catalog_models import CatalogBase, File, Holding,\
     Location, Transaction, Storage, Checksum, Tag
from nlds_processors.db_mixin import DBMixin

class CatalogError(Exception):
    def __init__(self, message, *args):
        super().__init__(args)
        self.message = message


class Catalog(DBMixin):
    """Catalog object containing methods to manipulate the Catalog Database"""

    def __init__(self, db_engine: str, db_options: str):
        """Store the catalog engine from the config strings passed in"""
        self.db_engine = None
        self.db_engine_str = db_engine
        self.db_options = db_options
        self.base = CatalogBase
        self.session = None


    def _user_has_get_holding_permission(self, 
                                         user: str, 
                                         group: str,
                                         holding: Holding) -> bool:
        """Check whether a user has permission to view this holding.
        When we implement ROLES this will be more complicated."""
        permitted = True
        permitted &= holding.user == user
        permitted &= holding.group == group
        return permitted


    def get_holding(self, 
                    user: str, 
                    group: str, 
                    label: str=None, 
                    holding_id: int=None,
                    transaction_id: str=None,
                    tag: dict=None) -> Holding:
        """Get a holding from the database"""
        assert(self.session != None)
        try:
            if holding_id:
                holding = self.session.query(Holding).filter(
                    Holding.user == user,
                    Holding.group == group,
                    Holding.id == holding_id,
                ).all()
            elif transaction_id:
                holding = self.session.query(Holding).filter(
                    Holding.user == user,
                    Holding.group == group,
                    Transaction.holding_id == Holding.id,
                    Transaction.transaction_id == transaction_id
                ).all()
            else:
                holding = self.session.query(Holding).filter(
                    Holding.user == user,
                    Holding.group == group,
                    Holding.label.regexp_match(label),
                ).all()
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
        except (IntegrityError, KeyError, ArgumentError):
            if holding_id:
                raise CatalogError(
                    f"Holding with holding_id:{holding_id} not found for "
                    f"user:{user} and group:{group}."
                )
            elif transaction_id:
                raise CatalogError(
                    f"Holding containing transaction_id:{transaction_id} not "
                    f"found for user:{user} and group:{group}."
                )
            else:
                raise CatalogError(
                    f"Holding with label:{label} not found for "
                    f"user:{user} and group:{group}."
                )
        except (OperationalError):
            raise CatalogError(
                f"Invalid regular expression:{label} when listing holding for "
                f"user:{user} and group:{group}."
            )

        return holding


    def create_holding(self, 
                       user: str, 
                       group: str, 
                       label: str) -> Holding:
        """Create the new Holding with the label, user, group"""
        assert(self.session != None)
        try:
            holding = Holding(
                label = label, 
                user = user, 
                group = group
            )
            self.session.add(holding)
            self.session.flush()            # update holding.id
        except (IntegrityError, KeyError) as e:
            raise CatalogError(
                f"Holding with label:{label} could not be added to the database."
            )
        return holding


    def modify_holding(self, 
                       holding: Holding,
                       new_label: str=None, 
                       new_tags: dict=None) -> Holding:
        """Find a holding and modify the information in it"""
        assert(self.session != None)
        # change the label if a new_label supplied
        if new_label:
            try:
                holding.label = new_label
                self.session.flush()
            except IntegrityError:
                raise CatalogError(
                    f"Cannot change holding with label:{holding.label} and "
                    f"holding_id:{holding.id} to new label:{new_label}. New "
                    f"label:{new_label} already in use by another holding."
                )

        if new_tags:
            for k in new_tags:
                # create_tag takes a key and value
                tag = self.create_tag(holding, k, new_tags[k])
                self.session.flush()

        return holding


    def get_transaction(self, 
                        id: int=None, 
                        transaction_id: str=None) -> Transaction:
        """Get a transaction from the database"""
        assert(self.session != None)
        try:
            if transaction_id:
                transaction = self.session.query(Transaction).filter(
                    Transaction.transaction_id == transaction_id
                ).one_or_none()
            else:
                transaction = self.session.query(Transaction).filter(
                    Transaction.id == id
                ).one_or_none()
        except (IntegrityError, KeyError):
            if transaction_id:
                raise CatalogError(
                    f"Transaction with transaction_id:{transaction_id} not "
                    "found."
                )
            else:
                raise CatalogError(
                    f"Transaction with id {id} not found."
                )
        return transaction


    def create_transaction(self, 
                           holding: Holding, 
                           transaction_id: str) -> Transaction:
        """Create a transaction that belongs to a holding and will contain files"""
        assert(self.session != None)
        try:
            transaction = Transaction(
                holding_id = holding.id,
                transaction_id = transaction_id,
                ingest_time = func.now()
            )
            self.session.add(transaction)
            self.session.flush()           # flush to generate transaction.id
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"Transaction with transaction_id:{transaction_id} could not "
                "be added to the database"
            )
        return transaction


    def _user_has_get_file_permission(self, 
                                      user: str, 
                                      group: str,
                                      file: File) -> bool:
        """Check whether a user has permission to access a file.
        Later, when we implement the ROLES this function will be a lot more
        complicated!"""
        assert(self.session != None)
        holding = self.session.query(Holding).filter(
            Transaction.id == file.transaction_id,
            Holding.id == Transaction.holding_id
        ).all()
        permitted = True
        for h in holding:
            permitted &= h.user == user
            permitted &= h.group == group

        return permitted


    def get_file(self, 
                 user: str, 
                 group: str,
                 original_path: str, 
                 holding: Holding = None, 
                 missing_error_fl: bool = True) -> File:
        """Get a single file details from the database, given the original path 
        of the file.  
        An optional holding can be supplied to get the file details from a
        particular holding - e.g. with a holding label, or tags"""
        assert(self.session != None)
        try:
            # holding is a list, but there should only be one
            if holding:
                try: 
                    holding = holding[0]
                except TypeError:
                    pass
                file = self.session.query(File).filter(
                    Transaction.id == File.transaction_id,
                    Transaction.holding_id == holding.id,
                    File.original_path.regexp_match(original_path),
                # this order_by->first is in case multiple copies of a file
                # have been added to the holding.  This will be illegal at some
                # point, but for now this is an easy fix
                ).order_by(Transaction.ingest_time.desc()).first()
                err_msg = (
                    f"File:{original_path} not found in holding:"
                    f"{holding.label} for user:{user} in group:{group}."
                )
            else:
                # if no holding given then we want to return the most recent
                # file with this original path
                file = self.session.query(File).filter(
                    Transaction.id == File.transaction_id,
                    File.original_path.regexp_match(original_path)
                ).order_by(Transaction.ingest_time.desc()).first()
                err_msg = (
                    f"File:{original_path} not found for user:{user} in "
                    f"group:{group}."
                )

            # check the file was found
            if not file and missing_error_fl: 
                raise CatalogError(err_msg)

            # check user has permission to access this file
            if (file and not self._user_has_get_file_permission(user, group, 
                                                                    file)):
                raise CatalogError(
                    f"User:{user} in group:{group} does not have permission to "
                    f"access the file with original path:{original_path}."
                )

        except (IntegrityError, KeyError, OperationalError):
            if holding:
                err_msg = (f"File with original path:{original_path} not found "
                           f"in holding:{holding[0].label}")
            else:
                err_msg = f"File with original path:{original_path} not found"
            raise CatalogError(err_msg)
        return file


    def get_files(self, 
                  user: str, 
                  group: str, 
                  holding_label: str=None, 
                  holding_id: int=None,
                  transaction_id: str=None,
                  path: str=None, 
                  tag: dict=None) -> list:

        """Get a multitue of file details from the database, given the user,
        group, label, holding_id, path (can be regex) or tag(s)"""
        assert(self.session != None)
        # Nones are set to .* in the regexp matching
        # get the matching holdings first, these match all but the path
        if holding_label:
            holding_search = holding_label
        else:
            holding_search = ".*"

        holding = self.get_holding(
            user, group, holding_search, holding_id, transaction_id, tag
        )

        if path:
            search_path = path
        else:
            search_path = ".*"

        # (permissions have been checked by get_holding)
        file_list = []
        try:
            for h in holding:
                if transaction_id:
                    file = self.session.query(File).filter(
                        Transaction.id == File.transaction_id,
                        Transaction.transaction_id == transaction_id,
                        Transaction.holding_id == h.id,
                        File.original_path.regexp_match(search_path)
                    ).all()
                else:
                    file = self.session.query(File).filter(
                        Transaction.id == File.transaction_id,
                        Transaction.holding_id == h.id,
                        File.original_path.regexp_match(search_path)
                    ).all()
                for f in file:
                    file_list.append(f)
            # no files found
            if len(file_list) == 0:
                raise KeyError

        except (IntegrityError, KeyError, OperationalError):
            if holding_label:
                err_msg = f"File with holding_label:{holding_label} not found "
            elif holding_id:
                err_msg = f"File with holding_id:{holding_id} not found "
            elif transaction_id:
                err_msg = f"File with transaction_id:{transaction_id} not found "
            else:
                err_msg = f"File with original_path:{path} not found "
            raise CatalogError(err_msg)
        
        return file_list


    def create_file(self, 
                    transaction: Transaction, 
                    user: str = None,
                    group: str = None,
                    original_path: str = None,
                    path_type: str = None,
                    link_path: str = None,
                    size: str = None,
                    file_permissions: str = None) -> File:
        """Create a file that belongs to a transaction and will contain 
        locations"""
        assert(self.session != None)
        try:
            new_file = File(
                transaction_id = transaction.id,
                original_path = original_path,
                path_type = path_type,
                link_path = link_path,
                size = int(size * 1000),
                user = user,
                group = group,
                file_permissions = file_permissions
            )
            self.session.add(new_file)
            self.session.flush()           # flush to generate file.id
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"File with original path {original_path} could not be added to"
                 " the database"
            )
        return new_file

    def delete_files(self, 
                     user: str, 
                     group: str, 
                     holding_label: str=None,  
                     holding_id: int=None,
                     path: str=None, 
                     tag: dict=None) -> list:
        """Delete a given path from the catalog. If a holding is specified only 
        the matching file from that holding will be deleted, otherwise all 
        matching files will. Utilises get_files().
        
        """
        assert(self.session != None)

        files = self.get_files(user, group, holding_label=holding_label, 
                               holding_id=holding_id, path=path, tag=tag)
        checkpoint = self.session.begin_nested()
        try:
            for f in files:
                self.session.delete(f)
        except (IntegrityError, KeyError, OperationalError):
            # This rollsback only to the checkpoint, so any successful deletes 
            # done already will stay in the transaction.
            checkpoint.rollback()
            err_msg = f"File with original_path:{path} could not be deleted"
            raise CatalogError(err_msg)

    def get_location(self, 
                     file: File, 
                     storage_type: Enum) -> Location:
        """Get a storage location for a file, given the file and the storage
        type"""
        assert(self.session != None)
        try:
            location = self.session.query(Location).filter(
                Location.file_id == file.id,
                Location.storage_type == storage_type
            ).one_or_none()
        except (IntegrityError, KeyError):
            raise CatalogError(            
                f"Location of storage type {storage_type} not found for file "
                f"{file.original_path}"
            )
        return location


    def create_location(self, 
                        file: File,
                        storage_type: Enum,
                        root: str,
                        object_name: str, 
                        access_time: float) -> Location:
        """Add the storage location for object storage"""
        assert(self.session != None)
        try:
            location = Location(
                storage_type = storage_type,
                # root is bucket for Object Storage which is the transaction id
                # which is now stored in the Holding record
                root = root,
                # path is object_name for object storage
                path = object_name,
                # access time is passed in the file details
                access_time = access_time,
                file_id = file.id
            )
            self.session.add(location)
            self.session.flush()           # flush to generate location.id
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"Location with root {root}, path {file.original_path} and "
                f"storage type {Storage.OBJECT_STORAGE} could not be added to "
                 "the database")
        return location


    def create_tag(self,
                   holding: Holding,
                   key: str,
                   value: str):
        """Create a tag and add it to a holding"""
        assert(self.session != None)
        try:
            tag = Tag(
                key = key,
                value = value,
                holding_id = holding.id
            )
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"Tag could not be added to holding:{holding.label}"
            )
        return tag