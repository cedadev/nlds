# SQLalchemy imports
from sqlalchemy import func, Enum
from sqlalchemy.exc import IntegrityError, OperationalError

from nlds_processors.catalog.catalog_models import Base, File, Holding,\
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
        self.db_engine = db_engine
        self.db_options = db_options
        self.base = Base


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
                    tag: dict=None) -> Holding:
        """Get a holding from the database"""
        try:
            if holding_id:
                holding = self.session.query(Holding).filter(
                    Holding.user == user,
                    Holding.group == group,
                    Holding.id == holding_id
                ).all()
            else:
                holding = self.session.query(Holding).filter(
                    Holding.user == user,
                    Holding.group == group,
                    Holding.label.regexp_match(label)
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
        except (IntegrityError, KeyError):
            if holding_id:
                raise CatalogError(
                    f"Holding with holding_id:{holding_id} not found for "
                    f"user:{user} and group:{group}."
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
        try:
            holding = Holding(
                label = label, 
                user = user, 
                group = group
            )
            self.session.add(holding)
            self.session.flush()            # update holding.id
            self.commit_required = True     # indicate a commit at end of session
        except (IntegrityError, KeyError) as e:
            raise CatalogError(
                f"Holding with label:{label} could not be added to the database."
            )
        return holding


    def modify_holding(self, 
                       user: str, 
                       group:str, 
                       holding_label: str=None, 
                       holding_id: int=None, 
                       tag: dict=None,
                       new_label: str=None, 
                       new_tag: dict=None) -> Holding:
        """Find a holding and modify the information in it"""
        holdings=None
        if holding_label or holding_id:
            holdings = self.get_holding(
                user, group, holding_label, holding_id
            )

        if not holdings or len(holdings) == 0:
            raise CatalogError(
                "Holding not found: holding_id or label not specified"
            )
        
        if len(holdings) > 1:
            if holding_label:
                raise CatalogError(
                    f"More than one holding returned for label:{holding_label}"
                )
            elif holding_id:
                raise CatalogError(
                    f"More than one holding returned for holding_id:{holding_id}"
                )
        else:
            holding = holdings[0]

        # change the label if a new_label supplied
        if new_label:
            try:
                holding.label = new_label
                self.session.flush()
                self.commit_required = True
            except IntegrityError:
                raise CatalogError(
                    f"Cannot change holding with label:{holding_label} and "
                    f"holding_id:{holding_id} to new label:{new_label}. New "
                    f"label:{new_label} already in use by another holding."
                )

        return holding


    def get_transaction(self, 
                        id: int=None, 
                        transaction_id: str=None) -> Transaction:
        """Get a transaction from the database"""
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
        try:
            transaction = Transaction(
                holding_id = holding.id,
                transaction_id = transaction_id,
                ingest_time = func.now()
            )
            self.session.add(transaction)
            self.session.flush()           # flush to generate transaction.id
            self.commit_required = True    # indicate a commit at end of session
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
                 holding: Holding = None) -> File:
        """Get a single file details from the database, given the original path 
        of the file.  
        An optional holding can be supplied to get the file details from a
        particular holding - e.g. with a holding label, or tags"""
        try:
            # holding is a list, but there should only be one
            if holding:
                file = self.session.query(File).filter(
                    Transaction.id == File.transaction_id,
                    Transaction.holding_id == holding[0].id,
                    File.original_path == original_path,
                # this order_by->first is in case multiple copies of a file
                # have been added to the holding.  This will be illegal at some
                # point, but for now this is an easy fix
                ).order_by(Transaction.ingest_time.desc()).first()
                err_msg = (
                    f"File:{original_path} not found in holding:"
                    f"{holding[0].label} for user:{user} in group:{group}."
                )
            else:
                # if no holding given then we want to return the most recent
                # file with this original path
                file = self.session.query(File).filter(
                    File.original_path == original_path,
                    Transaction.id == File.transaction_id
                ).order_by(Transaction.ingest_time.desc()).first()
                err_msg = (
                    f"File:{original_path} not found for user:{user} in "
                    f"group:{group}."
                )

            # check the file was found
            if not file:
                raise CatalogError(err_msg)

            # check user has permission to access this file
            if not self._user_has_get_file_permission(user, group, file):
                raise CatalogError(
                    f"User:{user} in group:{group} does not have permission to "
                    f"access the file with original path:{original_path}."
                )

        except (IntegrityError, KeyError):
            if holding:
                err_msg = (f"File with original path:{original_path} not found "
                           f"in holding:{holding.label}")
            else:
                err_msg = f"File with original path:{original_path} not found"
            raise CatalogError(err_msg)
        return file


    def get_files(self, 
                  user: str, 
                  group: str, 
                  holding_label: str=None, 
                  holding_id: int=None,
                  path: str=None, 
                  tag: dict=None) -> list:

        """Get a multitue of file details from the database, given the user,
        group, label, holding_id, path (can be regex) or tag(s)"""
        # Nones are set to .* in the regexp matching
        # get the matching holdings first, these match all but the path
        if holding_label:
            holding_search = holding_label
        else:
            holding_search = ".*"
        holding = self.get_holding(
            user, group, holding_search, holding_id, tag
        )

        if path:
            search_path = path
        else:
            search_path = ".*"

        # (permissions have been checked by get_holding)
        file_list = []
        try:
            for h in holding:
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

        except (IntegrityError, KeyError):
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
            self.commit_required = True    # indicate a commit at end of session            
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"File with original path {original_path} could not be added to"
                 " the database"
            )
        return new_file


    def get_location(self, 
                     file: File, 
                     storage_type: Enum) -> Location:
        """Get a storage location for a file, given the file and the storage
        type"""
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
            self.commit_required = True    # indicate a commit at end of session
        except (IntegrityError, KeyError):
            raise CatalogError(
                f"Location with root {root}, path {file.original_path} and "
                f"storage type {Storage.OBJECT_STORAGE} could not be added to "
                 "the database")
            return None
        return location

