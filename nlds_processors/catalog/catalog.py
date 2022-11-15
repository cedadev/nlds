# SQLalchemy imports
from sqlalchemy import create_engine, func, Enum
from sqlalchemy.exc import ArgumentError, IntegrityError
from sqlalchemy.orm import Session

from nlds_processors.catalog.catalog_models import Base, File, Holding,\
     Location, Transaction, Storage, Checksum, Tag

class CatalogException(Exception):
    def __init__(self, message, *args):
        super().__init__(args)
        self.message = message

class Catalog():
    """Catalog object containing methods to manipulate the Catalog Database"""

    def __init__(self, db_engine: str, db_options: str):
        """Create a catalog engine from the config strings passed in"""
        self.db_engine = db_engine
        self.db_options = db_options


    def _get_db_string(self):
        # create the connection string with the engine
        db_connect = self.db_engine + "://"
        # add user if defined
        if len(self.db_options["db_user"]) > 0:
            db_connect += self.db_options["db_user"]
            # add password if defined
            if len(self.db_options["db_passwd"]) > 0:
                db_connect += ":" + self.db_options["db_passwd"]
            # add @ symbol
            db_connect += "@"
        # add the database name
        db_connect += self.db_options["db_name"]
        return db_connect


    def connect(self):
        # connect to the database using the information in the config
        # get the database connection string
        db_connect = self._get_db_string()

        # indicate database not connected yet
        self.db_engine = None

        # connect to the database
        try:
            self.db_engine  = create_engine(
                                db_connect, 
                                echo=self.db_options["echo"],
                                future=True
                            )
        except ArgumentError as e:
            raise CatalogException("Could not create database engine")

        # create the db if not already created
        try:
            Base.metadata.create_all(self.db_engine)
        except IntegrityError as e:
            raise CatalogException("Could not create database tables")
        # return db_connect string to log
        return db_connect


    def start_session(self):
        """Create a SQL alchemy session"""
        self.session = Session(self.db_engine)
        self.commit_required = False


    def end_session(self):
        """Finish and commit a SQL alchemy session"""
        if self.commit_required:
            self.session.commit()
        self.commit_required = False
        self.session = None


    def get_holding(self, user: str, group: str, 
                    label: str, holding_id: int=None) -> object:
        """Get a holding from the database"""
        try:
            if holding_id:
                holding = self.session.query(Holding).filter(
                    Holding.user == user,
                    Holding.group == group,
                    Holding.id == holding_id
                ).one_or_none()
            else:
                holding = self.session.query(Holding).filter(
                    Holding.user == user,
                    Holding.group == group,
                    Holding.label.like(label)
                ).one_or_none()
            # should we throw an error here if there is more than one holding
            # returned?
        except (IntegrityError, KeyError) as e:
            if holding_id:
                raise CatalogException(
                    f"Holding with holding_id:{holding_id} not found for "
                    f"user:{user} and group:{group}."
                )
            else:
                raise CatalogException(
                    f"Holding with label:{label} not found for "
                    f"user:{user} and group:{group}."
                )

        return holding


    def get_holdings(self, user: str, group: str,
                     tags: dict=None) -> object:
        """Plural version of get holding, where a specific label or id is NOT
        given"""
        try:
            if tags:
                pass
            else:
                holdings = self.session.query(Holding).filter(
                    Holding.user == user,
                    Holding.group == group                    
                ).all()
        except (IntegrityError, KeyError) as e:
            pass
        return holdings

    def create_holding(self, user: str, group: str, label: str) -> object:
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
            raise CatalogException(
                f"Holding with label {label} could not be added to the database."
            )
        return holding


    def get_transaction(self, transaction_id: str) -> object:
        """Get a transaction from the database"""
        try:
            transaction = self.session.query(Transaction).filter(
                Transaction.transaction_id == transaction_id
            ).one_or_none()
        except (IntegrityError, KeyError) as e:
            raise CatalogException(
                f"Transaction with transaction_id {transaction_id} not found."
            )
        return transaction


    def create_transaction(self, holding: object, transaction_id: str) -> object:
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
        except (IntegrityError, KeyError) as e:
            raise CatalogException(
                f"Transaction with transaction_id {transaction_id} could not "
                "be added to the database"
            )
        return transaction


    def get_file(self, original_path: str, holding = None):
        """Get file details from the database, given the original path of the 
        file.  
        An optional holding can be supplied to get the file details from a
        particular holding - e.g. with a holding label, or tags"""
        try:
            if holding:
                file = self.session.query(File).filter(
                    Transaction.id == File.transaction_id,
                    Transaction.holding_id == holding.id,
                    File.original_path == original_path,
                ).one_or_none()
            else:
                # if no holding given then we want to return the most recent
                # file with this original path
                file = self.session.query(File).filter(
                    File.original_path == original_path,
                    Transaction.id == File.transaction_id
                ).order_by(Transaction.ingest_time.desc()).first()
        except:  # which exceptions???
            if holding:
                err_msg = (f"File with original path {original_path} not found "
                        f"in holding {holding.label}")
            else:
                err_msg = f"File with original path {original_path} not found"
            raise CatalogException(err_msg)
        return file


    def create_file(self, 
                    transaction: object, 
                    user: str = None,
                    group: str = None,
                    original_path: str = None,
                    path_type: str = None,
                    link_path: str = None,
                    size: str = None,
                    file_permissions: str = None) -> object:
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
        except (IntegrityError, KeyError) as e:
            raise CatalogException(
                f"File with original path {original_path} could not be added to"
                 " the database"
            )
        return new_file


    def get_location(self, file: object, storage_type: Enum):
        """Get a storage location for a file, given the file and the storage
        type"""
        try:
            location = self.session.query(Location).filter(
                Location.file_id == file.id,
                Location.storage_type == storage_type
            ).one_or_none()
        except (IntegrityError, KeyError) as e:
            raise CatalogException(            
                f"Location of storage type {storage_type} not found for file "
                f"{file.original_path}"
            )
        return location


    def create_location(self, 
                        file,
                        storage_type: Enum,
                        root: str,
                        object_name: str, 
                        access_time: float) -> object:
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
        except (IntegrityError, KeyError) as e:
            raise CatalogException(
                f"Location with root {root}, path {file.original_path} and "
                f"storage type {Storage.OBJECT_STORAGE} could not be added to "
                 "the database")
            return None
        return location

