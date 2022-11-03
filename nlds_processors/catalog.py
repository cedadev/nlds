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
import os
import uuid

# Typing imports
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

# SQLalchemy imports
from sqlalchemy import create_engine, select, func, Enum
from sqlalchemy.exc import ArgumentError, IntegrityError
from sqlalchemy.orm import Session

from datetime import datetime, timezone

from nlds.rabbit.consumer import RabbitMQConsumer as RMQC
from nlds_processors.catalog_models import Base, File, Holding, Location, Transaction
from nlds_processors.catalog_models import Storage, Checksum, Tag
from nlds.details import PathDetails, PathType

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
        if len(self.db_options[CatalogConsumer._DB_OPTIONS_USER]) > 0:
            db_connect += self.db_options[CatalogConsumer._DB_OPTIONS_USER]
            # add password if defined
            if len(self.db_options[CatalogConsumer._DB_OPTIONS_PASSWD]) > 0:
                db_connect += ":" + self.db_options[CatalogConsumer._DB_OPTIONS_PASSWD]
            # add @ symbol
            db_connect += "@"
        # add the database name
        db_connect += self.db_options[CatalogConsumer._DB_OPTIONS_DB_NAME]
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
                                echo=self.db_options[CatalogConsumer._DB_ECHO],
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


    def end_session(self):
        """Finish and commit a SQL alchemy session"""
        self.session.commit()
        self.session = None


    def get_holding(self, user, group, label: str, holding_id: int=None) -> object:
        """Get a holding from the database"""
        try:
            if holding_id:
                holding_Q = self.session.execute(
                    select(Holding).where(
                        Holding.id == holding_id
                    )
                )
            else:
                holding_Q = self.session.execute(
                    select(Holding).where(
                        Holding.label == label
                    )
                )
            # should we throw an error here if there is more than one holding
            # returned?
            holding = holding_Q.fetchone()
        except (IntegrityError, KeyError) as e:
            if holding_id:
                raise CatalogException(f"Holding with holding_id {holding_id} not found")
            else:
                raise CatalogException(f"Holding with label {label} not found")

        return holding.Holding


    def create_holding(self, user: str, group: str, label: str) -> object:
        """Create the new Holding with the label, user, group"""
        try:
            holding = Holding(
                label = label, 
                user = user, 
                group = group
            )
            self.session.add(holding)
            self.session.flush() # update holding.id
        except (IntegrityError, KeyError) as e:
            raise CatalogException(
                f"Holding with label {label} could not be added to the database"
            )
        return holding


    def get_transaction(self, transaction_id: str) -> object:
        """Get a transaction from the database"""
        try:
            transaction_Q = self.session.execute(
                select(Transaction).where(
                    Transaction.transaction_id == label
                )
            )
            # should we throw an error here if there is more than one holding
            # returned?
            transaction = transaction_Q.fetchone()            
        except (IntegrityError, KeyError) as e:
            raise CatalogException(
                f"Transaction with transaction_id {transaction_id} not found"
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
            # flush to generate transaction.id
            self.session.flush()
        except (IntegrityError, KeyError) as e:
            raise CatalogException(
                f"Transaction with transaction_id {transaction_id} could not "
                "be added to the database"
            )
        return transaction


    def get_file(self, original_path: str, holding = None):
        """Get file details from the database, given the original path of the file.  
        An optional holding can be supplied to get the file details from a
        particular holding - e.g. with a holding label, or tags"""
        try:
            if holding:
                file_Q = self.session.execute(
                    select(File).where(
                        Transaction.holding_id == holding.id,
                        File.transaction_id == Transaction.id,
                        File.original_path == original_path,
                    )
                )
            else:
                file_Q = self.session.execute(
                    select(File).where(
                        File.original_path == original_path
                    )
                )
            file = file_Q.fetchone()
        except:
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
        """Create a file that belongs to a transaction and will contain locations"""
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
            self.session.flush()
        except (IntegrityError, KeyError) as e:
            raise CatalogException(
                f"File with original path {original_path} could not be added to "
                 "the database"
            )
        return new_file


    def get_location(self, file: object, storage_type: Enum):
        """Get a storage location for a file, given the file and the storage
        type"""
        try:
            # get the object storage location for this file
            location_Q = self.session.execute(
                select(Location).where(
                    Location.file_id == file.id,
                    Location.storage_type == storage_type
                )
            )
            # again, can (in theory) have more than one, but just fetch the first
            location = location_Q.fetchone()
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
                # root is bucket for Object Storage and that is the transaction id
                # which is now stored in the Holding record
                root = root,
                # path is object_name for object storage
                path = object_name,
                # access time is passed in the file details
                access_time = access_time,
                file_id = file.id
            )
            self.session.add(location)
            self.session.flush()
        except (IntegrityError, KeyError) as e:
            self.logger(f"Location with root {root}, path {original_path} and "
                        f"storage type {Storage.OBJECT_STORAGE} could not be "
                         "added to the database",
                        RMQC.RK_LOG_ERROR)
            return None
        return location


class CatalogConsumer(RMQC):
    DEFAULT_QUEUE_NAME = "catalog_q"
    DEFAULT_ROUTING_KEY = (f"{RMQC.RK_ROOT}.{RMQC.RK_CATALOG}.{RMQC.RK_WILD}")

    # Possible options to set in config file
    _DB_ENGINE = "db_engine"
    _DB_OPTIONS = "db_options"
    _DB_OPTIONS_DB_NAME = "db_name"
    _DB_OPTIONS_USER = "db_user"
    _DB_OPTIONS_PASSWD = "db_passwd"
    _DB_ECHO = "echo"

    DEFAULT_CONSUMER_CONFIG = {
        _DB_ENGINE: "sqlite",
        _DB_OPTIONS: {
            _DB_OPTIONS_DB_NAME: "/nlds_catalog.db",
            _DB_OPTIONS_USER: "",
            _DB_OPTIONS_PASSWD: "",
            _DB_ECHO: True,
        },
    }


    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)


    def _catalog_get(self, body: dict) -> None:
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

        # start the database transactions
        self.catalog.start_session()

        # get the holding from the database
        if holding_label is None:
            holding = None
        else:
            try:
                holding = self.catalog.get_holding(
                    user, group, holding_label, holding_id
                )
            except CatalogException as e:
                self.logger(e.message, RMQC.RK_LOG_ERROR)
                return

        # build the Pathlist from each file
        # two lists: completed PathDetails, failed PathDetails
        complete_pathlist = []
        failed_pathlist = []
        for f in filelist:
            file_details = PathDetails.from_dict(f)
            try:
                # get the file first
                file = self.catalog.get_file(
                    file_details.original_path, holding
                )
                if file is None:
                    raise CatalogException(
                        f"Could not find file with original path "
                        f"{file_details.original_path}"
                    )
                # now get the location so we can get where it is stored
                try:
                    location = self.catalog.get_location(
                        file.File, Storage.OBJECT_STORAGE
                    )
                    if location is None:
                        raise CatalogException(
                            f"Could not find location for file with original path "
                            f"{file_details.original_path}"
                        )         
                    object_name = ("nlds." +
                                location.Location.root + ":" + 
                                location.Location.path)
                    access_time = location.Location.access_time.timestamp()
                    # create a new PathDetails with all the info from the DB
                    new_file = PathDetails(
                        original_path = file.File.original_path,
                        object_name = object_name,
                        size = file.File.size,
                        user = file.File.user,
                        group = file.File.group,
                        permissions = file.File.file_permissions,                    
                        access_time = access_time,
                        path_type = file.File.path_type,
                        link_path = file.File.link_path
                    )
                except CatalogException as e:
                    self.log(e.message, self.RK_LOG_ERROR)
                    failed_pathlist.append(file_details)
                    continue
                complete_pathlist.append(new_file)

            except CatalogException as e:
                self.log(e.message, self.RK_LOG_ERROR)
                failed_pathlist.append(file_details)
                continue

        # send the succeeded and failed messages back to the NLDS worker Q
        # SUCCESS
        if len(complete_pathlist) > 0:
            rk_complete = ".".join([self.RK_ROOT,
                                    self.RK_CATALOG_GET, 
                                    self.RK_COMPLETE])
            self.log(
                f"Sending completed PathList from CATALOG_GET {complete_pathlist}",
                self.RK_LOG_DEBUG
            )
            body[self.MSG_DATA][self.MSG_FILELIST] = complete_pathlist
            self.publish_message(rk_complete, json.dumps(body))

        # FAILED
        if len(failed_pathlist) > 0:
            rk_failed = ".".join([self.RK_ROOT,
                                  self.RK_CATALOG_GET, 
                                  self.RK_FAILED])
            self.log(
                f"Sending failed PathList from CATALOG_GET {failed_pathlist}",
                self.RK_LOG_DEBUG
            )
            body[self.MSG_DATA][self.MSG_FILELIST] = failed_pathlist
            self.publish_message(rk_failed, json.dumps(body))
        # stop db transistions and commit
        self.catalog.end_session()


    def _catalog_list(self, body: dict) -> None:
        pass


    def _catalog_put(self, body: dict) -> None:
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
        except KeyError:
            # generate the label from the UUID - should loop here to make sure 
            # we get a unique label
            label = transaction_id[0:8]

        # get the holding_id from the metadata section of the message
        try:
            holding_id = body[self.MSG_META][self.MSG_HOLDING_ID]
        except KeyError:
            holding_id = None

        # start the database transactions
        self.catalog.start_session()

        # try to get the holding to see if it already exists and can be added to
        try:
            holding = self.catalog.get_holding(user, group, label, holding_id)
        except CatalogException:
            pass

        if holding is None:
            try:
                holding = self.catalog.create_holding(user, group, label)
            except CatalogException as e:
                self.logger(e.message, RMQC.RK_LOG_ERROR)
                return

        # create the transaction within the  holding - check for error on return
        try:
            transaction = self.catalog.create_transaction(
                holding, 
                transaction_id
            )
        except CatalogException:
            self.logger(e.message, RMQC.RK_LOG_ERROR)
            return

        complete_pathlist = []
        failed_pathlist = []
        # loop over the filelist
        for f in filelist:
            # convert to PathDetails class
            pd = PathDetails.from_dict(f)
            try:
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
                location = self.catalog.create_location(
                    file,
                    Storage.OBJECT_STORAGE,
                    transaction.transaction_id,
                    pd.object_name,
                    # access time is passed in the file details
                    access_time = datetime.fromtimestamp(
                        pd.access_time, tz=timezone.utc
                    )
                )
                complete_pathlist.append(pd)
            except CatalogException as e:
                failed_pathlist.append(pd)
                self.log(e.message, RMQC.RK_LOG_ERROR)
                continue
        # stop db transistions and commit
        self.catalog.end_session()


    def attach_catalog(self):
        """Attach the Catalog to the consumer"""
        # Load config options or fall back to default values.
        db_engine = self.load_config_value(
            self._DB_ENGINE
        )

        db_options = self.load_config_value(
            self._DB_OPTIONS
        )
        self.catalog = Catalog(db_engine, db_options)
        try:
            db_connect = self.catalog.connect()
            self.log(f"db_connect string is {db_connect}", RMQC.RK_LOG_DEBUG)
        except CatalogException as e:
            self.log(e.message, RMQC.RK_LOG_CRITICAL)


    def callback(self, ch: Channel, method: Method, properties: Header, 
                 body: bytes, connection: Connection) -> None:
        # Connect to database if not connected yet                
        # Convert body from bytes to json for ease of manipulation
        body = json.loads(body)

        self.log(f"Received {json.dumps(body, indent=4)} from "
                 f"{self.queues[0].name} ({method.routing_key})", 
                 self.RK_LOG_INFO)

        # Verify routing key is appropriate
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError as e:
            self.log("Routing key inappropriate length, exiting callback.", 
                     self.RK_LOG_ERROR)
            return

        # check whether this is a GET or a PUT
        if (rk_parts[1] == self.RK_CATALOG_GET):
            if (rk_parts[2] == self.RK_START): # this is part of the GET workflow
                self._catalog_get(body)
            elif (rk_parts[2] == self.RK_LIST): # this is part of the query workflow
                print("LIST")
                self._catalog_list(body)
        elif (rk_parts[1] == self.RK_CATALOG_PUT):
            self._catalog_put(body) # this is the only workflow for this


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