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
            "db_passwd" : ""
            "echo
        },
        "logging":{
            "enable": true
        }
"""

import json
import os

# Typing imports
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

# SQLalchemy imports
from sqlalchemy import create_engine, select, func
from sqlalchemy.exc import ArgumentError, IntegrityError
from sqlalchemy.orm import Session

from datetime import datetime, timezone

from nlds.rabbit.consumer import RabbitMQConsumer
from nlds_processors.catalog_models import Base, File, Holding, Location, Transaction
from nlds_processors.catalog_models import Storage, Checksum, Tag
from nlds.details import PathDetails, PathType

class CatalogConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "catalog_q"
    DEFAULT_ROUTING_KEY = (f"{RabbitMQConsumer.RK_ROOT}."
                           f"{RabbitMQConsumer.RK_CATALOG}."
                           f"{RabbitMQConsumer.RK_WILD}")

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


    def _get_db_string(self):
        # create the connection string with the engine
        db_connect = self.db_engine + "://"
        # add user if defined
        if len(self.db_options[self._DB_OPTIONS_USER]) > 0:
            db_connect += self.db_options[self._DB_OPTIONS_USER]
            # add password if defined
            if len(self.db_options[self._DB_OPTIONS_PASSWD]) > 0:
                db_connect += ":" + self.db_options[self._DB_OPTIONS_PASSWD]
            # add @ symbol
            db_connect += "@"
        # add the database name
        db_connect += self.db_options[self._DB_OPTIONS_DB_NAME]
        
        return db_connect


    def _connect_to_db(self):
        # connect to the database using the information in the config
        # Load config options or fall back to default values.
        self.db_engine = self.load_config_value(
            self._DB_ENGINE
        )

        self.db_options = self.load_config_value(
            self._DB_OPTIONS
        )

        # get the database connection string
        db_connect = self._get_db_string()
        self.log(f"db_connect string is {db_connect}", self.RK_LOG_DEBUG)

        # indicate database not connected yet
        self.db_engine = None

        # connect to the database
        try:
            self.db_engine  = create_engine(
                                db_connect, 
                                echo=self.db_options[self._DB_ECHO],
                                future=True
                            )
        except ArgumentError as e:
            self.log("Could not create database.", self.RK_LOG_CRITICAL)
            raise e

    
    def _create_db(self):
        """Create the database"""
        Base.metadata.create_all(self.db_engine)


    def _get_or_create_holding(self, session, user, group, label, holding_id=None) -> None:
        # create or return an existing Holding
        # check if holding id already exists as a holding
        # if it doesn't then check for the label, and if that doesn't exist then create
        # the holding
        try:
            if holding_id:
                holding_Q = session.execute(
                    select(Holding).where(
                        Holding.id == holding_id
                    )
                )
            else:
                holding_Q = session.execute(
                    select(Holding).where(
                        Holding.label == label
                    )
                )
            # if it doesn't then create
            holding = holding_Q.fetchone()
            if holding is None:
                # create the new Holding with the transaction id
                holding = Holding(
                    label = label,
                    user = user,
                    group = group
                )
                session.add(holding)
            else:
                holding = holding.Holding

            # need to flush to update the holding id
            session.flush()
        except IntegrityError:
            pass
        return holding

    def _create_transaction(self, session, holding, transaction_id):
        # create a transaction that belongs to a holding and will contain files
        transaction = Transaction(
            holding_id = holding.id,
            transaction_id = transaction_id,
            ingest_time = func.now()
        )
        session.add(transaction)
        # flush to generate transaction.id
        session.flush()
        return transaction

    def _catalog_add(self, session, transaction, path_details) -> None:
        # add to the catalog database
        # add the file
        new_file = File(
            transaction_id = transaction.id,
            original_path = path_details.original_path,
            path_type = path_details.path_type,
            link_path = path_details.link_path,
            size = int(path_details.size * 1000),
            user = path_details.user,
            group = path_details.group,
            file_permissions = path_details.permissions
        )
        session.add(new_file)
        session.flush()

        # add the storage location for object storage
        location = Location(
            storage_type = Storage.OBJECT_STORAGE,
            # root is bucket for Object Storage and that is the transaction id
            # which is now stored in the Holding record
            root = transaction.transaction_id,
            # path is object_name for object storage
            path = path_details.object_name,
            # access time is passed in the file details
            access_time = datetime.fromtimestamp(
                path_details.access_time, tz=timezone.utc
            ),
            file_id = new_file.id
        )
        session.add(location)


    def _getfilefromdb(self, session, file_details: PathDetails,
                       holding=None) -> PathDetails:
        # get a file from the db connected to via session
        print("!!!!", holding.id, holding.label)
        if holding:
            file_Q = session.execute(
                select(Transaction, File).where(
                    Transaction.holding_id == holding.id,
                    File.transaction_id == Transaction.id,
                    File.original_path == file_details.original_path,
                )
            )
        else:
            file_Q = session.execute(
                select(File).where(
                    File.original_path == file_details.original_path
                )
            )
        # can currently have more than one file with the same name so just
        # get the first
        file = file_Q.fetchone()
        # check file exists
        if file is not None:
            # get the object storage location for this file
            location_Q = session.execute(
                select(Location).where(
                    Location.file_id == file.File.id,
                    Location.storage_type == Storage.OBJECT_STORAGE
                )
            )
            # again, can (in theory) have more than one, but just fetch the first
            location = location_Q.fetchone()
            # check that the file location exists
            if location is not None:
                object_name = ("nlds." +
                               location.Location.root + ":" + 
                               location.Location.path)
                access_time = location.Location.access_time.timestamp()
                # create a new PathDetails with all the info from the DB
                new_pd = PathDetails(
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
                return new_pd
            else:
                # otherwise indicate failed files 
                raise KeyError(f"File record: {file_details.original_path} "
                                "does not contain a Location")
        else:
            # add to failed files
            raise KeyError(f"File record: {file_details.original_path} "
                            "not found")

    def _getholding(self, session, holding_label):
        # get the Holding from the holding_transaction_id
        holding = session.execute(
            select(Holding).where(
                Holding.label == holding_label
            )
        ).first()

        # check it's in the DB
        if holding is None:
            self.log(
                "Holding label is not in database, exiting callback.", 
                self.RK_LOG_ERROR
            )
            return
        
        return holding.Holding

    def _catalog_get(self, body: dict) -> None:
        # get the filelist from the data section of the message
        try:
            filelist = body[self.MSG_DATA][self.MSG_FILELIST]
        except KeyError as e:
            self.log(f"Invalid message contents, filelist should be in the data"
                     f"section of the message body.",
                     self.RK_LOG_ERROR)
            return

        # create a SQL alchemy session
        session = Session(self.db_engine)

        # get the holding label from the details section of the message
        # could (legit) be None
        try: 
            holding_label = body[self.MSG_META][self.MSG_LABEL]
        except KeyError:
            holding_label = None

        if holding_label is not None:
            holding = self._getholding(session, holding_label)
        else:
            holding = None

        # build the Pathlist from each file
        # two lists: completed PathDetails, failed PathDetails
        complete_pathlist = []
        failed_pathlist = []
        for f in filelist:
            file_details = PathDetails.from_dict(f)
            try:
                complete_details = self._getfilefromdb(
                    session, file_details, holding
                )
                complete_pathlist.append(complete_details)
            except KeyError:
                failed_pathlist.append(file_details)

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

        # create a SQL alchemy session
        session = Session(self.db_engine)
        # get or create the Holding
        holding = self._get_or_create_holding(session, user, group, label, 
                                              holding_id)
        # create the transaction
        transaction = self._create_transaction(session, holding, transaction_id)

        # loop over the filelist
        for f in filelist:
            # convert to PathDetails class
            pd = PathDetails.from_dict(f)
            self._catalog_add(session, transaction, pd)

        session.commit()


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
            self._catalog_get(body)
        elif (rk_parts[1] == self.RK_CATALOG_PUT):
            self._catalog_put(body)

def main():
    consumer = CatalogConsumer()
    # connect to message queue early so that we can send logging messages about
    # connecting to the database
    consumer.get_connection()
    # connect to the DB
    consumer._connect_to_db()
    # create the database
    consumer._create_db()
    # run the loop
    consumer.run()

if __name__ == "__main__":
    main()