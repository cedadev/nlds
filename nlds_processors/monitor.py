"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '15 Sep 2022'
__copyright__ = 'Copyright 2022 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

"""
Requires these settings in the /etc/nlds/server_config file:

    "monitor_q":{
        "db_engine": "sqlite",
        "db_options": {
            "db_name" : "/nlds_monitor.db",
            "db_user" : "",
            "db_passwd" : "",
            "echo": true
        },
        "logging":{
            "enable": true
        }
"""
import json
from typing import Dict

from sqlalchemy import create_engine, select
from sqlalchemy.exc import ArgumentError, IntegrityError
from sqlalchemy.orm import Session
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

from nlds.rabbit.consumer import RabbitMQConsumer
from nlds.details import PathDetails
from nlds_processors.monitor_models import Base, State, TransactionRecord
from nlds_processors.monitor_models import SubRecord, FailedFile

class MonitorError(Exception):
    def __init__(self, message, *args):
        super().__init__(args)
        self.message = message

class MonitorConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "monitor_q"
    DEFAULT_ROUTING_KEY = (f"{RabbitMQConsumer.RK_ROOT}."
                           f"{RabbitMQConsumer.RK_MONITOR}."
                           f"{RabbitMQConsumer.RK_WILD}")
    DEFAULT_REROUTING_INFO = f"->MONITOR_Q"

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
            _DB_OPTIONS_DB_NAME: "/nlds_monitor.db",
            _DB_OPTIONS_USER: "",
            _DB_OPTIONS_PASSWD: "",
            _DB_ECHO: True,
        },
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
        self.db_engine = self.load_config_value(self._DB_ENGINE)
        self.db_options = self.load_config_value(self._DB_OPTIONS)
    
    def _get_db_string(self):
        """NOTE: This is copied verbatim from catalog, should be refactored into 
        a mixin
        """
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
        """NOTE: This is copied verbatim from catalog, could maybe be refactored 
        into a mixin
        """

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

    def _create_transaction_record(
            self,
            transaction_id: str, 
            user: str,
            group: str,
            session: Session = None,
        ) -> TransactionRecord:
        """Creates a transaction_record with the minimum required input. 
        Optionally adds to a session and flushes to get the id field populated. 
        """
        transaction_record = TransactionRecord(
            transaction_id = transaction_id, 
            user = user,
            group = group, 
        )
        if session:
            session.add(transaction_record)
            # need to flush to update the transaction_record.id
            session.flush()
        return transaction_record
    
    def _create_sub_record(
            self,
            sub_id: str, 
            transaction_record_id: int, 
            state: State = None,
            session: Session = None,
        ) -> SubRecord:
        """Creates a SubRecord with the minimum required input. Optionally adds 
        to a session and flushes to get the id field populated. 
        """
        if state is None:
            # Set to first/default value
            state = State.ROUTING
        sub_record = SubRecord(
            sub_id = sub_id, 
            state = state,
            retry_count = 0,
            transaction_record_id = transaction_record_id,
        )
        if session:
            session.add(sub_record)
            # need to flush to update the transaction_record.id
            session.flush()
        return sub_record

    def _create_failed_file(
            self, 
            sub_record: SubRecord, 
            path_details: PathDetails,
            session: Session = None,
        ) -> FailedFile:
        failed_file = FailedFile(
            filepath=path_details.original_path,
            reason=path_details.retry_reasons[-1],
            sub_record_id=sub_record.id,
        )
        if session: 
            session.add(failed_file)
            session.flush()
        return failed_file
    
    def _get_transaction_record(
            self, session: Session, transaction_id: str, user: str, group: str,
            create_fl: bool = True
        ) -> TransactionRecord:
        """Gets a TransactionRecord from the DB from the given transaction_id or 
        creates one if not present.
        """
        try:
            tr_query = session.execute(
                select(TransactionRecord).where(
                    TransactionRecord.transaction_id == transaction_id
                )
            )
            # Should only be one item in the query as transaction_id should be 
            # unique
            transaction_record = tr_query.fetchone()

            # if nothing in the database then create a new row
            if transaction_record is None and create_fl:
                # create a new, minimum TransactionRecord with the transaction_id 
                # and user info passed through arguments
                transaction_record = self._create_transaction_record(
                    transaction_id, user, group, session=session
                )
            elif transaction_record is not None:
                transaction_record = transaction_record.TransactionRecord
            else: 
                transaction_record = None

        except IntegrityError:
            self.log("IntegrityError raised when attempting to get/create "
                     "transaction_record", self.RK_LOG_WARNING)
        return transaction_record
    
    def _get_sub_record(self, session: Session, sub_id: str, 
                        transaction_record: TransactionRecord, 
                        create_fl: bool = True):
        try:
            # Get subrecord by sub_id
            # TODO (2022-11-03) is it worth also filtering by transaction_id at 
            # this point? sub_id _should_ be unique so probably not necessary.
            sr_query = session.execute(
                select(SubRecord).where(SubRecord.sub_id == sub_id)
            )
            # Should only be one item in the query as sub_id should be unique
            sub_record = sr_query.fetchone()

            # if nothing in the database then create a new row
            if sub_record is None and create_fl:
                # create a new, minimum SubRecord with the sub_id, transaction_id 
                # and state (if passed?)
                sub_record = self._create_sub_record(
                    sub_id, 
                    transaction_record_id=transaction_record.id, 
                    session=session,
                )
            elif sub_record is not None:
                sub_record = sub_record.SubRecord
            else: 
                sub_record = None

        except IntegrityError:
            self.log("IntegrityError raised when attempting to get/create "
                     "sub_record", self.RK_LOG_WARNING)
        return sub_record

    def _update_sub_record(
            self, session: Session, sub_record: SubRecord, new_state: State
        ) -> None:
        """Update a retrieved SubRecord to reflect the new monitoring info. 
        Furthest state is updated, if required, and the retry count is 
        incremented by one if appropriate.
        TODO: Should retrying be a flag instead of a separate state? Probably, 
        yes
        """
        # Upgrade furthest_state to new_state, throw exception if regressing 
        # state (unless changing from a retry)
        if (new_state.value <= sub_record.state.value 
                and sub_record.state != State.RETRYING):
            raise MonitorError(f"Monitoring state cannot go backwards. "
                               f"Attempted {sub_record.state}->{new_state}")
        sub_record.state = new_state
        # Increment retry counter if appropriate
        if new_state == State.RETRYING:
            sub_record.retry_count = (
                SubRecord.retry_count + 1
            )
        elif new_state in State:
            self.log(f"Monitoring response not defined for state {new_state}.",
                     self.RK_LOG_DEBUG)
        else:
            # If state not in recognised list then something has gone wrong.
            # NOTE: This is probably caught earlier than this?
            session.close()
            raise ValueError(f"Invalid state {new_state} passed to monitor, "
                             "exiting callback.")
        session.add(sub_record)

    def _monitor_put(self, body: Dict[str, str]) -> None:
        """
        Create or update a monitoring record for an in-progress transaction. 
        """
        # get the transaction id from the details section of the message
        try: 
            transaction_id = body[self.MSG_DETAILS][self.MSG_TRANSACT_ID]
        except KeyError:
            self.log("Transaction id not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return

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
            self.log(f"Filelist field must contain a list", self.RK_LOG_ERROR)
            return

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
        
        # get the state from the details section of the message
        try:
            state = body[self.MSG_DETAILS][self.MSG_STATE]
            # Convert state to an actual ENUM value for ease of comparison, can 
            # either be passed as the enum.value (default) or as the state 
            # string
            if State.has_value(state):
                state = State(state)
            elif State.has_name(state):
                state = State[state]
            else:
                self.log("State found in message invalid, exiting callback.", 
                        self.RK_LOG_ERROR)
                return
        except KeyError:
            self.log("Required state not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return
        
        # get the transaction id from the details section of the message
        try: 
            sub_id = body[self.MSG_DETAILS][self.MSG_SUB_ID]
        except KeyError:
            self.log("Transaction sub-id not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return

        # create a SQL alchemy session
        session = Session(self.db_engine)

        # For any given monitoring update, we need to: 
        # - find the transaction record (create if not present)
        # - update the subrecord(s) associated with it
        #   - find an exisiting
        #   - see if it matches sub_id in message
        #       - update it if it does
        #           - change state
        #           - update retry count if retrying
        #           - add failed files if failed
        #       - create a new one if it doesn't
        #   - open question whether we delete the older subrecords (i.e. before
        #     a split)
        transaction_record = self._get_transaction_record(
            session, transaction_id, user, group
        )
        sub_record = self._get_sub_record(session, sub_id, transaction_record)
        if sub_record.transaction_record_id != transaction_record.id:
            self.log("Something has gone terribly wrong.", self.RK_LOG_ERROR)
            return
        # Update subrecord to match new monitoring data
        self._update_sub_record(session, sub_record, state)
        # Create failed_files if necessary
        if state == State.FAILED:
            for f in filelist:
                path_details = PathDetails.from_dict(f)
                self._create_failed_file(sub_record, path_details, 
                                         session=session)
        # Commit all transactions when we're sure everything is as it should be. 
        session.commit()
        self.log(f"Successfully commited monitoring update for transaction "
                 f"{transaction_id}, sub_record {sub_id}.", self.RK_LOG_INFO)

    def _monitor_get(body: Dict[str, str]) -> None:
        """
        Create or update a monitoring record for an in-progress transaction.
        NOTE: This might be sensible to move into a separate consumer so we can 
        scale out database reads separately from database writes - which at the 
        moment need to be done one at a time to avoid 
        """
        pass

    def callback(self,ch: Channel, method: Method, properties: Header, 
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
        if (rk_parts[1] == self.RK_MONITOR_GET):
            self.log("Starting get from monitoring db.", self.RK_LOG_INFO)
            self._monitor_get(body)
        elif (rk_parts[1] == self.RK_MONITOR_PUT):
            self.log("Starting put into monitoring db.", self.RK_LOG_INFO)
            self._monitor_put(body)
        else:
            self.log("Routing key did not specify a monitoring task.", 
                     self.RK_LOG_INFO)
        
        self.log("Callback complete!", self.RK_LOG_INFO)

def main():
    consumer = MonitorConsumer()
    # connect to message queue early so that we can send logging messages about
    # connecting to the database
    consumer.get_connection()
    # connect to the DB
    consumer._connect_to_db()
    # create the database
    consumer._create_db()
    consumer.run()

if __name__ == "__main__":
    main()