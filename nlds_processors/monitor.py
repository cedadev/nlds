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

from sqlalchemy import create_engine, select, func
from sqlalchemy.exc import ArgumentError, IntegrityError
from sqlalchemy.orm import Session
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

from nlds.rabbit.consumer import RabbitMQConsumer
from nlds_processors.monitor_models import Base, State, TransactionState

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
    
    def _get_transaction_state(
            self, session: Session, transaction_id: str, user: str, group: str
        ) -> TransactionState:
        """Gets a TransactionState from the DB from the given transaction_id or 
        creates one if not present.
        """
        try:
            ts_query = session.execute(
                select(TransactionState).where(
                    TransactionState.transaction_id == transaction_id
                )
            )
            # Should only be one item in the query as transaction_id should be 
            # unique
            transaction_state = ts_query.fetchone()

            # if nothing in the database then create a new row
            if transaction_state is None:
                # create a new, minimum TransactionState with the transaction_id 
                # and user info passed through arguments
                transaction_state = TransactionState(
                    transaction_id = transaction_id,
                    user = user,
                    group = group,
                    furthest_state = State.ROUTING,
                    subjob_count = 1,
                    routing_count = 0,
                    indexing_count = 0,
                    transfer_putting_count = 0,
                    catalog_putting_count = 0,
                    complete_count = 0,
                    failed_count = 0,
                    catalog_getting_count = 0,
                    transfer_getting_count = 0,
                    retry_count = 0,
                )
                # Unsure if we add here or later on
                session.add(transaction_state)
            else:
                transaction_state = transaction_state.TransactionState
            # need to flush to update the transaction_state.id?
            session.flush()
        except IntegrityError:
            self.log("IntegrityError raised when attempting to get/create "
                     "holding", self.RK_LOG_WARNING)
        return transaction_state

    def _update_transaction_state(
            self, session: Session, transaction_state: TransactionState, 
            new_state: State, subjob_delta: int = None) -> None:
        """Update a retrieved TransactionState to reflect the new monitoring 
        info. Furthest state is updated, if required, and the appropriate count 
        is incremented by one. The count of subjobs is also updated, can be by 
        more than one but currently design dictates it will be one of 
        {1, 0, -1} as each subjob created will have it's own monitoring update 
        message.
        """
        # Upgrade furthest_state to new_state if further along than current 
        # furthest state
        if transaction_state.furthest_state.value < new_state.value:
            transaction_state.furthest_state = new_state
        # Increment the count of subjobs with the change in subjobs 
        if subjob_delta is not None:
            transaction_state.subjob_count = (TransactionState.subjob_count 
                                              + subjob_delta)
        # Increment the necessary state-specific counter
        if new_state == State.ROUTING:
            transaction_state.routing_count = (
                TransactionState.routing_count + 1
            )
        elif new_state == State.INDEXING:
            transaction_state.indexing_count = (
                TransactionState.indexing_count + 1
            )
        elif new_state == State.TRANSFER_PUTTING:
            transaction_state.transfer_putting_count = (
                TransactionState.transfer_putting_count + 1
            )
        elif new_state == State.CATALOG_PUTTING:
            transaction_state.catalog_putting_count = (
                TransactionState.catalog_putting_count + 1
            )
        elif new_state == State.COMPLETE:
            transaction_state.complete_count = (
                TransactionState.complete_count + 1
            )
        elif new_state == State.FAILED:
            transaction_state.failed_count = (
                TransactionState.failed_count + 1
            )
        # And for the getting workflow
        elif new_state == State.CATALOG_GETTING:
            transaction_state.catalog_getting_count = (
                TransactionState.catalog_getting_count + 1
            )
        elif new_state == State.TRANSFER_GETTING:
            transaction_state.transfer_getting_count = (
                TransactionState.transfer_getting_count + 1
            )
        elif new_state == State.RETRYING:
            transaction_state.retry_count = (
                TransactionState.retry_count + 1
            )
        elif new_state in State._value2member_map_:
            self.log(f"Monitoring response not defined for state {new_state}.",
                     self.RK_LOG_DEBUG)
        else:
            # If state not in recognised list then something has gone wrong.
            session.close()
            raise ValueError("Invalid state passed to monitor, exiting "
                             "callback.")
        session.add(transaction_state)

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
            print(state)
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
        
        subjob_delta = None
        try: 
            subjob_delta = body[self.MSG_DETAILS][self.MSG_SPLIT_COUNT]
        except KeyError:
            self.log("Subjob_delta not provided in message, continuing without "
                     "value set.", self.RK_LOG_INFO)
            

        # create a SQL alchemy session
        session = Session(self.db_engine)

        transaction_state = self._get_transaction_state(
            session, transaction_id, user, group
        )
        self._update_transaction_state(session, transaction_state, state, 
                                       subjob_delta=subjob_delta)

        session.commit()

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