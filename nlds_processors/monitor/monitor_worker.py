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

from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

from nlds.rabbit.consumer import RabbitMQConsumer as RMQC
from nlds.rabbit.consumer import State
from nlds.details import Retries
from nlds_processors.monitor.monitor import Monitor, MonitorError
from nlds_processors.monitor.monitor_models import orm_to_dict
from nlds_processors.db_mixin import DBError


class MonitorConsumer(RMQC):
    DEFAULT_QUEUE_NAME = "monitor_q"
    DEFAULT_ROUTING_KEY = (f"{RMQC.RK_ROOT}."
                           f"{RMQC.RK_MONITOR}."
                           f"{RMQC.RK_WILD}")
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
        self.monitor = None


    @property
    def database(self):
        return self.monitor


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

        filelist = self.parse_filelist(body)

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

        # get the api-action from the details section of the message
        try:
            api_action = body[self.MSG_DETAILS][self.MSG_API_ACTION]
        except KeyError:
            self.log("API-action not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return

        # get the job_label from the details section of the message
        try:
            job_label = body[self.MSG_DETAILS][self.MSG_JOB_LABEL]
        except KeyError:
            job_label = transaction_id[0:8]
        
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
        
        # get the sub_record id from the details section of the message
        try: 
            sub_id = body[self.MSG_DETAILS][self.MSG_SUB_ID]
        except KeyError:
            self.log("Transaction sub-id not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return

        # get the retry_fl from the details section of the message
        retry_fl = False
        try: 
            retry_fl = body[self.MSG_DETAILS][self.MSG_RETRY]
        except KeyError:
            self.log("No retry_fl found in message, assuming false.", 
                     self.RK_LOG_DEBUG)
        
        # Get the transaction-level retry
        try: 
            trans_retries = Retries.from_dict(body)
        except KeyError:
            self.log("No retries found in message, continuing with an empty ",
                     "Retries object.", self.RK_LOG_DEBUG)
            trans_retries = Retries()

        # get the warning(s) from the details section of the message
        try:
            warnings = body[self.MSG_DETAILS][self.MSG_WARNING]                
        except KeyError:
            self.log("No warning found in message, continuing without",
                     self.RK_LOG_DEBUG)
            warnings = []

        # start the database transactions
        self.monitor.start_session()

        # For any given monitoring update, we need to: 
        # - find the transaction record (create if not present)
        # - update the subrecord(s) associated with it
        #   - find an existing
        #   - see if it matches sub_id in message
        #       - update it if it does
        #           - change state
        #           - update retry count if retrying
        #           - reset retry count if now continuing
        #           - add failed files if failed
        #       - create a new one if it doesn't
        #   - open question whether we delete the older subrecords (i.e. before
        #     a split)
        try:
            trec = self.monitor.get_transaction_record(
                user, group, idd=None, transaction_id=transaction_id
            )
        except MonitorError:
            # fine to pass here as if transaction_record is not returned then it
            # will be created in the next step
            trec = None 

        if trec is None or len(trec) == 0:
            try:
                trec = self.monitor.create_transaction_record(
                    user, group, transaction_id, job_label, api_action
                )
            except MonitorError as e:
                self.log(e.message, RMQC.RK_LOG_ERROR)
        else:
            trec = trec[0]

        # create any warnings if there are any
        if warnings and len(warnings) > 0:
            for w in warnings:
                warning = self.monitor.create_warning(trec, w)

        try:
            srec = self.monitor.get_sub_record(sub_id)
        except MonitorError:
            srec = None

        # create the sub rec if not found
        if not srec:
            try:
                srec = self.monitor.create_sub_record(trec, sub_id, state)
            except MonitorError as e:
                self.log(e.message, RMQC.RK_LOG_ERROR)

        # consistency check 
        if srec.transaction_record_id != trec.id:
            self.log("Transaction id does not match sub_record's transaction "
                     "id. Something has gone amiss, rolling back and exiting"
                     "callback.", self.RK_LOG_ERROR)
            return
        
        # Update subrecord to match new monitoring data
        try: 
            self.monitor.update_sub_record(srec, state, retry_fl)
        except MonitorError as e:
            # If the state update is invalid then rollback session and exit 
            # callback
            self.log(e, self.RK_LOG_ERROR)
            # session.rollback() # rollback needed?
            return

        # Create failed_files if necessary
        if state == State.FAILED:
            try:
                # Passing reason as None to create_failed_file will default to 
                # to the last reason in the PathDetails object retries section. 
                reason = None
                for pd in filelist:
                    # Check which was the final reason for failure and store 
                    # that as the failure reason for the FailedFile.
                    if len(trans_retries.reasons) > len(pd.retries.reasons):
                        reason = trans_retries.reasons[-1]
                    self.monitor.create_failed_file(srec, pd, reason=reason)
            except MonitorError as e:
                self.log(e, self.RK_LOG_ERROR)
        
        # If reached the end of a workflow then check for completeness                                          
        if state in State.get_final_states():
            self.log("This sub_record is now in its final state for this "
                     "workflow, now checking if all others have reached a "
                     "final state.", self.RK_LOG_INFO)
            self.monitor.check_completion(trec)

        # Commit all transactions when we're sure everything is as it should be.
        self.monitor.save() 
        self.monitor.end_session()
        self.log(f"Successfully commited monitoring update for transaction "
                 f"{transaction_id}, sub_record {sub_id}.", self.RK_LOG_INFO)


    def _monitor_get(self, body: Dict[str, str], properties: Header) -> None:
        """
        Get a list of monitoring records for in-progress or finished 
        transactions, filtered by flags passed by the user.
        NOTE: This might be sensible to move into a separate consumer so we can 
        scale out database reads separately from database writes - which at the 
        moment need to be done one at a time to avoid 
        """
        # TODO: what do we want to do with files? A list command?
        # filelist = self.parse_filelist(body)

        # NOTE: might not need to check/have these passed?

        # get the desired user id from the details section of the message
        try:
            user = body[self.MSG_DETAILS][self.MSG_USER]
        except KeyError:
            self.log("User not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return

        # get the desired group from the details section of the message
        try:
            group = body[self.MSG_DETAILS][self.MSG_GROUP]
        except KeyError:
            self.log("Group not in message, exiting callback.", 
                     self.RK_LOG_ERROR)
            return
        
        try:
            groupall = body[self.MSG_DETAILS][self.MSG_GROUPALL]
        except KeyError:
            groupall = False

        # get the api-action from the details section of the message
        try:
            api_action = body[self.MSG_DETAILS][self.MSG_API_ACTION]
        except KeyError:
            self.log("API-action not in message, continuing without.", 
                     self.RK_LOG_ERROR)
            api_action = None

        # get the desired user id from the details section of the message
        try:
            query_user = body[self.MSG_DETAILS][self.MSG_USER_QUERY]
        except KeyError:
            self.log("Query user not in message, continuing without.", 
                     self.RK_LOG_INFO)
            query_user = None

        # get the desired group from the details section of the message
        try:
            query_group = body[self.MSG_DETAILS][self.MSG_GROUP_QUERY]
        except KeyError:
            self.log("Query group not in message, continuing without.", 
                     self.RK_LOG_INFO)
            query_group = None

        # For now we're not allowing users to query other users, but will in the 
        # future with the inclusion of ROLES. Leave this here for completeness 
        # and ease of insertion of the appropriate logic in the future.
        # groupall allows users to query other groups
        if groupall and query_group is not None and query_group != group:
            self.log("Attempting to query a group that does not match current "
                     "group, exiting callback", self.RK_LOG_ERROR)
            return

        elif query_user is not None and user != query_user:
            self.log("Attempting to query a user that does not match current "
                     "user, exiting callback", self.RK_LOG_ERROR)
            return
        
        # get the id / primary key
        try: 
            idd = body[self.MSG_DETAILS][self.MSG_ID]
        except KeyError:
            self.log("Id not in message, continuing without.", 
                     self.RK_LOG_INFO)
            idd = None

        # get the desired transaction id from the details section of the message
        try: 
            transaction_id = body[self.MSG_DETAILS][self.MSG_TRANSACT_ID]
        except KeyError:
            self.log("Transaction id not in message, continuing without.", 
                     self.RK_LOG_INFO)
            transaction_id = None
        
        try:
            job_label = body[self.MSG_DETAILS][self.MSG_JOB_LABEL]
        except:
            self.log("job label not in message, continuing without.", 
                     self.RK_LOG_INFO)
            job_label = None
        
        # get the desired state from the details section of the message
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
                self.log("State found in message invalid, continuing without.", 
                         self.RK_LOG_INFO)
                state = None
        except KeyError:
            self.log("Required state not in message, continuing without.", 
                     self.RK_LOG_INFO)
            state = None
        
        # get the desired sub_record id from the details section of the message
        try: 
            sub_id = body[self.MSG_DETAILS][self.MSG_SUB_ID]
        except KeyError:
            self.log("Transaction sub-id not in message, continuing without.", 
                     self.RK_LOG_INFO)
            sub_id = None

        # get the desired retry_count from the DETAILS section of the message
        try: 
            retry_count = int(body[self.MSG_DETAILS][self.MSG_RETRY_COUNT_QUERY])
        except (KeyError, TypeError):
            self.log("Transaction sub-id not in message, continuing without.", 
                     self.RK_LOG_INFO)
            retry_count = None

        # start a SQL alchemy session
        self.monitor.start_session()

        try:
            trecs = self.monitor.get_transaction_record(
                user, group, groupall=groupall, idd=idd, 
                transaction_id=transaction_id, job_label=job_label
            )
        except MonitorError:
            trecs = []

        # Convert list of objects to json-friendly dict
        # we want a list of transaction_records, each transaction_record
        # contains a list of sub_records
        trecs_dict = {}
        for tr in trecs:
            srecs = self.monitor.get_sub_records(
                tr, sub_id, query_user, query_group, state, 
                retry_count, api_action
            )

            if tr.id in trecs_dict:
                t_rec = trecs_dict[tr.id]
            else:
                t_rec = {
                    "id": tr.id,
                    "transaction_id": tr.transaction_id,
                    "user": tr.user,
                    "group": tr.group,
                    "job_label": tr.job_label,
                    "api_action": tr.api_action,
                    "creation_time": tr.creation_time.isoformat(),
                    "warnings": tr.get_warnings(),
                    "sub_records" : []
                }
                trecs_dict[tr.id] = t_rec

            for sr in srecs:
                s_rec = {
                    "id": sr.id,
                    "sub_id": sr.sub_id,
                    "state": sr.state.name,
                    "retry_count": sr.retry_count,
                    "last_updated": sr.last_updated.isoformat(),
                    "failed_files": [orm_to_dict(ff) for ff in sr.failed_files]
                }
                t_rec["sub_records"].append(s_rec)

        self.monitor.end_session()

        # Send the recovered record as an RPC response if there are one or more
        # sub records
        ret_list = []
        for id_ in trecs_dict:
            if len(trecs_dict[id_]['sub_records']) > 0:
                ret_list.append(trecs_dict[id_])
                
        body[self.MSG_DATA][self.MSG_RECORD_LIST] = ret_list
        self.publish_message(
            properties.reply_to,
            msg_dict=body,
            exchange={'name': ''},
            correlation_id=properties.correlation_id
        )
        self.log(f"Successfully returned query via RPC message to api-server", 
                 self.RK_LOG_INFO)


    def callback(self, ch: Channel, method: Method, properties: Header, 
                 body: bytes, connection: Connection) -> None:
        # Connect to database if not connected yet                
        # Convert body from bytes to json for ease of manipulation
        body = json.loads(body)

        self.log(f"Received {json.dumps(body, indent=4)} from "
                 f"{self.queues[0].name} ({method.routing_key})", 
                 self.RK_LOG_INFO)

        # Get the API method and decide what to do with it
        try:
            api_method = body[self.MSG_DETAILS][self.MSG_API_ACTION]
        except KeyError:
            self.log(f"Message did not contain an API method, exiting callback", 
                     self.RK_LOG_ERROR)
            return

        # check whether this is a GET or a PUT
        if (api_method == self.RK_STAT):
            self.log("Starting stat from monitoring db.", self.RK_LOG_INFO)
            self._monitor_get(body, properties)
        elif api_method in (self.RK_PUT, self.RK_PUTLIST, 
                            self.RK_GET, self.RK_GETLIST, 
                            self.RK_ARCHIVE_PUT, self.RK_ARCHIVE_GET):
            # Verify routing key is appropriate
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
                self.log("Routing key inappropriate length, exiting callback.", 
                         self.RK_LOG_ERROR)
                return
            # NOTE: Could check that rk_parts[2] is 'start' here? No particular 
            # need as the exchange does that for us and merely having three 
            # parts is enough to tell that it didn't come from the api-server
            self.log("Starting put into monitoring db.", self.RK_LOG_INFO)
            self._monitor_put(body)
        else:
            self.log("API method key did not specify a valid task.", 
                     self.RK_LOG_ERROR)
        
        self.log("Callback complete!", self.RK_LOG_INFO)


    def attach_database(self, create_db_fl: bool = True):
        """Attach the Monitor to the consumer"""
        # Load config options or fall back to default values.
        db_engine = self.load_config_value(self._DB_ENGINE)
        db_options = self.load_config_value(self._DB_OPTIONS)
        self.monitor = Monitor(db_engine, db_options)

        try:
            db_connect = self.monitor.connect(create_db_fl=create_db_fl)
            if create_db_fl:
                self.log(f"db_connect string is {db_connect}", RMQC.RK_LOG_DEBUG)
        except DBError as e:
            self.log(e.message, RMQC.RK_LOG_CRITICAL)


    def get_engine(self):
        # Method for making the db_engine available to alembic
        return self.database.db_engine
    

    def get_url(self):
        """ Method for making the sqlalchemy url available to alembic"""
        # Create a minimum version of the catalog to put together a url
        if self.monitor is None:
            db_engine = self.load_config_value(self._DB_ENGINE)
            db_options = self.load_config_value(self._DB_OPTIONS)
            self.monitor = Monitor(db_engine, db_options)
        return self.monitor.get_db_string()


def main():
    consumer = MonitorConsumer()
    # connect to message queue early so that we can send logging messages about
    # connecting to the database
    consumer.get_connection()
    # connect to the DB
    consumer.attach_database()
    # run the loop
    consumer.run()

if __name__ == "__main__":
    main()