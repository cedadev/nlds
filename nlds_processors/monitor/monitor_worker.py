# encoding: utf-8
"""
monitor_worker.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "15 Sep 2022"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

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

from typing import Dict

from retry.api import retry_call

from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

from nlds.rabbit.consumer import RabbitMQConsumer as RMQC
from nlds.rabbit.consumer import State
from nlds_processors.monitor.monitor import Monitor, MonitorError
from nlds_processors.monitor.monitor_models import orm_to_dict
from nlds_processors.db_mixin import DBError

import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


class MonitorConsumer(RMQC):
    DEFAULT_QUEUE_NAME = "monitor_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.MONITOR}." f"{RK.WILD}"
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

    def _parse_transaction_id(self, body, mandatory=True):
        # get the transaction id from the details section of the message.
        try:
            transaction_id = body[MSG.DETAILS][MSG.TRANSACT_ID]
        except KeyError:
            if mandatory:
                msg = "Transaction id not in message, exiting callback."
                self.log(msg, RK.LOG_ERROR)
                raise MonitorError(message=msg)
            else:
                transaction_id = None
        return transaction_id

    def _parse_user(self, body):
        # get the user id from the details section of the message
        try:
            user = body[MSG.DETAILS][MSG.USER]
        except KeyError:
            msg = "User not in message, exiting callback."
            self.log(msg, RK.LOG_ERROR)
            raise MonitorError(message=msg)
        return user

    def _parse_group(self, body):
        # get the group from the details section of the message
        try:
            group = body[MSG.DETAILS][MSG.GROUP]
            if group is None:
                raise ValueError
        except (KeyError, ValueError):
            msg = "Group not in message, exiting callback."
            self.log(msg, RK.LOG_ERROR)
            raise MonitorError(message=msg)
        return group

    def _parse_api_action(self, body, mandatory=True):
        # get the api-action from the details section of the message
        try:
            api_action = body[MSG.DETAILS][MSG.API_ACTION]
        except KeyError:
            if mandatory:
                msg = "API-action not in message, exiting callback."
                self.log(msg, RK.LOG_ERROR)
                raise Monitor(message=msg)
            else:
                api_action = None
        return api_action

    def _parse_job_label(self, body):
        # get the job_label from the details section of the message
        # if it doesn't exist then use the transaction_id
        # if that doesn't exist then return blank
        try:
            job_label = body[MSG.DETAILS][MSG.JOB_LABEL]
        except KeyError:
            try:
                transaction_id = body[MSG.DETAILS][MSG.TRANSACT_ID]
                job_label = transaction_id[0:8]
            except KeyError:
                job_label = ""
        return job_label

    def _parse_state(self, body):
        # get the state from the details section of the message
        try:
            state = body[MSG.DETAILS][MSG.STATE]
            # Convert state to an actual ENUM value for ease of comparison, can
            # either be passed as the enum.value (default) or as the state
            # string
            if State.has_value(state):
                state = State(state)
            elif State.has_name(state):
                state = State[state]
            else:
                msg = f"State found in message invalid: {state}, exiting callback."
                self.log(msg, RK.LOG_ERROR)
                raise MonitorError(message=msg)
        except KeyError:
            msg = "Required state not in message, exiting callback."
            self.log(msg, RK.LOG_ERROR)
            raise MonitorError(message=msg)
        return state

    def _parse_search_state(self, body):
        # get the state when searching / listing from _monitor_get
        try:
            state_list = body[MSG.META][MSG.STATE]
            states = []
            for s in state_list:
                if State.has_value(s):
                    states.append(State(s))
                elif State.has_name(s):
                    states.append(State[s])
                else:
                    msg = f"State found in message invalid: {s}."
                    self.log(msg, RK.LOG_ERROR)
                    raise MonitorError(message=msg)
        except KeyError:
            states = None
        return states

    def _parse_subid(self, body, mandatory=True):
        # get the sub_record id from the details section of the message
        try:
            sub_id = body[MSG.DETAILS][MSG.SUB_ID]
        except KeyError:
            if mandatory:
                msg = "Transaction sub-id not in message, exiting callback."
                self.log(msg, RK.LOG_ERROR)
                raise MonitorError(message=msg)
            else:
                sub_id = None
        return sub_id

    def _parse_warnings(self, body):
        # get the warning(s) from the details section of the message
        try:
            warnings = body[MSG.DETAILS][MSG.WARNING]
        except KeyError:
            self.log("No warning found in message, continuing without", RK.LOG_DEBUG)
            warnings = []
        return warnings

    def _parse_groupall(self, body):
        # get whether to list all jobs in the group, or just the user's groups
        try:
            groupall = body[MSG.DETAILS][MSG.GROUPALL]
        except KeyError:
            groupall = False
        return groupall

    def _parse_querygroup(self, body, user, group):
        # get the desired group from the details section of the message
        try:
            query_group = body[MSG.DETAILS][MSG.GROUP_QUERY]
        except KeyError:
            self.log("Query group not in message, continuing without.", RK.LOG_INFO)
            query_group = None

        # For now we're not allowing users to query other users, but will in the
        # future with the inclusion of ROLES. Leave this here for completeness
        # and ease of insertion of the appropriate logic in the future.
        # groupall allows users to query other groups
        # nlds user is allowed to query everything
        if query_group is not None and (query_group != group and user != "nlds"):
            msg = (
                "Attempting to query a group that does not match current group, "
                "exiting callback"
            )
            self.log(msg, RK.LOG_ERROR)
            raise MonitorError(message=msg)
        return query_group

    def _parse_queryuser(self, body, user):
        # get the desired user id to search for from the details section of the
        # message. this can be different than the user making the call
        try:
            query_user = body[MSG.DETAILS][MSG.USER_QUERY]
        except KeyError:
            self.log("Query user not in message, continuing without.", RK.LOG_INFO)
            query_user = None

        # Allow the nlds user to query everything
        if query_user is not None and (user != query_user and user != "nlds"):
            msg = (
                "Attempting to query a user that does not match current "
                "user, exiting callback"
            )
            self.log(msg, RK.LOG_ERROR)
            raise MonitorError(message=msg)
        return query_user

    def _parse_idd(self, body):
        # get the id / primary key
        try:
            idd = body[MSG.DETAILS][MSG.ID]
        except KeyError:
            self.log("Id not in message, continuing without.", RK.LOG_INFO)
            idd = None
        return idd

    def _parse_regex(self, body: dict) -> str:
        # get the REGEX flag from the metadata section of the message
        try:
            regex = body[MSG.META][MSG.REGEX]
        except KeyError:
            regex = False
        return regex

    def _parse_limit(self, body: dict) -> str:
        # get an integer limit from the metadata section of the message
        try:
            limit = body[MSG.META][MSG.LIMIT]
        except KeyError:
            limit = None
        return limit

    def _parse_descending(self, body: dict) -> str:
        # get whether to sort ascending or descending
        try:
            descending = body[MSG.META][MSG.DESCENDING]
        except KeyError:
            descending = False
        return descending

    def _parse_api_action_from_meta(self, body):
        # get the api-action from the metadata section of the message
        try:
            api_action = body[MSG.META][MSG.API_ACTION]
        except KeyError:
            api_action = None
        return api_action

    def _parse_exclude_api_action_from_meta(self, body):
        try:
            exclude_action = body[MSG.META][MSG.EXCLUDE_API_ACTION]
        except KeyError:
            exclude_action = None
        return exclude_action

    def _get_transaction_record_with_retry(
        self,
        user,
        group,
        idd: int = None,
        transaction_id: str = None,
        with_for_update: bool = False,
    ):
        # Find an existing transaction record with the transaction id.
        try:
            args = [user, group]
            kwargs = {
                "idd": idd,
                "transaction_id": transaction_id,
                "with_for_update": with_for_update,
            }
            try:
                trec = retry_call(
                    self.monitor.get_transaction_record,
                    fargs=args,
                    fkwargs=kwargs,
                    delay=1,
                    tries=5,
                    backoff=2,
                )
            except Exception as e:
                raise e
        except MonitorError as e:
            # fine to pass here as if transaction_record is not returned then it
            # will be created in the next step
            self.log(e.message, RK.LOG_ERROR)
            raise e

        return trec

    def _get_or_create_sub_record(self, trec, sub_id, state, with_for_update=False):
        # get or create a sub record
        try:
            srec = self.monitor.get_sub_record(
                trec, sub_id, with_for_update=with_for_update
            )
        except MonitorError:
            srec = None

        # create the sub rec if not found
        if srec is None:
            try:
                srec = self.monitor.create_sub_record(trec, sub_id, state)
            except MonitorError as e:
                self.log(e.message, RK.LOG_ERROR)

        # consistency check
        if srec.transaction_record_id != trec.id:
            msg = (
                f"Transaction id {trec.id} does not match sub_record's transaction "
                f"id {srec.transaction_record_id}. Something has gone wrong, rolling "
                "back and exiting callback."
            )
            self.log(msg, RK.LOG_ERROR)
            raise MonitorError(message=msg)
        return srec

    def _monitor_init(self, body: Dict[str, str]) -> None:
        """
        Create a monitoring record for an in-progress transaction.
        """
        # get the required details from the message
        try:
            transaction_id = self._parse_transaction_id(body)
            user = self._parse_user(body)
            group = self._parse_group(body)
            api_action = self._parse_api_action(body)
            job_label = self._parse_job_label(body)
            state = self._parse_state(body)
            sub_id = self._parse_subid(body)
            warnings = self._parse_warnings(body)
        except MonitorError:
            # Functions above handled message logging, here we just return
            return True

        self.log(
            f"Received monitoring record creation for transaction {transaction_id}, "
            f"sub_record {sub_id}, api_action {api_action}, state {state}.",
            RK.LOG_INFO,
        )

        # start the database transactions
        self.monitor.start_session()
        # create the transaction record
        try:
            trec = self.monitor.create_transaction_record(
                user, group, transaction_id, job_label, api_action
            )
        except MonitorError as e:
            self.log(e.message, RK.LOG_ERROR)
        # save and end the sessions
        self.monitor.save()
        self.monitor.end_session()
        self.log(
            f"... Successfully created monitoring record",
            RK.LOG_INFO,
        )
        return True

    def _monitor_put(self, body: Dict[str, str]) -> None:
        """
        Update a monitoring record for an in-progress transaction.
        """
        # get the required details from the message
        try:
            transaction_id = self._parse_transaction_id(body)
            user = self._parse_user(body)
            group = self._parse_group(body)
            api_action = self._parse_api_action(body)
            job_label = self._parse_job_label(body)
            state = self._parse_state(body)
            sub_id = self._parse_subid(body)
            warnings = self._parse_warnings(body)
        except MonitorError:
            # Functions above handled message logging, here we just return
            return True

        # get last process from route
        route = body[MSG.DETAILS][MSG.ROUTE]
        route_parts = route.split("->")
        self.log(
            f"Received monitoring update for transaction {transaction_id}, "
            f"sub_record {sub_id}, api_action {api_action}, state {state}, "
            f"last process {route_parts[-1]}.",
            RK.LOG_INFO,
        )
        # get the filelist
        filelist = self.parse_filelist(body)
        # start the database transactions
        self.monitor.start_session()

        # For any given monitoring update, we need to:
        # - find the transaction record (create if not present)
        # - update the subrecord(s) associated with it
        #   - find an existing
        #   - see if it matches sub_id in message
        #       - update it if it does
        #           - change state
        #           - add failed files if failed
        #       - create a new one if it doesn't

        # find the transaction record

        try:
            trec = self._get_transaction_record_with_retry(
                user,
                group,
                idd=None,
                transaction_id=transaction_id,
                with_for_update=True,
            )
        except MonitorError as e:
            # fine to pass here as if transaction_record is not returned then it
            # will be created in the next step
            self.log(e.message, RK.LOG_ERROR)
            # don't ack - try again
            return False

        # create any warnings if there are any
        if warnings and len(warnings) > 0:
            for w in warnings:
                warning = self.monitor.create_warning(trec, w)

        # find or create the sub record
        try:
            srec = self._get_or_create_sub_record(
                trec, sub_id, state, with_for_update=True
            )
        except MonitorError as e:
            # Function above handled message logging, here we just return
            # don't ack - try again
            return False

        # Update subrecord to match new monitoring data
        try:
            self.monitor.update_sub_record(srec, state)
        except MonitorError as e:
            # If the state update is invalid then rollback session and exit
            # callback
            self.log(e.message, RK.LOG_ERROR)
            # session.rollback() # rollback needed?
            # don't ack - try again
            return False

        # Create failed_files if necessary
        if state in State.get_failed_states():
            self.log(
                "Creating FailedFiles records as transaction appears to have failed",
                RK.LOG_INFO,
            )
            try:
                for pd in filelist:
                    reason = ""
                    # Check which was the final reason for failure and pass
                    # that as the failure reason for the FailedFile.
                    if pd.failure_reason is not None:
                        reason = pd.failure_reason
                    self.monitor.create_failed_file(srec, pd, reason=reason)
            except MonitorError as e:
                self.log(e.message, RK.LOG_ERROR)

        # If reached the end of a workflow then check for completeness
        if state in State.get_final_states():
            self.log(
                "This sub_record is now in its final state for this workflow, now "
                "checking if all others have reached a final state.",
                RK.LOG_INFO,
            )
            try:
                self.monitor.check_completion(trec)
            except MonitorError as e:
                self.log(e.message, RK.LOG_ERROR)
                return False

        # Commit all transactions when we're sure everything is as it should be.
        self.monitor.save()
        self.monitor.end_session()
        self.log(
            f"... Successfully commited monitoring update",
            RK.LOG_INFO,
        )
        return True

    def _monitor_get(self, body: Dict[str, str], properties: Header) -> None:
        """
        Get a list of monitoring records for in-progress or finished
        transactions, filtered by flags passed by the user.
        NOTE: This might be sensible to move into a separate consumer so we can
        scale out database reads separately from database writes - which at the
        moment need to be done one at a time to avoid clashes.
        """

        # get the required details from the message
        try:
            transaction_id = self._parse_transaction_id(body, mandatory=False)
            user = self._parse_user(body)
            group = self._parse_group(body)
            api_action = self._parse_api_action_from_meta(body)
            exclude_api_action = self._parse_exclude_api_action_from_meta(body)
            job_label = self._parse_job_label(body)
            state = self._parse_search_state(body)
            sub_id = self._parse_subid(body, mandatory=False)
            groupall = self._parse_groupall(body)
            query_user = self._parse_queryuser(body, user)
            query_group = self._parse_querygroup(body, user, group)
            idd = self._parse_idd(body)
            regex = self._parse_regex(body)
            limit = self._parse_limit(body)
            descending = self._parse_descending(body)
        except MonitorError:
            # Functions above handled message logging, here we just return
            return

        # start a SQL alchemy session
        self.monitor.start_session()

        # form the request with the query user, special case for 'nlds' user
        if user == "nlds":
            if query_user is None:
                query_user = "**all**"
            if query_group is None:
                query_group = "**all**"
        else:
            query_user = user
            query_group = group

        try:
            trecs = self.monitor.get_transaction_records(
                query_user,
                query_group,
                groupall=groupall,
                idd=idd,
                transaction_id=transaction_id,
                api_action=api_action,
                exclude_api_action=exclude_api_action,
                job_label=job_label,
                regex=regex,
                limit=limit,
                descending=descending,
            )
        except MonitorError as e:
            self.log(e.message, RK.LOG_ERROR)
            body[MSG.DETAILS][MSG.FAILURE] = e.message
            trecs = []

        # Convert list of objects to json-friendly dict
        # we want a list of transaction_records, each transaction_record
        # contains a list of sub_records
        trecs_dict = {}
        # allow groupall to get all the sub records by setting query_user to None
        if groupall:
            query_user = None
        for tr in trecs:
            # filter on the state
            if state is not None:
                if tr.get_state() not in state:
                    continue

            # Note that state is used to filter on the final state, not the state of
            # each sub record - so return the sub records no matter what state they
            # are in
            srecs = self.monitor.get_sub_records(
                transaction_record=tr,
                sub_id=sub_id,
                user=query_user,
                group=query_group,
                state=None,
                api_action=api_action,
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
                    "sub_records": [],
                }
                trecs_dict[tr.id] = t_rec

            for sr in srecs:
                s_rec = {
                    "id": sr.id,
                    "sub_id": sr.sub_id,
                    "state": sr.state.name,
                    "last_updated": sr.last_updated.isoformat(),
                    "failed_files": [orm_to_dict(ff) for ff in sr.failed_files],
                }
                t_rec["sub_records"].append(s_rec)

        self.monitor.end_session()

        ret_list = []
        for id_ in trecs_dict:
            # NRM - return all trecs, even if they are empty - the client will interpret
            # them
            #if len(trecs_dict[id_]["sub_records"]) > 0:
            ret_list.append(trecs_dict[id_])
        body[MSG.DATA][MSG.RECORD_LIST] = ret_list
        self.publish_message(
            properties.reply_to,
            msg_dict=body,
            exchange={"name": ""},
            correlation_id=properties.correlation_id,
        )
        self.log(
            f"Successfully returned query via RPC message to api-server", RK.LOG_INFO
        )

    def callback(
        self,
        ch: Channel,
        method: Method,
        properties: Header,
        body: bytes,
        connection: Connection,
    ) -> None:
        # Connect to database if not connected yet
        # Convert body from bytes to json for ease of manipulation
        body = self._deserialize(body)

        self.log(
            f"Received from {self.queues[0].name} ({method.routing_key})",
            RK.LOG_DEBUG,
            body_json=body,
        )

        if self._is_system_status_check(body_json=body, properties=properties):
            return

        # Get the API method and decide what to do with it
        try:
            api_method = body[MSG.DETAILS][MSG.API_ACTION]
        except KeyError:
            self.log(
                f"Message did not contain an api_action, exiting callback", RK.LOG_ERROR
            )
            return

        # check whether this is a GET or a PUT
        if api_method == RK.STAT:
            self.log("Starting stat from monitoring db.", RK.LOG_INFO)
            self._monitor_get(body, properties)

        elif api_method in (
            RK.PUT,
            RK.PUTLIST,
            RK.GET,
            RK.GETLIST,
            RK.ARCHIVE_PUT,
            RK.ARCHIVE_GET,
        ):
            # Verify routing key is appropriate
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError as e:
                self.log(
                    "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
                )
                return
            # Check whether this is init or put (put is also update)
            if rk_parts[2] == RK.INITIATE:
                self._monitor_init(body)
            elif rk_parts[2] == RK.START:
                self._monitor_put(body)
        else:
            self.log("API method key did not specify a valid task.", RK.LOG_ERROR)

        self.log("Callback complete!", RK.LOG_DEBUG)

    def attach_database(self, create_db_fl: bool = True):
        """Attach the Monitor to the consumer"""
        # Load config options or fall back to default values.
        db_engine = self.load_config_value(self._DB_ENGINE)
        db_options = self.load_config_value(self._DB_OPTIONS)
        self.monitor = Monitor(db_engine, db_options)

        try:
            db_connect = self.monitor.connect(create_db_fl=create_db_fl)
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
