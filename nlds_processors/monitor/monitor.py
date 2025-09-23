# encoding: utf-8
"""
monitor.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "15 Sep 2022"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from sqlalchemy.exc import IntegrityError, OperationalError, DataError, NoResultFound

from nlds_processors.monitor.monitor_models import MonitorBase, TransactionRecord
from nlds_processors.monitor.monitor_models import SubRecord, FailedFile, Warning

from nlds.rabbit.consumer import State
from nlds.details import PathDetails

from nlds_processors.db_mixin import DBMixin
from nlds.errors import MessageError


class MonitorError(MessageError):
    pass


class Monitor(DBMixin):
    """Monitor object containing methods to manipulate the Monitor Database"""

    def __init__(self, db_engine: str, db_options: str):
        """Record the monitor engine from the config strings passed in"""
        self.db_engine_str = db_engine
        self.db_options = db_options
        self.base = MonitorBase
        self.session = None

    def create_transaction_record(
        self,
        user: str,
        group: str,
        transaction_id: str,
        job_label: str,
        api_action: str,
    ) -> TransactionRecord:
        """Creates a transaction_record with the minimum required input"""
        try:
            transaction_record = TransactionRecord(
                transaction_id=transaction_id,
                user=user,
                group=group,
                api_action=api_action,
                job_label=job_label,
            )

            self.session.add(transaction_record)
            self.session.commit()  # commit to update the transaction_record.id and
            # prevent contention with any other monitor worker
            # processes
        except (IntegrityError, KeyError) as e:
            raise MonitorError(
                f"Transaction record with transaction_id {transaction_id} "
                "could not be added to the database"
            )
        return transaction_record

    def get_transaction_record(
        self, user: str, group: str, idd: int = None, transaction_id: str = None
    ) -> TransactionRecord:
        """Fast version of get_transaction_record for internal messaging"""
        try:
            # filter on idd or transaction_id
            if idd:
                trec_q = self.session.query(TransactionRecord).filter(
                    TransactionRecord.id == idd
                )
            else:
                trec_q = self.session.query(TransactionRecord).filter(
                    TransactionRecord.transaction_id == transaction_id,
                )
            # filter on user and group
            trec_q = trec_q.filter(
                TransactionRecord.group == group, TransactionRecord.user == user
            )
            trec = trec_q.one()
        except (NoResultFound, KeyError, OperationalError):
            if idd:
                raise MonitorError(f"TransactionRecord with id:{idd} not found")
            else:
                raise MonitorError(
                    f"TransactionRecord with transaction_id:{transaction_id} "
                    f"not found"
                )
        return trec

    def get_transaction_records(
        self,
        user: str,
        group: str,
        groupall: bool = False,
        idd: int = None,
        transaction_id: str = None,
        api_action: list[str] = None,
        exclude_api_action: list[str] = None,
        job_label: str = None,
        regex: bool = False,
        limit: int = None,
        descending: bool = False,
    ) -> list:
        """Gets a list of TransactionRecords from the DB from the given a whole host of
        information.  Only used for user queries."""
        if transaction_id:
            transaction_search = transaction_id
            transaction_regex = False
        else:
            transaction_search = ".*"
            transaction_regex = True

        try:
            if idd:
                trec = self.session.query(TransactionRecord).filter(
                    TransactionRecord.id == idd
                )
            elif job_label:
                if regex:
                    trec = self.session.query(TransactionRecord).filter(
                        TransactionRecord.job_label.regexp_match(job_label),
                    )
                else:
                    trec = self.session.query(TransactionRecord).filter(
                        TransactionRecord.job_label == job_label,
                    )

            else:
                if transaction_regex:
                    trec = self.session.query(TransactionRecord).filter(
                        TransactionRecord.transaction_id.regexp_match(
                            transaction_search
                        ),
                    )
                else:
                    trec = self.session.query(TransactionRecord).filter(
                        TransactionRecord.transaction_id == transaction_search,
                    )
            if api_action:
                trec = trec.filter(TransactionRecord.api_action.in_(api_action))
            if exclude_api_action:
                trec = trec.filter(
                    TransactionRecord.api_action.not_in(exclude_api_action)
                )
            # group filter
            if group != "**all**":
                trec = trec.filter(TransactionRecord.group == group)
            # user filter
            if not groupall and user != "**all**":
                trec = trec.filter(TransactionRecord.user == user)
            # Order up or down
            if descending:
                trec = trec.order_by(TransactionRecord.creation_time.desc())
            else:
                trec = trec.order_by(TransactionRecord.creation_time)
            # limit for speed - but how many sub-records (where the api-action is stored)
            if limit:
                trecs = trec.limit(limit).all()
            else:
                trecs = trec.all()

            if len(trecs) == 0:
                raise KeyError

        except (IntegrityError, KeyError, OperationalError):
            if idd:
                raise MonitorError(f"TransactionRecord with id:{idd} not found")
            elif job_label:
                raise MonitorError(
                    f"TransactionRecord with job_label:{job_label} " f"not found"
                )
            else:
                raise MonitorError(
                    f"TransactionRecord with transaction_id:{transaction_id} "
                    f"not found"
                )
        except DataError as e:
            if regex:
                raise MonitorError(f"Invalid regular expression: {transaction_search}")
            else:
                raise MonitorError(f"Error getting transaction_record: {e}")
        return trecs

    def create_sub_record(
        self, transaction_record: TransactionRecord, sub_id: str, state: State = None
    ) -> SubRecord:
        """Creates a SubRecord with the minimum required input. Optionally adds
        to a session and commits to get the id field populated.
        """
        if state is None:
            # Set to initial/default value
            state = State.INITIALISING

        try:
            sub_record = SubRecord(
                sub_id=sub_id,
                state=state,
                retry_count=0,
                transaction_record_id=transaction_record.id,
            )
            self.session.add(sub_record)
            # need to commit to update the transaction_record.id
            # and prevent contention with any other monitor worker processes
            self.session.commit()
        except (IntegrityError, KeyError):
            raise MonitorError(
                f"SubRecord for transaction_record_id:{transaction_record.id} "
                "could not be added to the database"
            )
        return sub_record

    def create_failed_file(
        self, sub_record: SubRecord, path_details: PathDetails, reason: str = None
    ) -> FailedFile:
        """Creates a FailedFile object for the monitoring database. Requires the
        input of the parent SubRecord and the PathDetails object of the failed
        file in question. Optionally requires a reason str, which will otherwise
        be attempted to be taken from the PathDetails object. If no reason can
        be found then a MonitorError will be raised.
        """
        try:
            failed_file = FailedFile(
                filepath=path_details.original_path,
                reason=reason,
                sub_record_id=sub_record.id,
            )
            self.session.add(failed_file)
            self.session.commit()  # commit to prevent contention with any other
            # monitor worker processes
        except (IntegrityError, KeyError):
            raise MonitorError(
                f"FailedFile for sub_record_id:{sub_record.id} could not be "
                "added to the database"
            )
        return failed_file

    def get_sub_record(
        self, transaction_record: TransactionRecord, sub_id: str
    ) -> SubRecord:
        """Return a single sub record identified by the sub_id"""
        try:
            # Get subrecord(s) by sub_id
            srecs = (
                self.session.query(SubRecord)
                .filter(SubRecord.transaction_record_id == transaction_record.id)
                .filter(SubRecord.sub_id == sub_id)
            )
            if len(srecs) > 1:
                raise MonitorError(
                    f"More than one sub record with sub_id:{sub_id} found"
                )
            srec = srecs[0]
        except (IntegrityError, IndexError, NoResultFound):
            raise MonitorError(f"SubRecord with sub_id:{sub_id} not found")
        return srec

    def get_sub_records(
        self,
        transaction_record: TransactionRecord,
        sub_id: str = None,
        user: str = None,
        group: str = None,
        state: list[State] = None,
        api_action: str = None,
        exclude_api_action: str = None,
    ) -> list:
        """Return many sub records, identified by one of the (many) function
        parameters"""

        try:
            query = self.session.query(SubRecord).filter(
                SubRecord.transaction_record_id == transaction_record.id
            )

            # apply filters one at a time if present. Results in a big 'and' query
            # of the passed flags
            if sub_id is not None:
                query = query.filter(SubRecord.sub_id == sub_id)
            if state is not None:
                query = query.filter(SubRecord.state.in_(state))
            if api_action is not None:
                query = query.filter(TransactionRecord.api_action.in_(api_action))
            if exclude_api_action is not None:
                query = query.filter(
                    TransactionRecord.api_action.not_in(exclude_api_action)
                )
            if user is not None and user != "**all**":
                query = query.filter(TransactionRecord.user == user)
            if group is not None and group != "**all**":
                query = query.filter(TransactionRecord.group == group)
            srecs = query.join(TransactionRecord).all()
        except (IntegrityError, KeyError):
            raise MonitorError(
                f"Could not return SubRecords from function get_sub_records"
            )

        return srecs

    def update_sub_record(self, sub_record: SubRecord, new_state: State) -> SubRecord:
        """Update a retrieved SubRecord to reflect the new monitoring info.
        Furthest state is updated, if required.
        """
        # Upgrade state to new_state, but throw exception if regressing state
        # from COMPLETE or FAILED to a state below that
        # It IS permitted to go from SPLIT to any value
        if (
            sub_record.state.value != State.SPLIT.value
            and sub_record.state.value >= State.COMPLETE.value
            and new_state.value < State.COMPLETE.value
        ):
            raise MonitorError(
                f"Monitoring state cannot go backwards from {sub_record.state}. "
                f"Attempted {sub_record.state}->{new_state}"
            )
        sub_record.state = new_state
        self.session.commit()  # commit to prevent contention with any other monitor
        # worker processes

    def check_completion(self, transaction_record: TransactionRecord) -> None:
        """Get the complete list of sub records from a transaction record and
        check whether they are all in a final state, and update them to COMPLETE
        if so.
        """
        try:
            # Get all sub_records by transaction_record.id
            sub_records = (
                self.session.query(SubRecord)
                .filter(SubRecord.transaction_record_id == transaction_record.id)
                .all()
            )

            # Check for an empty query as this doesn't get caught by the below
            # all() check.
            if len(sub_records) == 0:
                raise MonitorError(
                    f"transaction_record {transaction_record.id} has no "
                    f"associated sub_records, something has gone wrong."
                )

            # Check whether all jobs have reached their final, but not-complete,
            # state.
            if all([sr.has_finished() for sr in sub_records]):
                # If all have, then set all non-failed jobs to complete
                for sr in sub_records:
                    if sr.state in State.get_failed_states():
                        self.update_sub_record(sr, State.FAILED)
                    else:
                        self.update_sub_record(sr, State.COMPLETE)

        except IntegrityError:
            raise MonitorError(
                "IntegrityError raised when attempting to get sub_records"
            )

    def create_warning(
        self, transaction_record: TransactionRecord, warning: str
    ) -> Warning:
        """Create a warning and add it to the TransactionRecord"""
        if self.session is None:
            raise RuntimeError("self.session is None")
        try:
            warning = Warning(
                warning=warning, transaction_record_id=transaction_record.id
            )
            self.session.add(warning)
            self.session.commit()  # commit to prevent contention with any other monitor
            # worker processes
        except (IntegrityError, KeyError):
            raise MonitorError(
                f"Warning for transaction_record:{transaction_record.id} could "
                "not be added to the database"
            )
        return warning
