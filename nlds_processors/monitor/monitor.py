from sqlalchemy import create_engine, func, Enum
from sqlalchemy.exc import ArgumentError, IntegrityError, OperationalError
from sqlalchemy.orm import Session

from nlds_processors.monitor.monitor_models import MonitorBase, TransactionRecord
from nlds_processors.monitor.monitor_models import SubRecord, FailedFile

from nlds.rabbit.consumer import State
from nlds.details import PathDetails

from nlds_processors.db_mixin import DBMixin

class MonitorError(Exception):
    def __init__(self, message, *args):
        super().__init__(args)
        self.message = message


class Monitor(DBMixin):
    """Monitor object containing methods to manipulate the Monitor Database"""
    def __init__(self, db_engine: str, db_options: str):
        """Record the monitor engine from the config strings passed in"""
        self.db_engine_str = db_engine
        self.db_options = db_options
        self.base = MonitorBase
        self.session = None

    def create_transaction_record(self,
                                  user: str,
                                  group: str,
                                  transaction_id: str, 
                                  api_action: str) -> TransactionRecord:
        """Creates a transaction_record with the minimum required input"""
        try:
            transaction_record = TransactionRecord(
                transaction_id = transaction_id, 
                user = user,
                group = group, 
                api_action = api_action,
            )

            self.session.add(transaction_record)
            self.session.flush()     # flush to update the transaction_record.id
        except (IntegrityError, KeyError) as e:
            raise MonitorError(
                f"Transaction record with transaction_id {transaction_id} "
                "could not be added to the database"
            )
        return transaction_record
    

    def get_transaction_record(self, transaction_id: str) -> TransactionRecord:
        """Gets a TransactionRecord from the DB from the given transaction_id"""
        try:
            trec = self.session.query(TransactionRecord).filter(
                TransactionRecord.transaction_id == transaction_id
            ).one_or_none()

        except (IntegrityError, KeyError):
            raise MonitorError(
                f"TransactionRecord with transaction_id:{transaction_id} not "
                f"found")
        return trec


    def create_sub_record(self, 
                          sub_id: str, 
                          transaction_record_id: int, 
                          state: State = None) -> SubRecord:
        """Creates a SubRecord with the minimum required input. Optionally adds 
        to a session and flushes to get the id field populated. 
        """
        if state is None:
            # Set to initial/default value
            state = State.INITIALISING
        
        try:
            sub_record = SubRecord(
                sub_id = sub_id, 
                state = state,
                retry_count = 0,
                transaction_record_id = transaction_record_id,
            )
            self.session.add(sub_record)
            # need to flush to update the transaction_record.id
            self.session.flush()
        except (IntegrityError, KeyError):
            raise MonitorError(
                f"SubRecord for transaction_record_id:{transaction_record_id} "
                "could not be added to the database"
            )
        return sub_record


    def create_failed_file(self, 
                           sub_record: SubRecord, 
                           path_details: PathDetails) -> FailedFile:
        try:
            failed_file = FailedFile(
                filepath=path_details.original_path,
                reason=path_details.retry_reasons[-1],
                sub_record_id=sub_record.id,
            )
            self.session.add(failed_file)
            self.session.flush()
        except (IntegrityError, KeyError):
            raise MonitorError(
                f"FailedFile for sub_record_id:{sub_record.id} could not be "
                "added to the database"
            )
        return failed_file    


    def get_sub_record(self, 
                       sub_id: str) -> SubRecord:
        """Return a single sub record identified by the sub_id"""
        try:
            # Get subrecord by sub_id
            srec = self.session.query(SubRecord).filter(
                SubRecord.sub_id == sub_id
            ).one_or_none()
        except  (IntegrityError, KeyError):
            raise MonitorError(
                f"SubRecord with sub_id:{sub_id} not found"
            )
        return srec


    def get_sub_records(self,
                        sub_id: str = None,
                        user: str = None,
                        group: str = None,
                        state: State = None,
                        retry_count: int = None,
                        transaction_id: str = None,
                        api_action: str = None) -> list:

        """Return many sub records, identified by one of the (many) function
        parameters"""

        try:
            query = self.session.query(TransactionRecord, SubRecord)

            # apply filters one at a time if present. Results in a big 'and' query 
            # of the passed flags
            # TODO: (2022-11-13) Will need to adapt this to do mulitple of each
            if sub_id is not None:
                # sub_record = self._get_sub_record(session, sub_id)
                query = query.filter(SubRecord.sub_id == sub_id)
            if state is not None:
                query = query.filter(SubRecord.state == state)
            if retry_count is not None:
                query = query.filter(SubRecord.retry_count == retry_count)
            if transaction_id is not None:
                query = query.filter(TransactionRecord.transaction_id == transaction_id)
            if api_action is not None:
                api_action = query.filter(TransactionRecord.api_action == api_action)
            if user is not None: 
                query = query.filter(TransactionRecord.user == user) 
            if group is not None:
                query = query.filter(TransactionRecord.group == group)
            srecs = query.all()
        except (IntegrityError, KeyError):
            raise MonitorError(
                f"Could not return SubRecords from function get_sub_records"
            )

        return srecs


    def update_sub_record(self, 
                          sub_record: SubRecord, 
                          new_state: State, 
                          retry_fl: bool) -> SubRecord:
        """Update a retrieved SubRecord to reflect the new monitoring info. 
        Furthest state is updated, if required, and the retry count is 
        incremented by one if appropriate.
        TODO: Should retrying be a flag instead of a separate state? Probably, 
        yes
        """
        # Increment retry counter if appropriate. 
        # NOTE: Do we want to just specify the retry_count in the message?
        if retry_fl:
            sub_record.retry_count = (
                SubRecord.retry_count + 1
            )
        # Reset retry count if retry was successful, keep it if the job failed 
        elif sub_record.retry_count > 0 and new_state != State.FAILED:
            sub_record.retry_count = 0
        # Upgrade state to new_state, but throw exception if regressing state 
        # (staying the same is fine)
        if (new_state.value < sub_record.state.value):
            raise ValueError(f"Monitoring state cannot go backwards or skip "
                             f"steps. Attempted {sub_record.state}->{new_state}"
                             )
        sub_record.state = new_state
        self.session.flush()


    def check_completion(self,
                         transaction_record: TransactionRecord) -> None:
        """Get the complete list of sub records from a transaction record and 
        check whether they are all in a final state, and update them to COMPLETE
        if so.
        """
        try:
            # Get all sub_records by transaction_record.id
            sub_records = self.session.query(SubRecord).filter(
                SubRecord.transaction_record_id == transaction_record.id
            ).all()
            
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
                    if sr.state == State.FAILED:
                        continue
                    self.update_sub_record(sr, State.COMPLETE, False)

        except IntegrityError:
            raise MonitorError(
                "IntegrityError raised when attempting to get sub_records"
            )