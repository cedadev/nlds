from sqlalchemy.exc import IntegrityError, OperationalError

from nlds_processors.monitor.monitor_models import MonitorBase, TransactionRecord
from nlds_processors.monitor.monitor_models import SubRecord, FailedFile, Warning

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
            self.session.flush()  # flush to update the transaction_record.id
        except (IntegrityError, KeyError) as e:
            raise MonitorError(
                f"Transaction record with transaction_id {transaction_id} "
                "could not be added to the database"
            )
        return transaction_record

    def get_transaction_record(
        self,
        user: str,
        group: str,
        groupall: bool = False,
        idd: int = None,
        transaction_id: str = None,
        job_label: str = None,
    ) -> list:
        """Gets a TransactionRecord from the DB from the given transaction_id,
        or the primary key (id)"""
        if transaction_id:
            transaction_search = transaction_id
        else:
            transaction_search = ".*"

        try:
            if idd:
                trec = self.session.query(TransactionRecord).filter(
                    TransactionRecord.group == group, TransactionRecord.id == idd
                )
            elif job_label:
                trec = self.session.query(TransactionRecord).filter(
                    TransactionRecord.group == group,
                    TransactionRecord.job_label.regexp_match(job_label),
                )
            else:
                trec = self.session.query(TransactionRecord).filter(
                    TransactionRecord.group == group,
                    TransactionRecord.transaction_id.regexp_match(transaction_search),
                )
            # user filter
            if not groupall:
                trec = trec.filter(TransactionRecord.user == user)
            trecs = trec.all()

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
        return trecs

    def create_sub_record(
        self, transaction_record: TransactionRecord, sub_id: str, state: State = None
    ) -> SubRecord:
        """Creates a SubRecord with the minimum required input. Optionally adds
        to a session and flushes to get the id field populated.
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
            # need to flush to update the transaction_record.id
            self.session.flush()
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
        if reason is None:
            if len(path_details.retries.reasons) <= 0:
                raise MonitorError(
                    f"FailedFile for sub_record_id:{sub_record.id} could not be "
                    "added to the database as no failure reason was supplied. "
                )
            else:
                reason = path_details.retries.reasons[-1]
        try:
            failed_file = FailedFile(
                filepath=path_details.original_path,
                reason=reason,
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

    def get_sub_record(self, sub_id: str) -> SubRecord:
        """Return a single sub record identified by the sub_id"""
        try:
            # Get subrecord by sub_id
            srec = (
                self.session.query(SubRecord)
                .filter(SubRecord.sub_id == sub_id)
                .one_or_none()
            )
        except (IntegrityError, KeyError):
            raise MonitorError(f"SubRecord with sub_id:{sub_id} not found")
        return srec

    def get_sub_records(
        self,
        transaction_record: TransactionRecord,
        sub_id: str = None,
        user: str = None,
        group: str = None,
        state: State = None,
        retry_count: int = None,
        api_action: str = None,
    ) -> list:
        """Return many sub records, identified by one of the (many) function
        parameters"""

        try:
            query = self.session.query(SubRecord).filter(
                SubRecord.transaction_record_id == transaction_record.id
            )

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

    def update_sub_record(
        self, sub_record: SubRecord, new_state: State, retry_fl: bool
    ) -> SubRecord:
        """Update a retrieved SubRecord to reflect the new monitoring info.
        Furthest state is updated, if required, and the retry count is
        incremented by one if appropriate.
        TODO: Should retrying be a flag instead of a separate state? Probably,
        yes
        """
        # Increment retry counter if appropriate.
        # NOTE: Do we want to just specify the retry_count in the message?
        if retry_fl:
            sub_record.retry_count = SubRecord.retry_count + 1
        # Reset retry count if retry was successful, keep it if the job failed
        elif sub_record.retry_count > 0 and new_state != State.FAILED:
            sub_record.retry_count = 0
        # Upgrade state to new_state, but throw exception if regressing state
        # (staying the same is fine)
        if new_state.value < sub_record.state.value:
            raise MonitorError(
                f"Monitoring state cannot go backwards or skip steps. Attempted"
                f" {sub_record.state}->{new_state}"
            )
        sub_record.state = new_state
        self.session.flush()

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
                        self.update_sub_record(sr, State.FAILED, False)
                    else:
                        self.update_sub_record(sr, State.COMPLETE, False)

        except IntegrityError:
            raise MonitorError(
                "IntegrityError raised when attempting to get sub_records"
            )

    def create_warning(
        self, transaction_record: TransactionRecord, warning: str
    ) -> Warning:
        """Create a warning and add it to the TransactionRecord"""
        assert self.session != None
        try:
            warning = Warning(
                warning=warning, transaction_record_id=transaction_record.id
            )
            self.session.add(warning)
            self.session.flush()
        except (IntegrityError, KeyError):
            raise MonitorError(
                f"Warning for transaction_record:{transaction_record.id} could "
                "not be added to the database"
            )
        return warning
