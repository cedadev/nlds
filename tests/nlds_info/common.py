from nlds_processors.monitor.monitor import Monitor
from nlds_processors.monitor.monitor_models import (
    TransactionRecord,
    SubRecord,
    FailedFile,
    Warning,
)
import uuid
from nlds.rabbit.consumer import State
from datetime import datetime, timedelta
def mock_monitor():
    """Function to mock the monitor database with in-memory SQLite for testing."""
    db_engine = "sqlite"
    db_options = {"db_name": "", "db_user": "", "db_passwd": "", "echo": False}

    # Set up Monitor instance and start the session
    monitor = Monitor(db_engine, db_options)
    monitor.connect()
    monitor.start_session()

    # Provide the monitor instance to the test without any pre-existing records
    return monitor
def generate_uuid_list(num):
    uuid_list = []
    for i in range(num):
        uuid_list.append(str(uuid.uuid4()))
    return uuid_list
def sample_record(
    id=1,
    uuid=generate_uuid_list(1),
    failed=False,
    warning=False,
    sub_record_num=1,
    state=[100],
    transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfcb",
    user="existing_user",
    group="test_group",
    creation_time=datetime.now() - timedelta(days=2),
):
    """Function to provide a reusable sample transaction record."""
    sub_record_list = []
    for i in range(sub_record_num):
        try:
            state_val = State(state[i])
        except IndexError:
            state_val = State(state[0])
        uuid_val = uuid[i]
        if failed:
            failed_files = [
                FailedFile(
                    id=i + id,
                    filepath="/Users/user/file.txt",
                    reason=(
                        "S3 error: S3 operation failed; code: AccessDenied, "
                        "message: Access Denied, resource: /nlds., request_id: , "
                        "host_id: None when creating bucket"
                    ),
                    sub_record_id=i + id,
                )
            ]
        else:
            failed_files = []
        sub_record = SubRecord(
            id=i + id,
            sub_id=uuid_val,
            state=state_val,
            retry_count=1,
            last_updated=creation_time + timedelta(seconds=id),
            failed_files=failed_files,
        )
        sub_record_list.append(sub_record)

    if warning:
        warning_list = [
            Warning(
                id=id,
                warning="Warning: you have the same tags",
                transaction_record_id=id,
            )
        ]
    else:
        warning_list = []

    transaction_record = TransactionRecord(
        id=id,
        transaction_id=transaction_id,
        user=user,
        group=group,
        job_label="5d3aea89",
        api_action="put",
        creation_time=creation_time,
        sub_records=sub_record_list,
        warnings=warning_list,
    )

    return transaction_record