import pytest
from click.testing import CliRunner
from datetime import datetime, timedelta

import uuid

from nlds_processors.monitor.monitor import Monitor
from nlds_processors.monitor.monitor_models import TransactionRecord, SubRecord, FailedFile
from nlds_utils import nlds_monitor
from nlds.rabbit.consumer import State

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

def generate_uuid_list(num):                                                    # TODO check if works
    uuid_list = []
    for i in range(num):
        uuid_list.append(str(uuid.uuid4()))
    return uuid_list

def sample_record(id=1, uuid=generate_uuid_list(1), sub_record_num=1, state=[100], transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfcb", user="existing_user", group="test_group", creation_time=datetime(2024, 10, 2, 7, 46, 37)):
    """Function to provide a reusable sample transaction record."""
    sub_record_list = []
    for i in range(sub_record_num):
        try:
            state_val = State(state[i])
        except IndexError:
            state_val = State(state[0])
        uuid_val = uuid[i]
        sub_record = SubRecord(
            id=i+id,
            sub_id=uuid_val,
            state=state_val,
            retry_count=1,
            last_updated=datetime(2024, 10, 2, 7, 46, (i+1)),
            failed_files=[FailedFile(
                id=i+id,
                filepath="/Users/user/file.txt",
                reason="S3 error: S3 operation failed; code: AccessDenied, message: Access Denied, resource: /nlds., request_id: , host_id: None when creating bucket",
                sub_record_id=i+id,
            )],
        )
        sub_record_list.append(sub_record)
    
    transaction_record =  TransactionRecord(
        id=id,
        transaction_id=transaction_id,
        user=user,
        group=group,
        job_label="5d3aea89",
        api_action="put",
        creation_time=creation_time,
        sub_records=sub_record_list,
    )
    
    return transaction_record

def generate_simple_expected_output(
    record_list, stat_string
):
    """Generate expected output for the simple monitor view."""
    # Header part (same as in print_simple_monitor)
    header = (
        f"{stat_string}\n"
        f"{'':<4}{'user':<16}{'group':<16}{'id':<6}{'action':<16}{'job label':<16}"
        f"{'state':<23}{'last update':<20}\n"
    )

    # Generate the rows for each record in the list
    records_output = ""
    for record in record_list:
        state_name = record.get_state().name  # Get the state name
        job_label = record.job_label if "job_label" in record.__dict__ and record.job_label else ""
        
        records_output += (
            f"{'':<4}{record.user:<16}{record.group:<16}{record.id:<6}"
            f"{record.api_action:<16}{job_label:<16}{state_name:<23}"
            f"{record.creation_time}\n"
        )

    return header + records_output

def generate_complex_expected_output(record_list, stat_string):
    """Generate a multi-line expected output for complex monitor."""
    output = []

    # Append the stat_string first
    output.append(stat_string)

    # Loop over the records and generate the detailed output for each one
    for record in record_list:
        output.append("")  # Blank line before each record's details
        output.append(f"{'':<4}{'id':<16}: {record.id}")
        output.append(f"{'':<4}{'user':<16}: {record.user}")
        output.append(f"{'':<4}{'group':<16}: {record.group}")
        output.append(f"{'':<4}{'action':<16}: {record.api_action}")
        output.append(f"{'':<4}{'transaction id':<16}: {record.transaction_id}")
        output.append(f"{'':<4}{'creation time':<16}: {record.creation_time}")
        state = record.get_state()
        output.append(f"{'':<4}{'state':<16}: {state.name}")

        # Warnings (if they exist)
        if "warnings" in record.__dict__:
            warn_str = ""
            for w in record.warnings:
                warn_str += w + f"\n{'':<22}"
            output.append(f"{'':<4}{'warnings':<16}: {warn_str[:-23]}")

        # Sub records
        output.append(f"{'':<4}{'sub records':<16}->")
        for sr in record.sub_records:
            output.append(f"{'':4}{'+':<4} {'id':<13}: {sr.id}")
            output.append(f"{'':<9}{'sub_id':<13}: {sr.sub_id}")
            output.append(f"{'':<9}{'state':<13}: {sr.state.name}")
            output.append(f"{'':<9}{'last update':<13}: {sr.last_updated}")

            # Failed files (if they exist)
            if len(sr.failed_files) > 0:
                output.append(f"{'':<9}{'failed files':<13}->")
                for ff in sr.failed_files:
                    output.append(f"{'':<9}{'+':<4} {'filepath':<8} : {ff.filepath}")
                    output.append(f"{'':<9}{'':>4} {'reason':<8} : {ff.reason}")

    # Join all the lines together with a newline character
    return "\n".join(output)

# # Test with overriding multiple attributes of the sample_record
# def test_query_with_modified_group_and_user():
#     """Test querying the monitor database with a modified user and group."""

#     monitor = mock_monitor()

#     # Modify the sample record user and group
#     record_1 = sample_record(sub_record_num=2)
#     record_2 = sample_record(id=3, sub_record_num=3, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfca")

#     # Add the modified sample transaction record
#     monitor.session.add(record_1)
#     monitor.session.add(record_2)
#     monitor.session.commit()

#     # Query for the modified user
#     result = monitor.session.query(TransactionRecord).filter(
#         TransactionRecord.user == "existing_user"
#     ).all()

#     for i in result:
#         print(i.__dict__)
#         print("")
#         for x in i.sub_records:
#             print(x.__dict__)
#             print("")
#         print("")
#         print("")

#     # Assert that the modified record is found
#     assert len(result) == 2
#     assert result[0].user == "existing_user"

def test_query_with_time_range(monkeypatch):
    """Test querying the database with start and end times using Click."""
    
    monitor = mock_monitor()
    
    # Add a sample transaction record to the mock_monitor
    uuid_list_1 = generate_uuid_list(1)
    uuid_list_2 = generate_uuid_list(1)
    monitor.session.add(sample_record(id=1, user="existing_user", uuid=uuid_list_1))
    monitor.session.add(sample_record(id=2, user="second_user", uuid=uuid_list_2))
    monitor.session.add(sample_record(id=3))  # Another record for 'existing_user'
    monitor.session.commit()

    # Prepare runner
    runner = CliRunner()

    # Calculate start_time and end_time
    start_time = (datetime.now() - timedelta(days=30)).replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S')
    end_time = datetime.now().strftime('%Y-%m-%d')

    monkeypatch.setattr(nlds_monitor, "connect_to_monitor", lambda: monitor)
    # Invoke the query_monitor_db function using the CLI runner
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            #"--start-time", start_time,
            #"--end-time", end_time,
            "--user", "existing_user"
        ],
    )

    # Assert no errors
    assert result.exit_code == 0
    
    
    expected_output = generate_simple_expected_output(
        record_list=[sample_record(id=1), sample_record(id=3, user="existing_user")],
        stat_string=f"State of transactions for user: existing_user, from: {start_time}, order: ascending"
    )
    
    # Assert the output from click.echo matches expectations
    assert expected_output in result.output
    
    # End database session
    monitor.end_session






"""
make a generated list of uuids that are created for each record inside the pytest
"""





# TODO add warnings
# TODO why is failed_files a list? Does this need multiple values?



"""
user - correct/ incorect
group - correct/ incorect
state - correct/ incorect
record_state - correct/ incorect                  (complex view)
id - correct/ incorect
transaction_id - correct/ incorect

{
    start_time/ end_time:
        left blank
        start time changed and correct
        endtime exists and correct
        endtime before start time
        time not a datatime
}

complex - on/ off                                 (complex view)
order - ascending/ descending/ neither

filtering on a thing that doesn’t exist

make sure warnings work                           (complex view)
make sure failed files                            (complex view)
"""