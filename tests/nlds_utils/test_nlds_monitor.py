import pytest
from click.testing import CliRunner
from datetime import datetime, timedelta

import uuid

from nlds_processors.monitor.monitor import Monitor
from nlds_processors.monitor.monitor_models import (
    TransactionRecord,
    SubRecord,
    FailedFile,
    Warning,
)
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


def generate_simple_expected_output(record_list, stat_string):
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
        job_label = (
            record.job_label
            if "job_label" in record.__dict__ and record.job_label
            else ""
        )

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
        # ensures warnings are loaded.
        # adding ', lazy="joined"' to warnings in the TransactionRecord model also works
        record.warnings

        output.append("")  # Blank line before each record's details
        output.append(f"{'':<4}{'id':<16}: {record.id}")
        output.append(f"{'':<4}{'user':<16}: {record.user}")
        output.append(f"{'':<4}{'group':<16}: {record.group}")
        output.append(f"{'':<4}{'action':<16}: {record.api_action}")
        output.append(f"{'':<4}{'job label':<16}: {record.job_label}")
        output.append(f"{'':<4}{'transaction id':<16}: {record.transaction_id}")
        output.append(f"{'':<4}{'last update':<16}: {record.creation_time}")
        state = record.get_state()
        output.append(f"{'':<4}{'state':<16}: {state.name}")

        # Warnings (if they exist)
        if "warnings" in record.__dict__:
            warn_str = ""
            for w in record.warnings:
                warn_str += w.warning + f"\n{'':<22}"
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


@pytest.fixture()
def start_time():
    return (
        (datetime.now() - timedelta(days=30))
        .replace(hour=0, minute=0, second=0)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


@pytest.fixture()
def setup_test(monkeypatch):
    monitor = mock_monitor()

    def _setup_test(record_list, output_records, stat_string, complex=False):
        # Add records to test database
        for record in record_list:
            monitor.session.add(record)
            for warning in record.warnings:
                monitor.session.add(warning)

        monitor.session.commit()

        # Mock connect_to_monitor to use the test database
        monkeypatch.setattr(nlds_monitor, "connect_to_monitor", lambda: monitor)

        runner = CliRunner()

        # Generate expected output
        if complex:
            expected_output = generate_complex_expected_output(
                record_list=output_records,
                stat_string=stat_string,
            )
        else:
            expected_output = generate_simple_expected_output(
                record_list=output_records,
                stat_string=stat_string,
            )
        return runner, expected_output

    yield _setup_test

    # Teardown database session
    monitor.end_session


def test_user_correct(setup_test, start_time):
    """Test filtering by an existing user and ensuring correct output."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2, user="user")
    record3 = sample_record(id=3)

    stat_string = (
        f"State of transactions for user: user, from: {start_time}, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=[record2],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-u",
            "user",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_user_nonexistent(setup_test, start_time):
    """Test filtering by a non-existent user and ensure no records are returned."""

    # Create records to test functionality
    record1 = sample_record()

    stat_string = (
        f"State of transactions for user: nonexistant-user, "
        f"from: {start_time}, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1], output_records=[], stat_string=stat_string
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-u",
            "nonexistant-user",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_group_correct(setup_test, start_time):
    """Test filtering by an existing group and ensuring correct output."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2, group="group")
    record3 = sample_record(id=3)

    stat_string = (
        f"State of transactions for group: group, from: {start_time}, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=[record2],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-g",
            "group",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_group_nonexistent(setup_test, start_time):
    """Test filtering by a non-existent group and ensure no records are returned."""

    # Create records to test functionality
    record1 = sample_record()

    stat_string = (
        f"State of transactions for group: nonexistant-group, "
        f"from: {start_time}, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1], output_records=[], stat_string=stat_string
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-g",
            "nonexistant-group",
        ],
    )
    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_state_correct(setup_test, start_time):
    """Test filtering by a valid state and ensuring correct output."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2, state=[101])
    record3 = sample_record(id=3)

    stat_string = (
        f"State of transactions for state: FAILED, from: {start_time}, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=[record2],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-s",
            "failed",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_state_nonexistent(setup_test, start_time):
    """Test filtering by a non-existent state and ensure no records are returned."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2)
    record3 = sample_record(id=3)

    stat_string = (
        f"State of transactions for state: FAILED, from: {start_time}, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=[],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-s",
            "failed",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_incorrect_state(setup_test, start_time):
    """Test handling when an invalid state is provided."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2)
    record3 = sample_record(id=3)

    stat_string = (
        f"State of transactions for state: FAILED, from: {start_time}, order: ascending"
    )

    runner, _ = setup_test(
        record_list=[record1, record2, record3],
        output_records=[],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-s",
            "fail",
        ],
    )

    expected_output = "Error: Invalid state: fail"

    # Assert has an error
    assert result.exit_code == 1

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_record_state_correct(setup_test, start_time):
    """Test the record state filtering with correct state in complex view."""

    # Create records to test functionality
    uuid_list_1 = generate_uuid_list(1)
    uuid_list_2 = generate_uuid_list(1)
    uuid_list_3 = generate_uuid_list(4)
    record1 = sample_record(id=1, warning=True, user="existing_user", uuid=uuid_list_1)
    record2 = sample_record(id=2, user="second_user", uuid=uuid_list_2)
    record3 = sample_record(id=3)
    record4 = sample_record(
        id=4, sub_record_num=4, uuid=uuid_list_3, state=[100, 100, 101, 100]
    )

    stat_string = (
        f"State of transactions for record state: FAILED, "
        f"from: {start_time}, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3, record4],
        output_records=[record4],
        stat_string=stat_string,
        complex=True,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-c",
            "-rs",
            "failed",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_record_state_nonexistent(setup_test, start_time):
    """Test the record state filtering with a non-existent state in complex view."""

    # Create records to test functionality
    uuid_list_1 = generate_uuid_list(1)
    uuid_list_2 = generate_uuid_list(1)
    uuid_list_3 = generate_uuid_list(4)
    record1 = sample_record(
        id=1, warning=True, user="existing_user", uuid=uuid_list_1, state=[3]
    )
    record2 = sample_record(id=2, user="second_user", uuid=uuid_list_2, state=[4])
    record3 = sample_record(id=3, state=[5])
    record4 = sample_record(
        id=4, sub_record_num=4, uuid=uuid_list_3, state=[6, 11, 101, 12]
    )

    stat_string = (
        f"State of transactions for record state: "
        f"COMPLETE, from: {start_time}, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3, record4],
        output_records=[],
        stat_string=stat_string,
        complex=True,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-c",
            "-rs",
            "COMPLETE",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_incorrect_record_state(setup_test, start_time):
    """Test handling when an invalid record state is provided in complex view."""

    # Create records to test functionality
    uuid_list_1 = generate_uuid_list(1)
    uuid_list_2 = generate_uuid_list(1)
    uuid_list_3 = generate_uuid_list(4)
    record1 = sample_record(
        id=1, warning=True, user="existing_user", uuid=uuid_list_1, state=[3]
    )
    record2 = sample_record(id=2, user="second_user", uuid=uuid_list_2, state=[4])
    record3 = sample_record(id=3, state=[5])
    record4 = sample_record(
        id=4, sub_record_num=4, uuid=uuid_list_3, state=[6, 11, 101, 12]
    )

    stat_string = (
        f"State of transactions for record state: COMPLETE, "
        f"from: {start_time}, order: ascending"
    )

    runner, _ = setup_test(
        record_list=[record1, record2, record3, record4],
        output_records=[],
        stat_string=stat_string,
        complex=True,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-c",
            "-rs",
            "success",
        ],
    )
    expected_output = "Error: Invalid state: success"

    # Assert has an error
    assert result.exit_code == 1

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_id_correct(setup_test):
    """Test filtering by a valid ID and ensuring correct output in complex view."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2)

    stat_string = f"State of transactions for id: 1, order: ascending"

    runner, expected_output = setup_test(
        record_list=[record1, record2],
        output_records=[record1],
        stat_string=stat_string,
        complex=True,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-i",
            1,
        ],
    )
    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_id_nonexistent(setup_test):
    """
    Test filtering by a non-existent ID and ensure no records are
    returned in complex view.
    """

    # Create records to test functionality
    record1 = sample_record()

    stat_string = f"State of transactions for id: 2, order: ascending"

    runner, expected_output = setup_test(
        record_list=[record1], output_records=[], stat_string=stat_string, complex=True
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-i",
            2,
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_id_wrong_datatype(setup_test, start_time):
    """Test handling when an invalid (wrong type) ID is provided in complex view."""

    # Create records to test functionality
    record1 = sample_record()

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, _ = setup_test(
        record_list=[record1], output_records=[record1], stat_string=stat_string
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-i",
            "id",
        ],
    )

    expected_output = (
        "Error: Invalid value for '-i' / '--id': 'id' is not a valid integer."
    )

    # Assert no errors
    assert result.exit_code == 2

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_transaction_id_correct(setup_test):
    """Test filtering by a valid transaction ID in complex view."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2, transaction_id="60a4e57b-7279-4a92-a224-6d0ececb6242")

    stat_string = (
        f"State of transactions for transaction id: "
        f"17e96f14-5f7b-49de-a309-b61e39a2cfcb, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1, record2],
        output_records=[record1],
        stat_string=stat_string,
        complex=True,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-ti",
            "17e96f14-5f7b-49de-a309-b61e39a2cfcb",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_transaction_id_nonexistent(setup_test):
    """Test filtering by a non-existent transaction ID in complex view."""

    # Create records to test functionality
    record1 = sample_record()

    stat_string = (
        f"State of transactions for transaction id: "
        f"60a4e57b-7279-4a92-a224-6d0ececb6242, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1], output_records=[], stat_string=stat_string, complex=True
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-ti",
            "60a4e57b-7279-4a92-a224-6d0ececb6242",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_time_left_blank(setup_test, start_time):
    """Test filtering with start time left blank. (no flags given)"""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2, creation_time=(datetime.now() - timedelta(days=2)))
    record3 = sample_record(
        id=3,
        creation_time=datetime(2023, 10, 2, 7, 46, 37),
    )

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=[record1, record2],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_time_start_time_correct(setup_test):
    """Test filtering with a valid start time."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2, creation_time=(datetime.now() - timedelta(days=4)))
    record3 = sample_record(
        id=3,
        creation_time=datetime(2023, 10, 2, 7, 46, 37),
    )

    start_time = datetime.now() - timedelta(days=3)

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=[record1],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        ["-st", (start_time)],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_time_end_time_correct(setup_test):
    """Test filtering with a valid end time."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2, creation_time=(datetime.now() - timedelta(days=4)))
    record3 = sample_record(
        id=3,
        creation_time=datetime(2023, 10, 2, 7, 46, 37),
    )

    start_time = datetime.now() - timedelta(days=5)
    end_time = datetime.now() - timedelta(days=2)

    stat_string = (
        f"State of transactions for between: {start_time} "
        f"and {end_time}, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=[record2],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-st",
            start_time,
            "-et",
            end_time,
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_time_endtime_before_start_time(setup_test):
    """Test handling of end time before the start time."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2, creation_time=(datetime.now() - timedelta(days=4)))
    record3 = sample_record(
        id=3,
        creation_time=datetime(2023, 10, 2, 7, 46, 37),
    )

    start_time = datetime.now() - timedelta(days=5)
    end_time = datetime.now() - timedelta(days=7)

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, _ = setup_test(
        record_list=[record1, record2, record3],
        output_records=[],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-st",
            start_time,
            "-et",
            end_time,
        ],
    )

    expected_output = "Error: Start time must be before end time."

    # Assert has an error
    assert result.exit_code == 1

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_time_not_datetime(setup_test, start_time):
    """Test handling of start/end time inputs that are not valid datetime objects."""

    # Create records to test functionality
    record1 = sample_record()

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, _ = setup_test(
        record_list=[record1], output_records=[], stat_string=stat_string
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-st",
            "yesterday",
        ],
    )

    expected_output = (
        "Error: Invalid value for '-st' / '--start-time': "
        "'yesterday' does not match the format '%Y-%m-%d'."
    )

    # Assert no errors
    assert result.exit_code == 2

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_non_complex_display(setup_test, start_time):
    """Test regular view and ensure correct output."""

    # Create records to test functionality
    record1 = sample_record()
    record2 = sample_record(id=2)
    record3 = sample_record(id=3)

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=[record1, record2, record3],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_complex_display(setup_test, start_time):
    """Test complex view and ensure correct output."""

    # Create records to test functionality
    uuid_list_1 = generate_uuid_list(4)
    record1 = sample_record()
    record2 = sample_record(id=2)
    record3 = sample_record(id=3, sub_record_num=4, uuid=uuid_list_1)

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=[record1, record2, record3],
        stat_string=stat_string,
        complex=True,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-c",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_order_ascending(setup_test, start_time):
    """Test ordering by creation time in ascending order."""

    # Create records to test functionality
    uuid_list_1 = generate_uuid_list(4)
    record1 = sample_record()
    record2 = sample_record(id=2)
    record3 = sample_record(id=3, sub_record_num=4, uuid=uuid_list_1)

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=[record1, record2, record3],
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-a",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_order_descending(setup_test, start_time):
    """Test ordering by creation time in descending order."""

    # Create records to test functionality
    uuid_list_1 = generate_uuid_list(4)
    record1 = sample_record(creation_time=(datetime.now() - timedelta(days=1)))
    record2 = sample_record(id=2, creation_time=(datetime.now() - timedelta(days=5)))
    record3 = sample_record(
        id=3,
        sub_record_num=4,
        uuid=uuid_list_1,
        creation_time=(datetime.now() - timedelta(days=7)),
    )

    # Expected records should be ordered by creation_time in descending order
    sorted_records = sorted(
        [record3, record2, record1], key=lambda r: r.creation_time, reverse=True
    )

    stat_string = f"State of transactions for from: {start_time}, order: descending"

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3],
        output_records=sorted_records,
        stat_string=stat_string,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-d",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_incorrect_command(setup_test, start_time):
    """
    Test handling of incorrect Click command and check for appropriate error message.
    """

    # Create records to test functionality
    record1 = sample_record()

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, _ = setup_test(
        record_list=[record1], output_records=[], stat_string=stat_string
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-z",
        ],
    )

    expected_output = (
        "Usage: view-jobs [OPTIONS]\n"
        "Try 'view-jobs --help' for help.\n\n"
        "Error: No such option: -z\n"
    )

    # Assert no errors
    assert result.exit_code == 2

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_warnings(setup_test, start_time):
    """Test that warning messages are displayed correctly in complex view."""

    # Create records to test functionality
    record1 = sample_record(warning=True)

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, expected_output = setup_test(
        record_list=[record1],
        output_records=[record1],
        stat_string=stat_string,
        complex=True,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-c",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_failed_files(setup_test, start_time):
    """Test that failed file messages are displayed correctly in complex view."""

    # Create records to test functionality
    record1 = sample_record(failed=True)

    stat_string = f"State of transactions for from: {start_time}, order: ascending"

    runner, expected_output = setup_test(
        record_list=[record1],
        output_records=[record1],
        stat_string=stat_string,
        complex=True,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-c",
        ],
    )

    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output


def test_multiple_filters(setup_test, start_time):
    """Test combining multiple filters to ensure correct output."""

    # Create records to test functionality
    uuid_list_1 = generate_uuid_list(1)
    uuid_list_2 = generate_uuid_list(1)
    uuid_list_3 = generate_uuid_list(4)
    record1 = sample_record(
        id=1, warning=True, failed=True, user="second_user", uuid=uuid_list_1
    )
    record2 = sample_record(id=2, user="second_user", uuid=uuid_list_2)
    record3 = sample_record(id=3)
    record4 = sample_record(id=4, sub_record_num=4, uuid=uuid_list_3)

    stat_string = (
        f"State of transactions for user: second_user, group: test_group, "
        f"state: COMPLETE, from: {start_time}, order: ascending"
    )

    runner, expected_output = setup_test(
        record_list=[record1, record2, record3, record4],
        output_records=[record1, record2],
        stat_string=stat_string,
        complex=True,
    )

    # get output from nlds_monitoring.py
    result = runner.invoke(
        nlds_monitor.view_jobs,
        [
            "-c",
            "-s",
            "complete",
            "-u",
            "second_user",
            "-g",
            "test_group",
        ],
    )
    # Assert no errors
    assert result.exit_code == 0

    # Assert the output from click.echo matches expectations
    assert expected_output in result.output
