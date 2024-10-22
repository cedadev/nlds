import click
from datetime import datetime, timedelta

from sqlalchemy.orm import joinedload
from sqlalchemy import between, asc, desc

from nlds_processors.monitor.monitor import Monitor
from nlds_processors.monitor.monitor_models import (
    TransactionRecord,
    SubRecord,
)
from nlds.rabbit.consumer import State
import nlds.server_config as CFG


def connect_to_monitor():
    """Connects to the monitor database"""
    config = CFG.load_config()
    db_engine = config["monitor_q"]["db_engine"]
    db_options = config["monitor_q"]["db_options"]
    db_options["echo"] = False

    nlds_monitor = Monitor(db_engine=db_engine, db_options=db_options)
    nlds_monitor.connect(create_db_fl=False)
    nlds_monitor.start_session()

    return nlds_monitor


def query_monitor_db(
    session,
    user,
    group,
    state,
    record_state,
    id,
    transaction_id,
    start_time,
    end_time,
    order,
):
    """Returns a list of TransactionRecords"""
    query = session.session.query(TransactionRecord).options(
        joinedload(TransactionRecord.sub_records).joinedload(SubRecord.failed_files),
        joinedload(TransactionRecord.warnings),
    )
    if id:
        query = query.filter(TransactionRecord.id == id)
    elif transaction_id:
        query = query.filter(TransactionRecord.transaction_id == transaction_id)
    else:
        if user:
            query = query.filter(TransactionRecord.user == user)

        if group:
            query = query.filter(TransactionRecord.group == group)

        if state:
            query = query.join(
                SubRecord, TransactionRecord.id == SubRecord.transaction_record_id
            )
            query = query.filter(SubRecord.state == state)

        if start_time and not end_time:
            query = query.filter(TransactionRecord.creation_time >= start_time)
        elif start_time and end_time:
            query = query.filter(
                between(TransactionRecord.creation_time, start_time, end_time)
            )

    if order == "ascending":
        query = query.order_by(asc(TransactionRecord.creation_time))
    elif order == "descending":
        query = query.order_by(desc(TransactionRecord.creation_time))

    trec = query.all()

    session.end_session()

    if record_state:
        for record in trec[:]:
            sr_list = record.sub_records
            hit = False
            for sr in sr_list:
                if sr.state == record_state:
                    hit = True
                    continue
            if hit == False:
                trec.remove(record)
    return trec


def print_simple_monitor(records_dict_list, stat_string):
    """Print a multi-line set of status for monitor"""
    click.echo(stat_string)
    click.echo(
        f"{'':<4}{'user':<16}{'group':<16}{'id':<6}{'action':<16}{'job label':<16}"
        f"{'state':<23}{'last update':<20}"
    )
    for record in records_dict_list:
        click.echo(
            f"{'':<4}{record['user']:<16}{record['group']:<16}{record['id']:<6}"
            f"{record['api_action']:<16}{record['job_label']:16}{record['state']:<23}"
            f"{(record['creation_time'])}"
        )


def print_complex_monitor(records_dict_list, stat_string):
    """Print a multi-line set of status for monitor in more detail, with a list of
    failed files if necessary"""
    click.echo(stat_string)
    for record in records_dict_list:
        click.echo("")
        click.echo(f"{'':<4}{'id':<16}: {record['id']}")
        click.echo(f"{'':<4}{'user':<16}: {record['user']}")
        click.echo(f"{'':<4}{'group':<16}: {record['group']}")
        click.echo(f"{'':<4}{'action':<16}: {record['api_action']}")
        click.echo(f"{'':<4}{'transaction id':<16}: {record['transaction_id']}")
        click.echo(f"{'':<4}{'creation time':<16}: {(record['creation_time'])}")
        click.echo(f"{'':<4}{'state':<16}: {record['state']}")

        warn_str = ""
        for w in record["warnings"]:
            warn_str += w["warning"] + f"\n{'':<22}"
        click.echo(f"{'':<4}{'warnings':<16}: {warn_str[:-23]}")

        click.echo(f"{'':<4}{'sub records':<16}->")
        for sr in record["sub_records"]:
            click.echo(f"{'':4}{'+':<4} {'id':<13}: {sr['id']}")
            click.echo(f"{'':<9}{'sub_id':<13}: {sr['sub_id']}")
            click.echo(f"{'':<9}{'state':<13}: {sr['state']}")
            click.echo(f"{'':<9}{'last update':<13}: {(sr['last_updated'])}")

            if len(sr["failed_files"]) > 0:
                click.echo(f"{'':<9}{'failed files':<13}->")
                for ff in sr["failed_files"]:
                    click.echo(f"{'':<9}{'+':<4} {'filepath':<8} : {ff['filepath']}")
                    click.echo(f"{'':<9}{'':>4} {'reason':<8} : {ff['reason']}")


def validate_inputs(start_time, end_time, state, record_state):
    """Ensures user inputs are valid before querying the database"""
    try:
        if start_time and end_time:
            if start_time > end_time:
                raise ValueError("Error: Start time must be before end time.")

        if state:
            try:
                state = State[state.upper()]
            except KeyError:
                raise ValueError(f"Error: Invalid state: {state}")

        if record_state:
            try:
                record_state = State[record_state.upper()]
            except KeyError:
                raise ValueError(f"Error: Invalid state: {record_state}")
    except ValueError as e:
        raise SystemExit(e)
    return state, record_state


def construct_stat_string(
    id,
    transaction_id,
    user,
    group,
    state,
    record_state,
    start_time,
    end_time,
    order,
):
    """
    Constructs a status string based on the inputs and prints the response.
    """
    details = []

    # Construct details based on provided values
    stat_string = "State of transactions for "
    if id:
        details.append(f"id: {id}")
    elif transaction_id:
        details.append(f"transaction id: {transaction_id}")
    else:
        if user:
            details.append(f"user: {user}")
        if group:
            details.append(f"group: {group}")
        if state:
            details.append(f"state: {state.name}")
        if record_state:
            details.append(f"record state: {record_state.name}")
        if start_time and end_time:
            details.append(f"between: {start_time} and {end_time}")
        elif start_time and not end_time:
            details.append(f"from: {start_time}")

    # If no details were added, set req_details to "all records"
    if not details:
        req_details = "all records"
    else:
        # Join the provided details with commas
        req_details = ", ".join(details)

    # Add order to the request details
    if order:
        req_details += f", order: {order}"

    stat_string += req_details

    return stat_string


def construct_record_dict(
    query,
):
    """
    Turn the monitor models into a large list of dictionarys to make it easier
    for the fastAPI web page
    """
    records_dict = []

    for record in query:
        # Create an empty dictionary for the TransactionRecord
        record_dict = {}

        # Specify the order of fields
        field_order = [
            "id",
            "transaction_id",
            "user",
            "group",
            "job_label",
            "api_action",
            "creation_time",
            "state",
            "warnings",
            "sub_records",
        ]

        for key in field_order:
            if key == "state":
                state = record.get_state()
                record_dict[key] = state.name
            else:
                if hasattr(record, key):
                    value = getattr(record, key)
                    record_dict[key] = value

        # Convert sub_records to dictionaries
        if record.sub_records:
            record_dict["sub_records"] = []
            for sub_record in record.sub_records:
                # Create an empty dictionary for the SubRecord
                sub_record_dict = {}
                sub_field_order = [
                    "id",
                    "sub_id",
                    "state",
                    "retry_count",
                    "last_updated",
                    "failed_files",
                    "transaction_record_id",
                ]  # Specify sub_record field order

                for key in sub_field_order:
                    if hasattr(sub_record, key):
                        value = getattr(sub_record, key)
                        if key == "state":
                            sub_record_dict[key] = value.name
                        else:
                            sub_record_dict[key] = value

                # Convert failed_files to dictionaries
                if sub_record.failed_files:
                    sub_record_dict["failed_files"] = []
                    for failed_file in sub_record.failed_files:
                        failed_file_dict = {}
                        failed_field_order = [
                            "id",
                            "filepath",
                            "reason",
                            "sub_record_id",
                        ]  # Specify failed_file field order

                        for key in failed_field_order:
                            if hasattr(failed_file, key):
                                value = getattr(failed_file, key)
                                failed_file_dict[key] = value

                        sub_record_dict["failed_files"].append(failed_file_dict)
                else:
                    sub_record_dict["failed_files"] = (
                        []
                    )  # If there are no failed_files, assign an empty list

                # Append the constructed sub_record_dict to the sub_records list
                record_dict["sub_records"].append(sub_record_dict)

        else:
            record_dict["sub_records"] = (
                []
            )  # If there are no sub_records, assign an empty list

        # Convert warnings to dictionaries
        if record.warnings:
            record_dict["warnings"] = []
            for warning in record.warnings:
                warning_dict = {}
                warning_field_order = [
                    "id",
                    "warning",
                    "transaction_record_id",
                ]  # Specify warning field order

                for key in warning_field_order:
                    if hasattr(warning, key):
                        value = getattr(warning, key)
                        warning_dict[key] = value

                record_dict["warnings"].append(warning_dict)
        else:
            record_dict["warnings"] = (
                []
            )  # If there are no warnings, assign an empty list

        # Append the constructed record_dict to records_dict
        records_dict.append(record_dict)

    return records_dict


@click.command()
@click.option(
    "-u",
    "--user",
    default=None,
    type=str,
    help="Enter the name of the user to filter by.",
)
@click.option(
    "-g",
    "--group",
    default=None,
    type=str,
    help="Enter the group work space to filter by.",
)
@click.option(
    "-s",
    "--state",
    default=None,
    type=str,
    help="Will return any record with that state in any "
    "of its sub-records (not record).",
)
@click.option(
    "-rs",
    "--record-state",
    default=None,
    type=str,
    help="Will return any record with that state.",
)
@click.option(
    "-i",
    "--id",
    default=None,
    type=int,
    help="Display the selected record in complex view using id.",
)
@click.option(
    "-ti",
    "--transaction-id",
    default=None,
    type=str,
    help="Display the selected record in complex view using the transaction id.",
)
@click.option(
    "-st",
    "--start-time",
    default=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Filter start time in YYYY-MM-DD format "
    "(leave blank for values from 30 days ago)",
)
@click.option(
    "-et",
    "--end-time",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Filter end time in YYYY-MM-DD format (leave blank for values up to present)",
)
@click.option(
    "-c",
    "--complex",
    is_flag=True,
    default=False,
    help="Display results in detailed view",
)
@click.option(
    "-d/-a",
    "--descending/--ascending",
    "order",
    default=False,
    help="Switch between ascending and descending order.",
)
def view_jobs(
    user,
    group,
    state,
    record_state,
    id,
    transaction_id,
    start_time,
    end_time,
    complex,
    order,
) -> None:
    """Returns all NLDS jobs filtered by user options."""

    if not order:
        order = "ascending"
    else:
        order = "descending"

    state, record_state = validate_inputs(start_time, end_time, state, record_state)

    # Connect to the monitor database
    session = connect_to_monitor()
    query = query_monitor_db(
        session,
        user,
        group,
        state,
        record_state,
        id,
        transaction_id,
        start_time,
        end_time,
        order,
    )

    stat_string = construct_stat_string(
        id,
        transaction_id,
        user,
        group,
        state,
        record_state,
        start_time,
        end_time,
        order,
    )

    records_dict_list = construct_record_dict(query)

    if complex or id or transaction_id:
        print_complex_monitor(records_dict_list, stat_string)
    else:
        print_simple_monitor(records_dict_list, stat_string)


if __name__ == "__main__":
    view_jobs()

# python nlds_monitor.py
