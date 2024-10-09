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

def query_monitor_db(user, group, state, record_state, id, start_time, end_time, order):
    """Connects to the monitor database"""
    config = CFG.load_config()

    db_engine = config["monitor_q"]["db_engine"]
    db_options = config["monitor_q"]["db_options"]
    db_options['echo'] = False
    nlds_monitor = Monitor(db_engine=db_engine, db_options=db_options)
    db_connect = nlds_monitor.connect(create_db_fl=False)

    nlds_monitor.start_session()

    """Returns a list of TransactionRecords"""
    query = nlds_monitor.session.query(TransactionRecord).options(
        joinedload(TransactionRecord.sub_records).joinedload(SubRecord.failed_files),
        joinedload(TransactionRecord.warnings),
    )
    if id:
        query = query.filter(TransactionRecord.id == id)
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

    if order == 'ascending':
        query = query.order_by(asc(TransactionRecord.creation_time))
    elif order == 'descending':
        query = query.order_by(desc(TransactionRecord.creation_time))

    trec = query.all()

    nlds_monitor.end_session()
    
    if record_state:
        for record in trec[:]:
            state = record.get_state()
            if state != record_state:
                trec.remove(record)

    return(trec)

def print_simple_monitor(record_list, stat_string):
    """Print a multi-line set of status for monitor"""
    click.echo(stat_string)
    click.echo(
            f"{'':<4}{'user':<16}{'id':<6}{'action':<16}{'job label':<16}"
            f"{'state':<23}{'last update':<20}"
        )
    for record in record_list:
        state = record.get_state()
        if "job_label" in record.__dict__ and record.__dict__["job_label"]:
            job_label = record.job_label
        else:
            job_label = ""
        click.echo(
            f"{'':<4}{record.user:<16}{record.id:<6}{record.api_action:<16}"
            f"{job_label:16}{state.name:<23}{(record.creation_time)}"
        )

def print_complex_monitor(record_list, stat_string):
    """Print a multi-line set of status for monitor in more detail, with a list of
    failed files if necessary"""
    click.echo(stat_string)
    for record in record_list:
        click.echo("")
        click.echo(f"{'':<4}{'id':<16}: {record.id}")
        click.echo(f"{'':<4}{'user':<16}: {record.user}")
        click.echo(f"{'':<4}{'group':<16}: {record.group}")
        click.echo(f"{'':<4}{'action':<16}: {record.api_action}")
        click.echo(f"{'':<4}{'transaction id':<16}: {record.transaction_id}")
        click.echo(f"{'':<4}{'creation time':<16}: {(record.creation_time)}")
        state = record.get_state()
        click.echo(f"{'':<4}{'state':<16}: {state.name}")
        if "warnings" in record.__dict__:
            warn_str = ""
            for w in record.warnings:
                warn_str += w + f"\n{'':<22}"
            click.echo(f"{'':<4}{'warnings':<16}: {warn_str[:-23]}")

        click.echo(f"{'':<4}{'sub records':<16}->")
        for sr in record.sub_records:
            sr_dict = sr.__dict__
            click.echo(f"{'':4}{'+':<4} {'id':<13}: {sr.id}")
            click.echo(f"{'':<9}{'sub_id':<13}: {sr.sub_id}")
            click.echo(f"{'':<9}{'state':<13}: {sr.state.name}")
            click.echo(f"{'':<9}{'last update':<13}: {(sr.last_updated)}")

            if len(sr_dict["failed_files"]) > 0:
                click.echo(f"{'':<9}{'failed files':<13}->")
                for ff in sr.failed_files:
                    click.echo(f"{'':<9}{'+':<4} {'filepath':<8} : {ff.filepath}")
                    click.echo(f"{'':<9}{'':>4} {'reason':<8} : {ff.reason}")

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
    "-st",
    "--start-time",
    default=(datetime.now() - timedelta(days=30)),
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
    
    if start_time and end_time:
        if start_time > end_time:
            click.echo("Error: Start time must be before end time.")
            return

    if state:
        try:
            state = State[state.upper()]
        except KeyError:
            click.echo(f"Invalid state: {state}")
            return

    if record_state:
        try:
            record_state = State[record_state.upper()]
        except KeyError:
            click.echo(f"Invalid state: {record_state}")
            return

    query = query_monitor_db(
        user,
        group,
        state,
        record_state,
        id,
        start_time,
        end_time,
        order,
    )


    details = []

    # Check if each value is provided and add to details
    stat_string = "State of transactions for "
    if id:
        details.append(f"id: {id}")
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
        if start_time and not end_time:
            details.append(f"from: {start_time}")

    # If no details were added, set req_details to 'all records'
    if not details:
        req_details = "all records"
    else:
        # Join the provided details with commas
        req_details = ", ".join(details)

    # Add the order
    if order:
        req_details += f", order: {order}"

    stat_string += req_details

    if complex or id:
        print_complex_monitor(query, stat_string)
    else:
        print_simple_monitor(query, stat_string)




if __name__ == "__main__":
    view_jobs()

                                                                                # TODO add pytests


                                                                                # TODO consider how this would work as a website


# cd nlds/nlds_utils
# python nlds_monitor.py