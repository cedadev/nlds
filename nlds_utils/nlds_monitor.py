import click
from datetime import datetime

from nlds_processors.monitor.monitor_models import (
    TransactionRecord,
    SubRecord,
    FailedFile,
    Warning,
)

from sqlalchemy.orm import joinedload
from sqlalchemy import asc, desc

from nlds_processors.monitor.monitor import Monitor
from nlds.rabbit.consumer import State
import nlds.server_config as CFG

def query_monitor_db(user, group, state, time, order):
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
    
    if user:
        print("user")
        query = query.filter(TransactionRecord.user == user)
    
    if group:
        print("group")
        query = query.filter(TransactionRecord.group == group)
    
    if state:
        print("state")
        query = query.join(SubRecord, TransactionRecord.id == SubRecord.transaction_record_id)
        query = query.filter(SubRecord.state == state)

    #if time:
    #    print("time")
    #    query = query.filter(TransactionRecord.creation_time == time)
    
    if order == 'ascending':
        query = query.order_by(asc(TransactionRecord.creation_time))
    elif order == 'descending':
        query = query.order_by(desc(TransactionRecord.creation_time))
    
    trec = query.all()
    
    nlds_monitor.end_session()
    
    return(trec)

def print_simple_monitor(record_list, req_details):
    """Print a multi-line set of status for monitor"""
    stat_string = "State of transactions for "
    stat_string += req_details
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

def print_complex_monitor(record_list, req_details):
    """Print a multi-line set of status for monitor in more detail, with a list of
    failed files if necessary"""
    stat_string = "State of transactions for "
    stat_string += req_details
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
            click.echo(f"{'':<4}{'warnings':<16}: {warn_str[:-23]}")            # TODO check this works with warnings

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
@click.option(                                                                  # TODO add an overall filter by state and not just sub-record
    "-s",
    "--state",
    default=None,
    type=str,
    help="Will return any record with that state in any of its sub-records (not record).",
)
@click.option(                                                                  # TODO (add aditional feature)
    "-i",
    "--id",
    default=None,
    type=int,
    help="Display the selected record in complex view using id.",
)
@click.option(                                                                  # TODO (add aditional feature), fix help statement
    "-t",
    "--time",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    help="DateTime in YYYY-MM-DD HH:MM:SS format",
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
def view_jobs(user, group, state, id, time, complex, order) -> None:
    """Returns all NLDS jobs filtered by user options."""
    
    if not order:
        order = "ascending"
    else:
        order = "descending"

    print(user)
    print(group)
    print(state)
    print(time)
    print(complex)
    print(order)
    print("")
    print("")
    
    if state:
        try:
            state = State[state.upper()]
        except KeyError:
            print(f"Invalid state: {state}")
            exit()
    
    query = query_monitor_db(user, group, state, time, order)
    
    
    details = []

    # Check if each value is provided and add to details
    if user:
        details.append(f"user: {user}")
    if group:
        details.append(f"group: {group}")
    if state:
        details.append(f"state: {state}")
    if time:
        details.append(f"time: {time}")
    
    # If no details were added, set req_details to 'all records'
    if not details:
        req_details = "all records"
    else:
        # Join the provided details with commas
        req_details = ", ".join(details)

    # Add the order
    if order:
        req_details += f", order: {order}"
    
    
    if complex:
        print_complex_monitor(query, req_details)
    else:
        print_simple_monitor(query, req_details)
    
    


if __name__ == "__main__":
    view_jobs()



                                                                                # TODO tidy up debug script
                                                                                # TODO tidy up old commented out code
                                                                                # TODO give functions a brief description
                                                                                # TODO clear up imports
                                                                                # TODO reformat everything to conform to pep8 (or whatever)






# Try switching off some of the microprocessors before doing a PUT as that will cause the transaction to stall and have a different state.


# cd nlds/nlds_utils
# python nlds_monitor.py