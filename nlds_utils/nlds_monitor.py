import click
from datetime import datetime # imported for later feature

from nlds_processors.monitor.monitor_models import (
    TransactionRecord,
    SubRecord,
    FailedFile,
    Warning,
)

from sqlalchemy.orm import joinedload

from nlds_processors.monitor.monitor import Monitor
from nlds.rabbit.consumer import State
import nlds.server_config as CFG

def _connect_to_monitor(user, group, state, time, order):
    config = CFG.load_config()

    db_engine = config["monitor_q"]["db_engine"]
    db_options = config["monitor_q"]["db_options"]
    db_options['echo'] = False
    nlds_monitor = Monitor(db_engine=db_engine, db_options=db_options)
    db_connect = nlds_monitor.connect(create_db_fl=False)
    
    nlds_monitor.start_session()
    
    """returns a list of TransactionRecords"""
    query = nlds_monitor.session.query(TransactionRecord).options(
        joinedload(TransactionRecord.sub_records).joinedload(SubRecord.failed_files)
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
    
    #if order:
    #    query = query.filter(TransactionRecord.order == order)
    
    trec = query.all()
    
    nlds_monitor.end_session()
    
    return(trec)
    
    
    #return nlds_monitor

@click.command()
@click.option(
    "-u",
    "--user",
    default=None,
    type=str,
    help="Enter the name of the user to filter by (can be 'ALL').",
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
    help="Enter the state of the job to filter by.",
)
@click.option(                                                  # TODO (add aditional feature)
    "-t",
    "--time",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    help="DateTime in YYYY-MM-DD HH:MM:SS format",
)
@click.option(                                                  # TODO (implement)
    "-c",
    "--complex",
    default=False,
    type=bool,
    help="Display results in detailed view",
)
@click.option(                                                  # TODO (implement)
    "-a"
    "--ascending",
    "order",
    flag_value="ascending",
    default=True,
    help="Show in created time ascending order (default).",
)
@click.option(                                                  # TODO (implement)
    "-d",
    "--descending",
    "order",
    flag_value="descending",
    help="Show in created time descending order.",
)
def view_jobs(user, group, state, time, order) -> None:
    """Returns all NLDS jobs filtered by user options."""

    print(user)
    print(group)
    print(state)
    print(time)
    print(order)
    print("")
    print("")
    
    if state:
        try:
            state = State[state.upper()]
        except KeyError:
            print(f"Invalid state: {state}")
            exit()
    
    thing = _connect_to_monitor(user, group, state, time, order)
    
    print("")
    print("")
    
    for t in thing:
        print(t)
    
    #thing = t
    
    #print(thing)
    
    # thing = thing[0]
    # sub = thing.sub_records[0]
    
    # print(sub)
    
    
    #print("")
    #print(thing, " thing")
    #print(type(thing), " type")
    #print("")
    
    #for attr in dir(sub):
    #    print(attr)
    #    print(getattr(sub, attr))
    #    print("")
    


if __name__ == "__main__":
    view_jobs()




# Try switching off some of the microprocessors before doing a PUT as that will cause the transaction to stall and have a different state.

# filter a range of dates