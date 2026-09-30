# encoding: utf-8
"""
send_archive_next.py
NOTE: This module is imported into a revision, and so should be very defensive
with how it imports external modules (like xrootd).
"""

__author__ = "Jack Leland and Neil Massey"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from uuid import uuid4
import json

import click

from nlds.routers import rabbit_publisher
from nlds.rabbit.consumer import State

import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


@click.command()
@click.option(
    "-h",
    "--holding_id",
    default=None,
    type=int,
    help="The numeric id of an existing holding to put the file into.",
)
@click.option(
    "-p",
    "--tape_pool",
    default=None,
    type=str,
    help="The tape pool to use to archive the files to.",
)
@click.option(
    "-t",
    "--tenancy",
    default=None,
    type=str,
    help="The object store tenancy to use to archive the files from.",
)
@click.option(
    "-d",
    "--ingest_deadline",
    default=0,
    type=int,
    help=(
        "Deadline after which ingest to tape will be made for holdings.  Ingest will "
        "only be attempted after the time that all files in the holding completed "
        "transfer to the object store + the ingest deadline (in seconds)."
    ),
)
def send_archive_next(
    holding_id: int, tape_pool: str, tenancy: str, ingest_deadline: int
):
    print(holding_id, tape_pool, tenancy, ingest_deadline)
    CRONJOB_CONFIG_SECTION = "cronjob_publisher"
    DEFAULT_CONFIG = {
        # MSG.DETAILS section
        MSG.ACCESS_KEY: None,
        MSG.SECRET_KEY: None,
        MSG.TAPE_URL: None,
        MSG.TENANCY: None,
        # MSG.META section
        MSG.TAPE_POOL: None,
        MSG.TAPE_POOL_STRATEGY: None,
        MSG.ARCHIVE_INGEST_DEADLINE: 86400,  # 24 hours
    }
    # Load any cronjob config, if present
    cronjob_config = DEFAULT_CONFIG
    if CRONJOB_CONFIG_SECTION in rabbit_publisher.whole_config:
        cronjob_config |= rabbit_publisher.whole_config[CRONJOB_CONFIG_SECTION]

    uuid = str(uuid4())
    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: uuid,
            # for the root message, the sub_id is the transaction_id
            MSG.SUB_ID: uuid,
            MSG.TARGET: None,
            MSG.API_ACTION: RK.ARCHIVE_PUT,
            MSG.JOB_LABEL: "archive-next",
            MSG.USER: "admin-placeholder",
            MSG.GROUP: "admin-placeholder",
            MSG.STATE: State.ARCHIVE_INIT.value,
            MSG.ACCESS_KEY: cronjob_config[MSG.ACCESS_KEY],
            MSG.SECRET_KEY: cronjob_config[MSG.SECRET_KEY],
            MSG.TAPE_URL: cronjob_config[MSG.TAPE_URL],
            MSG.TENANCY: cronjob_config[MSG.TENANCY],
        },
        MSG.DATA: {
            # Convert to PathDetails for JSON serialisation
            MSG.FILELIST: [],
        },
        MSG.META: {
            MSG.TAPE_POOL_STRATEGY: cronjob_config[MSG.TAPE_POOL_STRATEGY],
            MSG.ARCHIVE_INGEST_DEADLINE: cronjob_config[MSG.ARCHIVE_INGEST_DEADLINE],
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    # add the holding id if it exists
    if holding_id:
        msg_dict[MSG.META][MSG.HOLDING_ID] = holding_id
    if tape_pool:
        msg_dict[MSG.META][MSG.TAPE_POOL_STRATEGY].insert(0, "user")
        msg_dict[MSG.META][MSG.TAPE_POOL] = tape_pool
    if tenancy:
        msg_dict[MSG.DETAILS][MSG.TENANCY] = tenancy
    if ingest_deadline:
        msg_dict[MSG.META][MSG.ARCHIVE_INGEST_DEADLINE] = ingest_deadline

    routing_key = f"{RK.ROOT}.{RK.CATALOG_ARCHIVE_NEXT}.{RK.START}"

    click.echo(f"Sending message to {routing_key}: \n{json.dumps(msg_dict, indent=4)}")
    rabbit_publisher.publish_message(routing_key, msg_dict)
    click.echo("Message sent!")


if __name__ == "__main__":
    send_archive_next()
