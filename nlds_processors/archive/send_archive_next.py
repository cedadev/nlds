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
def send_archive_next():
    CRONJOB_CONFIG_SECTION = "cronjob_publisher"
    DEFAULT_CONFIG = {
        MSG.ACCESS_KEY: None,
        MSG.SECRET_KEY: None,
        MSG.TAPE_URL: None,
        MSG.TENANCY: None,
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
            **cronjob_config,
        },
        MSG.DATA: {
            # Convert to PathDetails for JSON serialisation
            MSG.FILELIST: [],
        },
        MSG.META: {
            # Insert an empty meta dict
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    routing_key = f"{RK.ROOT}.{RK.CATALOG_ARCHIVE_NEXT}.{RK.START}"

    click.echo(f"Sending message to {routing_key}: \n{json.dumps(msg_dict, indent=4)}")
    rabbit_publisher.publish_message(routing_key, msg_dict)
    click.echo("Message sent!")

if __name__ == "__main__":
    send_archive_next()
