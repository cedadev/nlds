#! /usr/bin/env python
# encoding: utf-8
"""
reset_tape_status.py
"""
__author__ = "Neil Massey"
__date__ = "18 Sep 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from uuid import uuid4
import json

import click

from nlds_processors.catalog.catalog import Catalog
from nlds_processors.catalog.catalog_models import Storage
import nlds.server_config as CFG

@click.command()
@click.option(
    "-u", "--user", default=None, type=str, help="The username to reset holdings for."
)
@click.option(
    "-g", "--group", default=None, type=str, help="The group to reset holdings for."
)
@click.option(
    "-i",
    "--holding_id",
    default=None,
    type=int,
    help="The numeric id of the holding to reset tape archive entries for.",
)
def reset_tape_status(user: str, group: str, holding_id: int):
    """Reset the tape status of a file by deleting a STORAGE LOCATION associated
    with a file, if the details in the STORAGE LOCATION are empty.
    """
    if user is None:
        raise click.UsageError("Error - user not specified")
    if group is None:
        raise click.UsageError("Error - group not specified")
    if holding_id is None:
        raise click.UsageError("Error - holding id not specified")
    config = CFG.load_config()

    db_engine = config["catalog_q"]["db_engine"]
    db_options = config["catalog_q"]["db_options"]
    nlds_cat = Catalog(db_engine=db_engine, db_options=db_options)
    db_connect = nlds_cat.connect(create_db_fl=False)
    nlds_cat.start_session()
    holding = nlds_cat.get_holding(user=user, group=group, holding_id=holding_id)[0]
    # get the locations
    for t in holding.transactions:
        for f in t.files:
            for l in f.locations:
                if (l.storage_type == Storage.TAPE and 
                    l.url_scheme == "" and 
                    l.url_netloc == "" and 
                    l.root == ""):
                    nlds_cat.delete_location(f, Storage.TAPE)
    nlds_cat.save()
    nlds_cat.end_session()

if __name__ == "__main__":
    reset_tape_status()
