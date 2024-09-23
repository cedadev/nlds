#! /usr/bin/env python
# encoding: utf-8
"""
view_aggregations.py

View the aggregations for a Holding.
"""
__author__ = "Neil Massey"
__date__ = "23 Sep 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from uuid import uuid4
import json

import click

from nlds_processors.catalog.catalog_models import Storage
from reset_tape_status import _connect_to_catalog


@click.command()
@click.option(
    "-u",
    "--user",
    default=None,
    type=str,
    help="The username to view aggregations for.",
)
@click.option(
    "-g", "--group", default=None, type=str, help="The group to view aggregations for."
)
@click.option(
    "-i",
    "--holding_id",
    default=None,
    type=int,
    help="The numeric id of the holding to view aggregations for.",
)
def view_aggregations(user: str, group: str, holding_id: int) -> None:
    """View the aggregations for a Holding"""
    if user is None:
        raise click.UsageError("Error - user not specified")
    if group is None:
        raise click.UsageError("Error - group not specified")
    if holding_id is None:
        raise click.UsageError("Error - holding id not specified")

    nlds_cat = _connect_to_catalog()
    nlds_cat.start_session()

    holding = nlds_cat.get_holding(user=user, group=group, holding_id=holding_id)[0]
    print(f"Holding : {holding.id}")
    print(f"User    : {holding.user}")
    print(f"Group   : {holding.group}")
    print(f"Label   : {holding.label}")
    print(f"Transactions : ")
    for t in holding.transactions:
        print(f"  Tran id     : {t.id}")
        print(f"  UUID        : {t.transaction_id}")
        print(f"  Ingest time : {t.ingest_time}")
        # only need the first file to find the aggregation
        f0 = t.files[0]
        for l in f0.locations:
            if l.storage_type == Storage.TAPE:
                agg = nlds_cat.get_aggregation(l.aggregation_id)
        print(f"    Tarfile  : {agg.tarname}")
        print(f"    Checksum : {agg.checksum}")
        print(f"    Algol    : {agg.algorithm}")
        print(f"    Files :")
        for f in t.files:
            print(f"     {f.original_path}")
    nlds_cat.end_session()


if __name__ == "__main__":
    view_aggregations()
