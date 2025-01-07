#! /usr/bin/env python
# encoding: utf-8
"""
view_holding_full.py

View all the information for a Holding, including the Transactions, Tags, Files,
Locations, Aggregations.
"""
__author__ = "Neil Massey"
__date__ = "23 Sep 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import click

from nlds_processors.catalog.catalog_models import (
    Storage,
    Holding,
    Tag,
    Transaction,
    File,
    Location,
    Aggregation,
)
from reset_storage_status import _connect_to_catalog


def integer_permissions_to_string(intperm):
    octal = oct(intperm)[2:]
    result = ""
    value_letters = [(4, "r"), (2, "w"), (1, "x")]
    # Iterate over each of the digits in octal
    for digit in [int(n) for n in str(octal)]:
        # Check for each of the permissions values
        for value, letter in value_letters:
            if digit >= value:
                result += letter
                digit -= value
            else:
                result += "-"
    return result


def pretty_size(size):
    """Returns file size in human readable format"""

    suffixes = [
        ("B", 1),
        ("K", 1000),
        ("M", 1000000),
        ("G", 1000000000),
        ("T", 1000000000000),
    ]
    level_up_factor = 2000.0
    for suf, multipler in suffixes:
        if float(size) / multipler > level_up_factor:
            continue
        else:
            return round(size / float(multipler), 2).__str__() + suf
    return round(size / float(multipler), 2).__str__() + suf


def print_holding(holding: Holding):
    click.echo(f"{'':<2}└─┐ {'id':<17}: {holding.id}")
    click.echo(f"{'':<4}├─ {'label':<16}: {holding.label}")
    click.echo(f"{'':<4}├─ {'user':<16}: {holding.user}")
    click.echo(f"{'':<4}├─ {'group':<16}: {holding.group}")


def print_tag(tag: Tag):
    click.echo(f"{'':<6}├─┐ {'id':<17}: {tag.id}")
    click.echo(f"{'':<6}│ ├─ {'key':<16}: {tag.key}")
    click.echo(f"{'':<6}│ ├─ {'value':<16}: {tag.value}")


def print_transaction(transaction: Transaction):
    click.echo(f"{'':<6}├─┐ {'id':<17}: {transaction.id}")
    click.echo(f"{'':<6}│ ├─ {'transaction id':<16}: {transaction.transaction_id}")
    click.echo(f"{'':<6}│ ├─ {'ingest time':<16}: {transaction.ingest_time}")


def print_file(file: File):
    click.echo(f"{'':<6}│ │ ├─┐ {'id':<17}: {file.id}")
    click.echo(f"{'':<6}│ │ │ ├─ {'path':<16}: {file.original_path}")
    click.echo(f"{'':<6}│ │ │ ├─ {'type':<16}: {file.path_type}")
    if file.link_path:
        click.echo(f"{'':<6}│ │ │ ├─ {'link path':<16}: {file.link_path}")
    click.echo(f"{'':<6}│ │ │ ├─ {'size':<16}: {pretty_size(file.size)}")
    click.echo(f"{'':<6}│ │ │ ├─ {'uid':<16}: {file.user}")
    click.echo(f"{'':<6}│ │ │ ├─ {'gid':<16}: {file.group}")
    click.echo(
        f"{'':<6}│ │ │ ├─ {'permissions':<16}: "
        f"{integer_permissions_to_string(file.file_permissions)}"
    )

def print_location(location: Location):
    click.echo(f"{'':<6}│ │ │   ├─┐ {'id':<17}: {location.id}")
    click.echo(f"{'':<6}│ │ │   │ ├─ {'type':<16}: {str(location.storage_type)}")
    click.echo(f"{'':<6}│ │ │   │ ├─ {'url scheme':<16}: {location.url_scheme}")
    click.echo(f"{'':<6}│ │ │   │ ├─ {'url netloc':<16}: {location.url_netloc}")
    click.echo(f"{'':<6}│ │ │   │ ├─ {'root':<16}: {location.root}")
    click.echo(f"{'':<6}│ │ │   │ ├─ {'path':<16}: {location.path}")
    click.echo(f"{'':<6}│ │ │   │ └─ {'access time':<16}: {location.access_time}")

def print_aggregation(aggregation: Aggregation):
    click.echo(f"{'':<8}├─┐ {'id':<16}: {aggregation.id}")
    click.echo(f"{'':<8}│ ├─ {'tarfile':<16}: {aggregation.tarname}")
    click.echo(f"{'':<8}│ ├─ {'checksum':<16}: {aggregation.checksum}")
    click.echo(f"{'':<8}│ └─ {'algorithm':<16}: {aggregation.algorithm}")

def print_file_compact_header():
    click.echo(
        f"{'':<6}{'':<4}{'Original Path':<48}{'User':<6}{'Group':<7}"
        f"{'Size':<8}{'Type':<8}{'Location':<24}{'Aggregation':48}"
    )

def print_file_compact(file: File, nlds_cat: object):
    location_str = ""
    for l in file.locations:
        location_str += str(l.storage_type) + ", "
    location_str = location_str[:-2]
    output_str = (
        f"{'':<6}{'':<4}{file.original_path:<48}{file.user:<6}{file.group:<7}"
        f"{pretty_size(file.size):<8}{file.path_type:<8}{location_str:<24}"
    )
    for l in file.locations:
        if l.storage_type == Storage.TAPE:
            agg = nlds_cat.get_aggregation(l.aggregation_id)
            output_str += f"{agg.tarname:<32}"

    click.echo(output_str)


@click.command()
@click.option(
    "-u",
    "--user",
    default=None,
    type=str,
    help="The username to view the holding for.",
)
@click.option(
    "-g", "--group", default=None, type=str, help="The group to view the holding for."
)
@click.option(
    "-i",
    "--holding_id",
    default=None,
    type=int,
    help="The numeric id of the holding to view.",
)
@click.option(
    "-C",
    "--compact",
    is_flag=True,
    default=False,
    type=bool,
    help="Display in compact format, one file per line"
)
def view_holding(user: str, group: str, holding_id: int, compact: bool) -> None:
    """View the full Holding, including Transactions, Tags, Files, Locations and
    Aggregations."""
    if user is None:
        raise click.UsageError("Error - user not specified")
    if group is None:
        raise click.UsageError("Error - group not specified")
    if holding_id is None:
        raise click.UsageError("Error - holding id not specified")
    
    nlds_cat = _connect_to_catalog()
    nlds_cat.start_session()

    holding = nlds_cat.get_holding(user=user, group=group, holding_id=holding_id)[0]
    click.echo(f"{'':<2}┐ Holding")
    print_holding(holding)
    click.echo(f"{'':<4}├─┐ Tags")
    for tag in holding.tags:
        print_tag(tag)
    click.echo(f"{'':<4}└─┐ Transactions")
    for t in holding.transactions:
        print_transaction(t)
        click.echo(f"{'':<6}│ ├─┐ Files")
        aggregations = []
        if compact:
            print_file_compact_header()
        for f in t.files:
            if compact:
                print_file_compact(f, nlds_cat)
                continue
            if f is None:
                click.echo(f"{'':<8}** None **")
                continue
            print_file(f)
            click.echo(f"{'':<6}│ │ │ └─┐ Locations")
            for l in f.locations:
                print_location(l)
                # keep a track of aggregations
                if l.storage_type == Storage.TAPE:
                    agg = nlds_cat.get_aggregation(l.aggregation_id)
                    if l is not None and agg not in aggregations:
                        aggregations.append(agg)
        if not compact:
            click.echo(f"{'':<6}│ └─┐ Aggregations")
            for a in aggregations:
                if a is None:
                    click.echo(f"{'':<8}** None **")
                    continue
                print_aggregation(a)
                click.echo(f"{'':<8}└─┐ {' Files':<16}")
                for l in a.locations:
                    f = File(id=l.file_id)
                    f = nlds_cat.get_location_file(l)
                    click.echo(f"{'':<12}{f.original_path}")

    nlds_cat.end_session()


if __name__ == "__main__":
    view_holding()
