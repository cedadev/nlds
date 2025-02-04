#! /usr/bin/env python
# encoding: utf-8
"""
reset_tape_status.py

This should be used if a file is marked as TAPE but the copy to tape did not succeed.
This results in url_scheme, url_netloc and root being null strings ("").
These are checked before the TAPE location is removed, unless --force option is 
supplied.
"""
__author__ = "Neil Massey"
__date__ = "24 Sep 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import click
import minio

from nlds_processors.catalog.catalog import Catalog
from nlds_processors.catalog.catalog_models import Storage, File, Location
from nlds.details import PathDetails
import nlds.server_config as CFG


def _connect_to_catalog():
    config = CFG.load_config()

    db_engine = config["catalog_q"]["db_engine"]
    db_options = config["catalog_q"]["db_options"]
    db_options["echo"] = False
    nlds_cat = Catalog(db_engine=db_engine, db_options=db_options)
    db_connect = nlds_cat.connect(create_db_fl=False)
    return nlds_cat


def _connect_to_s3(access_key: str, secret_key: str):
    # get the tenancy from the server config
    config = CFG.load_config()
    tenancy = config["transfer_put_q"]["tenancy"]
    client = minio.Minio(
        tenancy,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
    return client


def _remove_location_from_file(
    file: File,
    location: Location,
    loc_type: Storage,
    force: bool,
    delete: bool,
    s3_client,
    nlds_cat: Catalog,
):
    delloc = (
        location.url_scheme == "" and location.url_netloc == "" and location.root == ""
    ) or force
    if location.storage_type == loc_type:
        if delloc:
            if delete and s3_client is not None:
                pd = PathDetails.from_filemodel(file)
                # delete from object storage -
                s3_client.remove_object(pd.bucket_name, pd.object_name)
                click.echo(f"Deleted object: {pd.get_object_store().url}")
            # delete from catalog
            nlds_cat.delete_location(file, loc_type)
            click.echo(f"Removed {loc_type} location for {file.original_path}")
            if loc_type == Storage.TAPE and location.aggregation_id is not None:
                agg = nlds_cat.get_aggregation(location.aggregation_id)
                nlds_cat.delete_aggregation(agg)
                click.echo(f"Removed TAPE aggregation for {file.original_path}")
        else:
            click.echo(
                f"Location URL details not empty for the file {file.original_path} and "
                f" force not set in command line options.  Skipping."
            )


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
    help="The numeric id of the holding to reset Storage Location entries for.",
)
@click.option(
    "-a",
    "--access_key",
    default=None,
    type=str,
    help="Access key for user's object storage access",
)
@click.option(
    "-s",
    "--secret_key",
    default=None,
    type=str,
    help="Secret key for user's object storage access",
)
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    help="Force the deletion of the Storage Location record",
)
@click.option(
    "-d",
    "--delete",
    default=False,
    is_flag=True,
    help="Delete the associated object(s) from the object storage only",
)
@click.option(
    "-l",
    "--location",
    default=None,
    type=str,
    help="Storage Location type to delete records for.  OBJECT_STORAGE|TAPE",
)
@click.option(
    "-S",
    "--settings",
    default="",
    type=str,
    help="The location of the settings file for NLDS.",
)
def reset_storage_status(
    user: str,
    group: str,
    holding_id: int,
    access_key: str,
    secret_key: str,
    force: bool,
    delete: bool,
    location: str,
    settings: str,
) -> None:
    """Reset the tape status of a file by deleting a STORAGE LOCATION associated
    with a file, if the details in the STORAGE LOCATION are empty.
    """
    if user is None:
        raise click.UsageError("Error - user not specified")
    if group is None:
        raise click.UsageError("Error - group not specified")
    if holding_id is None:
        raise click.UsageError("Error - holding id not specified")
    if location is None:
        raise click.UsageError("Error - location not specified")
    else:
        if location == "OBJECT_STORAGE":
            loc_type = Storage.OBJECT_STORAGE
        elif location == "TAPE":
            loc_type = Storage.TAPE
        else:
            raise click.UsageError(
                f"Error - unknown location type {location}.  Choices are OBJECT_STORAGE"
                " or TAPE"
            )

    # only need to contact S3 if deleting from object storage
    if delete and loc_type == Storage.OBJECT_STORAGE:
        s3_client = _connect_to_s3(access_key, secret_key)
        if access_key is None:
            raise click.UsageError("Error - access key not specified")
        if secret_key is None:
            raise click.UsageError("Error - secret key not specified")
    else:
        s3_client = None

    if settings != "":
        nlds_cat = _connect_to_catalog(settings)
    else:
        nlds_cat = _connect_to_catalog()

    nlds_cat.start_session()
    holding = nlds_cat.get_holding(user=user, group=group, holding_id=holding_id)[0]

    # get the locations
    for t in holding.transactions:
        for f in t.files:
            for l in f.locations:
                # first check whether a deletion will leave no locations left
                if len(f.locations) == 1 and loc_type == l.storage_type:
                    click.echo(
                        f"Deleting this location would leave no storage locations for "
                        f"the file {f.original_path}.  Skipping."
                    )
                else:
                    _remove_location_from_file(
                        f, l, loc_type, force, delete, s3_client, nlds_cat
                    )

    nlds_cat.save()
    nlds_cat.end_session()


if __name__ == "__main__":
    reset_storage_status()
