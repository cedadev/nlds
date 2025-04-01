# encoding: utf-8
"""
nlds_chown.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "07 Dec 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import pathlib as pth
import os

import click

NLDS_UID = 7054096


@click.command()
@click.argument("new_uid", type=int)
@click.argument("filepath", type=str)
def chown_nlds(new_uid, filepath):
    # Parse cli arguments
    new_uid = int(new_uid)
    filepath = pth.Path(filepath).resolve()
    # Check path exists and is a file
    if (
        filepath.exists()
        and not filepath.is_symlink()
        and (filepath.is_dir() or filepath.is_file())
    ):
        stat_result = filepath.stat()
        if stat_result.st_uid != NLDS_UID:
            raise PermissionError(
                "Cannot change ownership of a file not owned by the NLDS "
                "(uid=7054096)"
            )
        else:
            # Change the owner but leave the gid
            os.chown(filepath, new_uid, stat_result.st_gid)
    else:
        raise FileNotFoundError(f"Couldn't find file or directory at {filepath}")


if __name__ == "__main__":
    chown_nlds()
