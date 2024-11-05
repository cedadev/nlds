"""
s3_to_tarfile_disk.py
"""

__author__ = "Neil Massey"
__date__ = "18 Sep 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import os
from typing import List
from zlib import adler32

from nlds.details import PathDetails
from nlds_processors.archiver.s3_to_tarfile_stream import (
    S3ToTarfileStream,
    S3StreamError,
)
from nlds_processors.archiver.adler32file import Adler32File
import nlds.rabbit.routing_keys as RK


class S3ToTarfileDisk(S3ToTarfileStream):
    """Class to stream files from / to an S3 resource (AWS, minio, DataCore Swarm etc.)
    to a tarfile that resides on disk.

    The streams are defined in these directions:
    PUT : S3 -> Tarfile
    GET : Tarfile -> S3"""

    def __init__(
        self,
        s3_tenancy: str,
        s3_access_key: str,
        s3_secret_key: str,
        disk_location: str,
        logger,
    ) -> None:
        # Initialise the S3 client first
        super().__init__(
            s3_tenancy=s3_tenancy,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            logger=logger,
        )
        # record and make the disk location directory if it doesn't exist
        try:
            self.disk_loc = os.path.expanduser(disk_location)
            os.mkdir(self.disk_loc)
        except FileExistsError:
            # it's okay if the path already exists
            pass
        except FileNotFoundError:
            raise S3StreamError(
                f"Couldn't create or find DISKTAPE directory ({self.disk_loc})."
            )

    def put(
        self, holding_prefix: str, filelist: List[PathDetails], chunk_size: int
    ) -> tuple[List[PathDetails], List[PathDetails], str, int]:
        """Stream from Object Store to a tarfile on disk"""
        if self.filelist != []:
            raise ValueError(f"self.filelist is not None: {self.filelist[0]}")
        self.filelist = filelist
        self.holding_prefix = holding_prefix
        # self._generate_filelist_hash and self._check_files_exist use the member
        # variables already set and the function definitions are in the parent class
        self.filelist_hash = self._generate_filelist_hash()
        completelist, failedlist = self._check_files_exist()
        if len(failedlist) > 0:
            return [], failedlist, None

        # make or find the holding folder on the disk
        try:
            os.mkdir(self.holding_diskpath)
        except FileExistsError:
            # it's okay if the path already exists
            pass
        except FileNotFoundError:
            raise S3StreamError(
                f"Couldn't create or find holding directory ({self.holding_diskpath})."
            )

        try:
            # open the tarfile to write to
            with open(self.tarfile_diskpath, "wb") as file:
                file_object = Adler32File(file, debug_fl=False)
                completelist, failedlist, checksum = self._stream_to_fileobject(
                    file_object, self.filelist, chunk_size
                )
        except FileExistsError:
            msg = (
                f"Couldn't create tarfile ({self.tarfile_diskpath}). File already "
                "exists."
            )
            self.log(msg, RK.LOG_ERROR)
            raise S3StreamError(msg)
        except FileNotFoundError:
            msg = (
                f"Couldn't create tarfile ({self.tarfile_diskpath}). Parent directory "
                "not found."
            )
            self.log(msg, RK.LOG_ERROR)
            raise S3StreamError(msg)
        except S3StreamError as e:
            msg = (
                f"Exception occurred during write of tarfile {self.tarfile_diskpath}. "
                f"This file will now be deleted from the DISKTAPE. "
                f"Original exception: {e}"
            )
            self.log(msg, RK.LOG_ERROR)
            try:
                self._remove_tarfile_from_disktape()
            except S3StreamError as e:
                msg += f" {e.message}"
            raise S3StreamError(msg)

        # now verify the checksum
        try:
            self._validate_tarfile_checksum(checksum)
        except S3StreamError as e:
            msg = (
                f"Exception occurred during validation of tarfile "
                f"{self.tarfile_diskpath}.  Original exception: {e}"
            )
            self.log(msg, RK.LOG_ERROR)
            try:
                self._remove_tarfile_from_disktape()
            except S3StreamError as e:
                msg += f" {e.message}"
            raise S3StreamError(msg)
        # add the location to the completelist
        for f in completelist:
            f.set_tape(
                server="",
                tapepath=self.holding_diskpath,
                tarfile=f"{self.filelist_hash}.tar",
            )
        return completelist, failedlist, self.tarfile_diskpath, checksum

    def get(
        self,
        holding_prefix: str,
        tarfile: str,
        filelist: List[PathDetails],
        chunk_size: int,
    ) -> tuple[List[PathDetails], List[PathDetails], str, int]:
        """Stream from a tarfile on disk to Object Store"""
        if self.filelist != []:
            raise ValueError(f"self.filelist is not Empty: {self.filelist[0]}")
        self.filelist = filelist
        self.holding_prefix = holding_prefix
        try:
            # open the tarfile to read from
            with open(tarfile, "rb") as file:
                file_object = Adler32File(file, debug_fl=False)
                completelist, failedlist = self._stream_to_s3object(
                    file_object, self.filelist, chunk_size
                )
        except FileNotFoundError:
            msg = f"Couldn't open tarfile ({self.tarfile_diskpath})."
            self.log(msg, RK.LOG_ERROR)
            raise S3StreamError(msg)
        except S3StreamError as e:
            msg = f"Exception occurred during read of tarfile {self.tarfile_diskpath}."
            self.log(msg, RK.LOG_ERROR)
            raise S3StreamError(msg)

        return completelist, failedlist

    @property
    def holding_diskpath(self):
        """Get the holding diskpath (i.e. the enclosing directory) on the DISKTAPE"""
        if not self.disk_loc:
            raise ValueError("self.disk_lock is None")
        if not self.holding_prefix:
            raise ValueError("self.holding_prefix is None")
        return f"{self.disk_loc}/{self.holding_prefix}"

    @property
    def tarfile_diskpath(self):
        """Get the holding diskpath (i.e. the enclosing directory) on the DISKTAPE"""
        if not self.disk_loc:
            raise ValueError("self.disk_lock is None")
        if not self.holding_prefix:
            raise ValueError("self.holding_prefix is None")
        if not self.filelist_hash:
            raise ValueError("self.filelist_hash is None")
        return f"{self.disk_loc}/{self.holding_prefix}/{self.filelist_hash}.tar"

    def _validate_tarfile_checksum(self, tarfile_checksum: str):
        """Calculate the Adler32 checksum of the tarfile and compare it to the checksum
        calculated when streaming from the S3 server to the DISKTAPE"""
        asum = 1
        with open(self.tarfile_diskpath, "rb") as fh:
            while data := fh.read():
                asum = adler32(data, asum)
        if asum != tarfile_checksum:
            reason = (
                f"Checksum {asum} differs from that calculated during streaming "
                f"upload {tarfile_checksum}."
            )
            self.log(reason, RK.LOG_ERROR)
            raise S3StreamError(
                f"Failure occurred during DISKTAPE-write " f"({reason})."
            )

    def _remove_tarfile_from_disktape(self):
        """On failure, remove tarfile from disk"""
        try:
            os.remove(self.tarfile_diskpath)
        except FileNotFoundError:
            reason = "Could not delete file from DISKTAPE: not found"
            self.log(f"{reason}, will need to be manually deleted", RK.LOG_ERROR)
            raise S3StreamError(reason)
        else:
            self.log(
                "Deleted errored file from DISKTAPE to prevent tape write.",
                RK.LOG_INFO,
            )
