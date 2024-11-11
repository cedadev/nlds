"""
s3_to_tarfile_tape.py
"""

__author__ = "Neil Massey"
__date__ = "18 Sep 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Tuple, List
import os

from XRootD import client as XRDClient
from XRootD.client.flags import StatInfoFlags, MkDirFlags, OpenFlags, QueryCode

from nlds.details import PathDetails
from nlds_processors.archiver.s3_to_tarfile_stream import (
    S3ToTarfileStream,
    S3StreamError,
)
from nlds_processors.archiver.adler32file import Adler32XRDFile
import nlds.rabbit.routing_keys as RK


class S3ToTarfileTape(S3ToTarfileStream):
    """Class to stream files from / to an S3 resource (AWS, minio, DataCore Swarm etc.)
    to a tarfile that resides on tape.

    The streams are defined in these directions:
    PUT : S3 -> Tarfile
    GET : Tarfile -> S3"""

    # constants for environment variables
    XRD_SECRET_PROTOCOL = "XrdSecPROTOCOL"
    XRD_SECRET_KEYTAB = "XrdSecSSSKT"

    def __init__(
        self,
        s3_tenancy: str,
        s3_access_key: str,
        s3_secret_key: str,
        tape_url: str,
        logger,
    ) -> None:
        # Initialise the S3 client first
        super().__init__(
            s3_tenancy=s3_tenancy,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            logger=logger,
        )
        # get the location of the tape server and the base directory from the tape_url
        self.tape_server_url, self.tape_base_dir = self._split_tape_url(tape_url)
        self.log(
            f"Tape url:{tape_url} split into tape server:{self.tape_server_url} "
            f"and tape base directory:{self.tape_base_dir}.",
            RK.LOG_INFO,
        )

        # check that the environment variables are set:
        # XrdSecPROTOCOL & XrdSecSSSKT are needed for the XrootD authentication
        if S3ToTarfileTape.XRD_SECRET_KEYTAB not in os.environ:
            raise RuntimeError(
                f"{S3ToTarfileTape.XRD_SECRET_KEYTAB} environment variable not set."
            )

        if S3ToTarfileTape.XRD_SECRET_PROTOCOL not in os.environ:
            raise RuntimeError(
                f"{S3ToTarfileTape.XRD_SECRET_PROTOCOL} environment variable not set."
            )

        # create the connection to the tape server
        self.tape_client = XRDClient.FileSystem(f"root://{self.tape_server_url}")
        self._verify_tape_server()
        self.log(f"Connected to tape server: {self.tape_server_url}", RK.LOG_INFO)

    def put(
        self, holding_prefix: str, filelist: List[PathDetails], chunk_size: int
    ) -> tuple[List[PathDetails], List[PathDetails], str, int]:
        """
        Put the filelist to the tape server using the already created S3 client and
         tape client.
        The holding_prefix is calculated from the message body in the archive_put or
        archive_get worker process.
        """
        if self.filelist != []:
            raise ValueError("self.filelist is not empty")
        self.filelist = filelist
        self.holding_prefix = holding_prefix

        # self._generate_filelist_hash and self._check_files_exist use the member
        # variables already set and the function definitions are in the parent class
        self.filelist_hash = self._generate_filelist_hash()
        completelist, failedlist = self._check_files_exist()
        if len(failedlist) > 0:
            return [], failedlist, None

        # Make or find holding folder on the tape server
        status, _ = self.tape_client.mkdir(self.holding_tapepath, MkDirFlags.MAKEPATH)
        if status.status != 0:
            # If bucket directory couldn't be created then fail for retrying
            raise S3StreamError(
                f"Couldn't create or find holding directory ({self.holding_tapepath})."
            )

        try:
            with XRDClient.File() as XRD_file:
                # Open the file as NEW, to avoid having to prepare it
                flags = OpenFlags.NEW | OpenFlags.MAKEPATH
                status, _ = XRD_file.open(self.tarfile_absolute_tapepath, flags)
                if status.status != 0:
                    raise S3StreamError(
                        f"Failed to open file {self.tarfile_absolute_tapepath} for "
                        "writing."
                    )
                file_object = Adler32XRDFile(XRD_file, debug_fl=False)
                completelist, failedlist, checksum = self._stream_to_fileobject(
                    file_object, filelist, chunk_size
                )
        except S3StreamError as e:
            msg = (
                f"Exception occurred during write of tarfile "
                f"{self.tarfile_absolute_tapepath}.  This file will now be deleted "
                f"from the tape system disk cache. Original exception: {e}"
            )
            self.log(msg, RK.LOG_ERROR)
            try:
                self._remove_tarfile_from_tape()
            except S3StreamError as e:
                msg += f" {e.message}"
            raise S3StreamError(msg)
        # now verify the checksum
        try:
            self._validate_tarfile_checksum(checksum)
        except S3StreamError as e:
            msg = (
                f"Exception occurred during validation of tarfile "
                f"{self.tarfile_tapepath}.  Original exception: {e}"
            )
            self.log(msg, RK.LOG_ERROR)
            try:
                self._remove_tarfile_from_tape()
            except S3StreamError as e:
                msg += f" {e.message}"
            raise S3StreamError(msg)
        # add the location to the completelist
        for f in completelist:
            f.set_tape(
                server="",
                tapepath=self.holding_tapepath,
                tarfile=f"{self.filelist_hash}.tar",
            )
        return completelist, failedlist, self.tarfile_tapepath, checksum

    def get(
        self,
        holding_prefix: str,
        tarfile: str,
        filelist: List[PathDetails],
        chunk_size: int,
    ) -> tuple[List[PathDetails], List[PathDetails], str, int]:
        raise NotImplementedError

    def prepare_required(self) -> bool:
        """Query the storage system as to whether a file needs to be prepared (staged)."""
        raise NotImplementedError

    def prepare_request(self) -> int:
        """Request the storage system for a file to be prepared (staged)."""
        raise NotImplementedError

    def prepare_complete(self, prepare_id: int) -> bool:
        """Query the storage system whether the prepare (staging) for a file has been
        completed."""
        raise NotImplementedError

    """Note that there are a number of different methods below to get the tapepaths"""

    @property
    def holding_tapepath(self):
        """Get the holding tapepath (i.e. the enclosing directory), to be used with
        XRDClient functions on that directory."""
        if not self.tape_base_dir:
            raise ValueError("self.tape_base_dir is None")
        if not self.holding_prefix:
            raise ValueError("self.holding_prefix is None")
        return f"{self.tape_base_dir}/{self.holding_prefix}"

    @property
    def tarfile_tapepath(self):
        """Get the tapepath of the tar file, to be used with the XRDClient functions."""
        if not self.tape_base_dir:
            raise ValueError("self.tape_base_dir is None")
        if not self.holding_prefix:
            raise ValueError("self.holding_prefix is None")
        if not self.filelist_hash:
            raise ValueError("self.filelist_hash is None")
        return f"{self.tape_base_dir}/{self.holding_prefix}/{self.filelist_hash}.tar"

    @property
    def tarfile_absolute_tapepath(self):
        """Get the absolute tapepath of the tar file, to be used with the XRDClient.
        File functions / constructor, i.e. for the object that is to be streamed to."""
        if not self.tape_base_dir:
            raise ValueError("self.tape_base_dir is None")
        if not self.holding_prefix:
            raise ValueError("self.holding_prefix is None")
        if not self.filelist_hash:
            raise ValueError("self.filelist_hash is None")
        if not self.tape_server_url:
            raise ValueError("self.tape_server_url is None")
        return (
            f"root://{self.tape_server_url}/{self.tape_base_dir}/"
            f"{self.holding_prefix}/{self.filelist_hash}.tar"
        )

    @staticmethod
    def _split_tape_url(tape_url: str) -> Tuple[str]:
        """Split the tape URL into the server and base directory"""
        # Verify tape url is valid
        tape_url_parts = tape_url.split("//")
        if not (len(tape_url_parts) == 3 and tape_url_parts[0] == "root:"):
            raise S3StreamError(
                "Tape URL given was invalid. Must be of the "
                "form: root://{server}//{archive/path}, was "
                f"given as {tape_url}."
            )
        _, server, base_dir = tape_url_parts
        # prepend a slash onto the base_dir so it can directly be used to make
        # directories with the pyxrootd client

        return server, f"/{base_dir}"

    def _verify_tape_server(self):
        """Make several simple checks with xrootd to ensure the tape server and
        tape base path, derived form a given tape url, are valid and the xrootd
        endpoint they describe is accessible on the current system.
        """
        # Attempt to ping the tape server to check connection is workable.
        status, _ = self.tape_client.ping()
        if status.status != 0:
            msg = (
                f"Failed status message: {status.message}. ",
                f"Could not ping cta server at {self.tape_server_url}.",
            )
            raise S3StreamError(msg)

        # Stat the base directory and check it's a directory.
        status, resp = self.tape_client.stat(self.tape_base_dir)
        if status.status != 0:
            msg = (
                f"Failed status message: {status.message}. ",
                f"Base dir {self.tape_base_dir} could not be statted",
            )
            raise S3StreamError(msg)
        # Check whether the flag indicates it's a directory
        elif not bool(resp.flags & StatInfoFlags.IS_DIR):
            msg = (
                f"Failed status message: {status.message}. ",
                f"Full status object: {status}. ",
                f"Stat result for base dir {self.tape_base_dir} ",
                f"indicates it is not a directory.",
            )
            raise S3StreamError(msg)

    def _remove_tarfile_from_tape(self):
        """Part of the error handling process, if any error occurs during write
        we'll have to be very defensive and start the whole process again. If
        doing so we'll need to remove the tarfile from the disk cache on the tape
        system before it gets written to tape, hence this function.
        """

        status, _ = self.tape_client.rm(self.tarfile_tapepath)
        if status.status != 0:
            reason = "Could not delete file from disk-cache"
            self.log(
                f"{reason}, will need to be marked as deleted for future tape "
                "repacking",
                RK.LOG_ERROR,
            )
            raise S3StreamError(reason)
        else:
            self.log(
                "Deleted errored file from disk-cache to prevent tape write.",
                RK.LOG_INFO,
            )

    def _validate_tarfile_checksum(self, tarfile_checksum: str):
        """Validate the checksum of the tarfile by querying what the tape server
        calculated"""
        # Need to specify the type of checksum
        status, result = self.tape_client.query(
            QueryCode.CHECKSUM,
            f"{self.tarfile_tapepath}?cks.type=adler32",
        )
        if status.status != 0:
            self.log(
                f"Could not query xrootd's checksum for tar file "
                f"{self.tarfile_tapepath}.",
                RK.LOG_WARNING,
            )
        else:
            try:
                method, value = result.decode().split()
                if method != "adler32":
                    raise ValueError("method is not adler32")
                # Convert checksum from hex to int for comparison
                checksum = int(value[:8], 16)
                if checksum != tarfile_checksum:
                    # If it fails at this point then attempt to delete.  It will be
                    # scheduled to happend again, so long as the files are added to
                    # failed_list
                    reason = (
                        f"XRootD checksum {checksum} differs from that calculated "
                        f"during streaming upload {tarfile_checksum}."
                    )
                    self.log(reason, RK.LOG_ERROR)
                    raise S3StreamError(
                        f"Failure occurred during tape-write " f"({reason})."
                    )
            except ValueError as e:
                self.log(
                    f"Exception {e} when attempting to parse tarfile checksum from "
                    f"xrootd",
                    RK.LOG_ERROR,
                )
                raise e
