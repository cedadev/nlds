"""
s3_to_tarfile_stream.py
"""
__author__ = "Neil Massey"
__date__ = "18 Sep 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from hashlib import shake_256
from typing import List
from urllib3.exceptions import HTTPError
import tarfile

import minio
from minio.error import S3Error

from nlds.details import PathDetails
import nlds.rabbit.routing_keys as RK
from nlds.errors import MessageError


class S3StreamError(MessageError):
    pass

class S3ToTarfileStream:
    """Class to stream files from an S3 resource (AWS, minio, DataCore Swarm etc.) to
    a tarfile that could reside on either tape (XRootD) or disk.

    The streams are defined in these directions:
    PUT : S3 -> Tarfile
    GET : Tarfile -> S3

    This class is abstract and should be overloaded.
    """

    def __init__(
        self, s3_tenancy: str, s3_access_key: str, s3_secret_key: str, logger
    ) -> None:
        """Initialise the Minio / S3 client"""
        require_secure_fl = False
        self.s3_client = minio.Minio(
            s3_tenancy,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
            secure=require_secure_fl,
        )
        self.filelist = []
        self.log = logger

    def _generate_filelist_hash(self):
        # Generate a name for the tarfile by hashing the combined filelist.
        # Length of the hash will be 16.
        # NOTE: this breaks if a problem file is removed from an aggregation
        assert self.filelist != []
        filenames = [f.original_path for f in self.filelist]
        filelist_hash = shake_256("".join(filenames).encode()).hexdigest(8)
        return filelist_hash

    def _check_files_exist(self):
        # All files are now supposed to be from a single aggregation according
        # to the current implementation of the PUT workflow. Here we do an
        # initial loop over all the files to verify contents before writing
        # anything to tape.
        for path_details in self.filelist:
            try:
                # Split the object path to get bucket and object path
                check_bucket, check_object = path_details.object_name.split(":")
            except ValueError as e:
                raise S3StreamError(
                    "Could not unpack bucket and object info from path_details"
                )
            try:
                # Check the bucket and that the object is in the bucket and
                # matches the metadata stored.
                assert self.s3_client.bucket_exists(check_bucket)
                obj_stat_result = self.s3_client.stat_object(check_bucket, check_object)
                assert check_object == obj_stat_result.object_name
                assert path_details.size == obj_stat_result.size
            except (AssertionError, S3Error, HTTPError) as e:
                raise S3StreamError(
                    f"Could not verify that bucket {check_bucket} contained object "
                    f"{check_object} before writing to tape. Original exception: {e}"
                )

    def _stream_to_fileobject(
        self, file_object, filelist: List[PathDetails], chunk_size: int
    ):
        # Stream from the S3 Object Store to a tar file that is created using the
        # file_object - this is usually an Adler32File
        with tarfile.open(mode="w", fileobj=file_object, copybufsize=chunk_size) as tar:
            # local versions of the completelist and failedlist
            completelist = []
            failedlist = []

            for path_details in filelist:
                self.log(
                    f"Streaming file {path_details.path} from object store to tape "
                    f"archive",
                    RK.LOG_DEBUG,
                )

                # Add file info to the tarfile
                bucket_name, object_name = path_details.object_name.split(":")
                tar_info = tarfile.TarInfo(name=object_name)
                tar_info.size = int(path_details.size)

                # Attempt to stream the object directly into the tarfile object
                try:
                    stream = self.s3_client.get_object(bucket_name, object_name)
                    # Adds bytes to xrd.File from result, one chunk_size at a time
                    tar.addfile(tar_info, fileobj=stream)

                except (HTTPError, S3Error) as e:
                    # Catch error, log and then rethrow error to ensure file is deleted
                    reason = (
                        f"Stream-time exception occurred: " f"{type(e).__name__}: {e}"
                    )
                    self.log(f"{reason}", RK.LOG_ERROR)
                    # Retries have gone, replaced by straight failure
                    path_details.failure_reason = reason
                    failedlist.append(path_details)
                else:
                    # Log successful
                    self.log(f"Successfully archived {path_details.path}", RK.LOG_DEBUG)
                    completelist.append(path_details)
                finally:
                    # Terminate any hanging/unclosed connections
                    try:
                        stream.close()
                        stream.release_conn()
                    except AttributeError:
                        # If it can't be closed then dw
                        pass
        return completelist, failedlist, file_object.checksum
