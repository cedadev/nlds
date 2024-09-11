from hashlib import shake_256

import minio
from minio.error import S3Error
from urllib3.exceptions import HTTPError


class S3StreamError(Exception):
    pass


class S3ToTarfileStream:
    """Class to stream files from an S3 resource (AWS, minio, DataCore Swarm etc.) to
    a tarfile that could reside on either tape (XRootD) or disk.

    The streams are defined in these directions:
    PUT : S3 -> Tarfile
    GET : Tarfile -> S3

    This class is abstract and should be overloaded.
    """

    def __init__(self, s3_tenancy: str, s3_access_key: str, s3_secret_key: str) -> None:
        """Initialise the Minio / S3 client"""
        require_secure_fl = False
        self.s3_client = minio.Minio(
            s3_tenancy,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
            secure=require_secure_fl,
        )
        self.filelist = []

    def _generate_filelist_hash(self):
        # Generate a name for the tarfile by hashing the combined filelist.
        # Length of the hash will be 16.
        # NOTE: this breaks if a problem file is removed from an aggregation
        assert(self.filelist != [])
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
                raise S3Error(
                    f"Could not verify that bucket {check_bucket} contained "
                    f"object {check_object} before writing to tape."
                )
