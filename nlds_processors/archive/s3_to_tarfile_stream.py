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
from urllib3.exceptions import HTTPError, MaxRetryError
import tarfile
from datetime import datetime
from abc import abstractmethod

import minio
import urllib3
import certifi
from minio.error import S3Error
from urllib3.util import Timeout, Retry

from nlds.details import PathDetails
import nlds.rabbit.routing_keys as RK
from nlds.errors import MessageError
from nlds_processors.bucket_mixin import BucketMixin, BucketError
import nlds.server_config as CFG


class S3StreamError(MessageError):
    pass


class S3ToTarfileStream(BucketMixin):
    """Class to stream files from an S3 resource (AWS, minio, DataCore Swarm etc.) to
    a tarfile that could reside on either tape (XRootD) or disk.

    The streams are defined in these directions:
    PUT : S3 -> Tarfile
    GET : Tarfile -> S3

    This class is abstract and should be overloaded.
    """

    def __init__(
        self,
        s3_tenancy: str,
        s3_access_key: str,
        s3_secret_key: str,
        require_secure_fl: bool,
        http_timeout: int,
        logger,
    ) -> None:
        """Initialise the Minio / S3 client"""
        self.s3_client = self._create_s3_client(
            s3_tenancy,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
            secure=require_secure_fl,
            http_timeout=http_timeout,
        )
        self.filelist = []
        self.log = logger
        # load the whole config as it is needed for the object store access policies
        self.whole_config = CFG.load_config()

    def _create_s3_client(
        self,
        tenancy: str,
        access_key: str,
        secret_key: str,
        secure: bool,
        http_timeout: int,
    ):
        # Create a minio S3 client with a custom http client with a timeout of 24
        # hours
        _http = urllib3.PoolManager(
            timeout=Timeout(connect=http_timeout, read=http_timeout),
            maxsize=10,
            cert_reqs="CERT_NONE",
            ca_certs=certifi.where(),
            retries=Retry(
                total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]
            ),
        )
        # create Minio client with supplied info and custom http client created above
        _client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            http_client=_http,
        )
        return _client

    def _generate_filelist_hash(self):
        # Generate a name for the tarfile by hashing the combined filelist.
        # Length of the hash will be 16.
        # NOTE: this breaks if a problem file is removed from an aggregation
        if self.filelist == []:
            raise S3StreamError("self.filelist is empty")
        filenames = [f.original_path for f in self.filelist]
        filelist_hash = shake_256("".join(filenames).encode()).hexdigest(8)
        return filelist_hash

    def _check_files_exist(self):
        # All files are now supposed to be from a single aggregation according
        # to the current implementation of the PUT workflow. Here we do an
        # initial loop over all the files to verify contents before writing
        # anything to tape.
        failed_list = []
        for path_details in self.filelist:
            try:
                check_bucket, check_object = self._get_bucket_name_object_name(
                    path_details
                )
            except BucketError as e:
                path_details.failure_reason = (
                    "Could not unpack bucket and object info from path_details"
                )
                failed_list.append(path_details)
                continue

            try:
                # Check the bucket exists
                if not self._bucket_exists(check_bucket):
                    path_details.failure_reason = (
                        f"Bucket {check_bucket} does not exist when attempting to "
                        f"write to tape."
                    )
                    failed_list.append(path_details)
                    continue
            except (BucketError, MaxRetryError) as e:
                path_details.failure_reason = (
                    f"Could not verify that bucket {check_bucket} exists before "
                    f"writing to tape. Original exception: {e}"
                )
                failed_list.append(path_details)
                continue

            try:
                # Check that the object is in the bucket and the names match
                obj_stat_result = self.s3_client.stat_object(check_bucket, check_object)
                if check_object != obj_stat_result.object_name:
                    path_details.failure_reason = (
                        f"Could not verify file {check_bucket}:{check_object} before "
                        f"writing to tape. File name differs between original name and "
                        f"object name."
                    )
                    failed_list.append(path_details)
                    continue
            except (S3Error, HTTPError) as e:
                path_details.failure_reason = (
                    f"Could not verify file {check_bucket}:{check_object} exists "
                    f"before writing to tape. Original exception {e}."
                )
                failed_list.append(path_details)
                continue

            try:
                # Check that the object is in the bucket and the names match
                obj_stat_result = self.s3_client.stat_object(check_bucket, check_object)
                if path_details.size != obj_stat_result.size:
                    path_details.failure_reason = (
                        f"Could not verify file {check_bucket}:{check_object} before "
                        f"writing to tape. File size differs between original size and "
                        f"object size."
                    )
                    failed_list.append(path_details)
                    continue
            except (S3Error, HTTPError) as e:
                path_details.failure_reason = (
                    f"Could not verify that file {check_bucket}:{check_object} exists "
                    f"before writing to tape. Original exception {e}."
                )
                failed_list.append(path_details)
                continue
        return [], failed_list

    def _write_tar_header(self, tar: tarfile):
        tar_info = tarfile.TarInfo()
        # write the header for the tarfile, using the format, encoding, etc

    def _stream_write_chunk_by_chunk(
        self, tar: tarfile, path_details: PathDetails, chunksize: int
    ):
        """Stream the file from the S3 to a tarfile chunk by chunk, using HTTP range get."""
        # get the bucket and object name
        bucket_name, object_name = self._get_bucket_name_object_name(path_details)
        # We need the size details from the object first
        obj_details = self.s3_client.stat_object(bucket_name, object_name)

        # here is some hacking to allow an empty file to be opened in python3.14
        # we are essentially using undocumented members of the class to hand write
        # the tar_info into the file
        tar_info = tarfile.TarInfo(name=object_name)
        tar_info.size = obj_details.size
        tar_info.type = tarfile.REGTYPE
        # we can assign the user details to the items in the tarfile
        tar_info.uid = path_details.user
        tar_info.gid = path_details.group
        if path_details.permissions:
            tar_info.mode = path_details.permissions
        if path_details.access_time:
            tar_info.mtime = path_details.access_time

        # write the information buffer header
        buf = tar_info.tobuf(tar.format, tar.encoding, tar.errors)
        tar.fileobj.write(buf)
        tar.offset += len(buf)

        # stream the file chunk by chunk and add it to the tarfile
        for chunk in range(0, obj_details.size, chunksize):
            # get the object using HTTP range get
            s3_stream = self.s3_client.get_object(
                bucket_name=bucket_name,
                object_name=object_name,
                offset=chunk,
                length=chunksize,
            )
            # read part of the object and write to the tarfile
            d = s3_stream.read()
            tar.fileobj.write(d)
            # release the connection so it can be reused
            s3_stream.release_conn()

        # null pad to end of BLOCKSIZE and locate offset for next file in the tar
        blocks, remainder = divmod(tar_info.size, tarfile.BLOCKSIZE)
        if remainder > 0:
            tar.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
            blocks += 1
        tar.offset += blocks * tarfile.BLOCKSIZE

        tar.members.append(tar_info)

    def _stream_to_fileobject(
        self,
        file_object,
        filelist: List[PathDetails],
        chunk_size: int,
        num_parallel_uploads: int = 1,
    ):
        if self.s3_client is None:
            raise S3StreamError("self.s3_client is None")

        # Stream from the S3 Object Store to a tar file that is created using the
        # file_object - this is usually an Adler32File
        with tarfile.open(mode="w|", fileobj=file_object, bufsize=chunk_size) as tar:
            # local versions of the completelist and failedlist
            completelist = []
            failedlist = []

            # write the tar header first
            self._write_tar_header(tar=tar)
            for path_details in filelist:
                self.log(
                    f"Streaming file {path_details.path} from object store to tape "
                    f"archive",
                    RK.LOG_DEBUG,
                )
                # Attempt to stream the object directly into the tarfile object
                # NRM - chunk by chunk now
                try:
                    self._stream_write_chunk_by_chunk(
                        tar=tar, path_details=path_details, chunksize=chunk_size
                    )
                except BucketError as e:
                    reason = str(e)
                    self.log(f"{reason}", RK.LOG_ERROR)
                    path_details.failure_reason = reason
                    failedlist.append(path_details)
                    continue

                except (HTTPError, S3Error) as e:
                    # Catch error, add to failed list
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
        return completelist, failedlist, file_object.checksum

    def _stream_to_s3object(
        self,
        file_object,
        filelist: List[PathDetails],
        chunk_size: int,
        num_parallel_uploads: int = 1,
    ):
        if self.s3_client is None:
            raise S3StreamError("self.s3_client is None")

        # Ensure minimum part_size is met for put_object to function
        chunk_size = max(5 * 1024 * 1024, chunk_size)

        # Stream from the a tar file to the S3 Object Store that is created via the
        # file_object - this is usually an Adler32File
        with tarfile.open(mode="r", fileobj=file_object, copybufsize=chunk_size) as tar:
            # local versions of the completelist and failedlist
            completelist = []
            failedlist = []

            for path_details in filelist:
                self.log(
                    f"Streaming file {path_details.path} from tape archive to object store",
                    RK.LOG_DEBUG,
                )
                try:
                    bucket_name, object_name = self._get_bucket_name_object_name(
                        path_details
                    )
                except BucketError as e:
                    reason = f"Cannot get bucket_name, object_name from PathDetails"
                    self.log(f"{reason}", RK.LOG_ERROR)
                    path_details.failure_reason = reason
                    failedlist.append(path_details)
                    continue
                # Stream the object directly from the tarfile object to s3
                # create bucket first if it doesn't exist
                try:
                    tarinfo = tar.getmember(path_details.original_path)
                except KeyError:
                    # not found in tar so add to failed list
                    reason = (
                        f"Could not find tar member for path details object "
                        f"{path_details}"
                    )
                    path_details.failure_reason = reason
                    failedlist.append(path_details)
                    continue

                try:
                    # get or create the bucket
                    try:
                        # set access policies only if bucket is created
                        if self._make_bucket(bucket_name=bucket_name):
                            self._set_access_policies(
                                bucket_name=bucket_name, group=path_details.group
                            )
                    except BucketError as e:
                        raise S3StreamError(
                            f"Cannot make bucket {bucket_name}, reason: str{e}"
                        )
                    self.log(
                        f"Starting stream of {tarinfo.name} to object store bucket "
                        f"{bucket_name}.",
                        RK.LOG_INFO,
                    )
                    # Extract the file as a file object
                    f = tar.extractfile(tarinfo)
                    write_result = self.s3_client.put_object(
                        bucket_name,
                        object_name,
                        f,
                        -1,
                        part_size=chunk_size,
                        num_parallel_uploads=num_parallel_uploads,
                    )
                    self.log(
                        f"Finished stream of {tarinfo.name} to object store",
                        RK.LOG_INFO,
                    )
                except (HTTPError, S3Error) as e:
                    reason = (
                        f"Stream-time exception occurred: ({type(e).__name__}: {e})"
                    )
                    path_details.failure_reason = reason
                    self.log(reason, RK.LOG_DEBUG)
                    failedlist.append(path_details)
                except S3StreamError as e:
                    path_details.failure_reason = e.message
                    failedlist.append(path_details)
                except Exception as e:
                    reason = (
                        f"Unexpected exception occurred during stream {e}",
                        RK.LOG_ERROR,
                    )
                    self.log(reason, RK.LOG_DEBUG)
                    failedlist.append(path_details)
                else:
                    # success
                    self.log(
                        f"Successfully retrieved {path_details.path} from the archive "
                        "and streamed to object store",
                        RK.LOG_INFO,
                    )
                    # set access time as now
                    path_details.get_object_store().access_time = (
                        datetime.now().timestamp()
                    )
                    completelist.append(path_details)
        return completelist, failedlist

    @abstractmethod
    def put(
        self,
        holding_prefix: str,
        filelist: List[PathDetails],
        chunk_size: int,
        num_parallel_uploads: int = 1,
    ) -> tuple[List[PathDetails], List[PathDetails], str, int]:
        raise NotImplementedError

    @abstractmethod
    def get(
        self,
        holding_prefix: str,
        tarfile: str,
        filelist: List[PathDetails],
        chunk_size: int,
        num_parallel_uploads: int = 1,
    ) -> tuple[List[PathDetails], List[PathDetails]]:
        raise NotImplementedError

    @abstractmethod
    def prepare_required(self, tarfile: str) -> bool:
        """Query the storage system as to whether a file needs to be prepared."""
        raise NotImplementedError

    @abstractmethod
    def prepare_request(self, tarfilelist: List[str]) -> int:
        """Request the storage system for a file to be prepared"""
        raise NotImplementedError

    @abstractmethod
    def prepare_complete(self, prepare_id: str, tarfilelist: List[str]) -> bool:
        """Query the storage system whether the prepare for a file has been completed."""
        raise NotImplementedError

    @abstractmethod
    def evict(self, tarfilelist: List[str]):
        """Evict any files from the temporary storage cache on the storage system."""
        raise NotImplementedError
