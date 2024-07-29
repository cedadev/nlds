# encoding: utf-8
"""
archive_put.py
NOTE: This module is imported into a revision, and so should be very defensive 
with how it imports external modules (like xrootd). 
"""
__author__ = "Jack Leland and Neil Massey"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import List, Dict, Any
import tarfile
from hashlib import shake_256

import minio
from minio.error import S3Error
from urllib3.exceptions import HTTPError

ignore_xrootd = False
try:
    from XRootD import client as XRDClient
    from XRootD.client.flags import OpenFlags, MkDirFlags, QueryCode
except ModuleNotFoundError:
    ignore_xrootd = True

from nlds_processors.archiver.archive_base import (
    BaseArchiveConsumer,
    ArchiveError,
    AdlerisingXRDFile,
)
from nlds.rabbit.consumer import State
from nlds.details import PathDetails
from nlds.errors import CallbackError
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


class TapeWriteError(CallbackError):
    pass


class PutArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_put_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.TRANSFER_PUT}." f"{RK.WILD}"
    DEFAULT_STATE = State.ARCHIVE_PUTTING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

    def transfer(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        tape_url: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, str],
    ):
        # Make the routing keys now
        rk_complete = ".".join([rk_origin, RK.ARCHIVE_PUT, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.ARCHIVE_PUT, RK.FAILED])

        # Can call this with impunity as the url has been verified previously
        tape_server, tape_base_dir = self.split_tape_url(tape_url)
        self.log(
            f"Tape url:{tape_url} split into tape server:{tape_server} "
            f"and tape base directory:{tape_base_dir}.",
            RK.LOG_INFO,
        )

        # NOTE: For the purposes of tape reading and writing, the holding slug
        # has 'nlds.' prepended
        holding_slug = self.get_holding_slug(body_json)

        # Create minio client
        s3_client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        # Create the FileSystem client at this point to verify the tape_base_dir
        fs_client = XRDClient.FileSystem(f"root://{tape_server}")
        self.verify_tape_server(fs_client, tape_server, tape_base_dir)

        # Generate a name for the tarfile by hashing the combined filelist.
        # Length of the hash will be 16.
        # NOTE: this breaks if a problem file is removed from an aggregation
        filenames = [f.original_path for f in filelist]
        filelist_hash = shake_256("".join(filenames).encode()).hexdigest(8)

        tar_filename = f"{filelist_hash}.tar"

        # All files are now supposed to be from a single aggregation according
        # to the current implementation of the PUT workflow. Here we do an
        # initial loop over all the files to verify contents before writing
        # anything to tape.
        for path_details in filelist:
            try:
                tape_path = path_details.tape_path
                # Split out the root and path, as they are in the Location
                check_root, _ = tape_path.split(":")
                # Split this further to get the holding_slug and the
                # original_path
                check_holding_slug, check_hash = check_root.split("_")
                # Split the object path to get bucket and object path
                check_bucket, check_object = path_details.object_name.split(":")
            except ValueError as e:
                raise CallbackError(
                    "Could not unpack mandatory info from filelist, cannot continue."
                )
            try:
                # Check that the calculated filelist_hash and the constructed
                # holding_slug match those stored for each of the files passed
                assert check_hash == filelist_hash
                assert check_holding_slug == holding_slug
            except AssertionError as e:
                raise CallbackError(
                    f"Could not verify calculated filehash ({filelist_hash}) "
                    f"and holding slug ({holding_slug}) against values passed "
                    f"from catalog ({check_holding_slug} and {check_hash})."
                )
            try:
                # Check the bucket and that the object is in the bucket and
                # matches the metadata stored.
                assert s3_client.bucket_exists(check_bucket)
                obj_stat_result = s3_client.stat_object(check_bucket, check_object)
                assert check_object == obj_stat_result.object_name
                assert path_details.size == obj_stat_result.size
            except (AssertionError, S3Error, HTTPError) as e:
                raise CallbackError(
                    f"Could not verify that bucket {check_bucket} contained "
                    f"object {check_object} before writing to tape."
                )

        # After verifying the filelist integrity we can actually write to tape.
        # The paths to the tar file and holding folder formatted for the xrd
        # FileSystem client
        fs_holding_tapepath = f"{tape_base_dir}/{holding_slug}"
        fs_full_tapepath = f"{fs_holding_tapepath}/{tar_filename}"
        # Path to the tar file formatted for the standard xrd client
        client_full_tapepath = (
            f"root://{tape_server}/{fs_holding_tapepath}/" f"{tar_filename}"
        )

        # Make holding folder and retry if it can't be created.
        status, _ = fs_client.mkdir(fs_holding_tapepath, MkDirFlags.MAKEPATH)
        if status.status != 0:
            # If bucket directory couldn't be created then fail for retrying
            raise CallbackError(
                f"Couldn't create or find holding directory "
                f"({fs_holding_tapepath})."
            )

        with XRDClient.File() as f:
            # Open the file as NEW, to avoid having to prepare it
            flags = OpenFlags.NEW | OpenFlags.MAKEPATH
            status, _ = f.open(client_full_tapepath, flags)
            if status.status != 0:
                raise CallbackError("Failed to open file for writing")

            # From this point onward we have a file on the disk cache. If anything
            # goes wrong we'll need to delete it and try the whole write again.
            try:
                file_wrapper = AdlerisingXRDFile(f, debug_fl=False)

                with tarfile.open(
                    mode="w", fileobj=file_wrapper, copybufsize=self.chunk_size
                ) as tar:
                    for path_details in filelist:
                        item_path = path_details.path

                        self.log(
                            f"Attempting to stream file {item_path} "
                            "directly to tape archive",
                            RK.LOG_DEBUG,
                        )

                        # Get the relevant variables from the path_details.
                        # NOTE: This won't fail as it's been verified above.
                        bucket_name, object_name = path_details.object_name.split(":")

                        tar_info = tarfile.TarInfo(name=object_name)
                        tar_info.size = int(path_details.size)
                        # TODO: add more file data into the tar_info?

                        # Attempt to stream the object directly into the File
                        # object
                        try:
                            stream = s3_client.get_object(
                                bucket_name,
                                object_name,
                            )
                            # Adds bytes to xrd.File from result, one chunk_size
                            # at a time
                            tar.addfile(tar_info, fileobj=stream)

                        except (HTTPError, S3Error, ArchiveError) as e:
                            # Catch error, increment retry info and then rethrow
                            # error to ensure file is deleted
                            reason = (
                                f"Stream-time exception occurred: "
                                f"{type(e).__name__}: {e}"
                            )
                            self.log(f"{reason}", RK.LOG_ERROR)
                            # Retries have gone, replaced by straight failure
                            self.failedlist.append(path_details)
                            raise e
                        else:
                            # Log successful
                            self.log(f"Successfully archived {item_path}", RK.LOG_DEBUG)
                            self.completelist.append(path_details)
                        finally:
                            # Terminate any hanging/unclosed connections
                            try:
                                stream.close()
                                stream.release_conn()
                            except AttributeError:
                                # If it can't be closed then dw
                                pass

            except Exception as e:
                self.log(
                    f"Exception occurred during write, need to delete file from "
                    f"disk cache before we send for retry. Original exception: "
                    f"{e}",
                    RK.LOG_ERROR,
                )
                self.remove_file(fs_full_tapepath, fs_client)
                raise TapeWriteError(f"Failure occurred during tape-write ({e})")

        # Write has now finished so, unless something is wrong with the
        # checksum, we no longer have to delete the file.

        # Finally get the checksum out of the file wrapper to pass back to the
        # catalog
        body_json[MSG.DATA][MSG.CHECKSUM] = file_wrapper.checksum

        if self.query_checksum_fl:
            status, result = fs_client.query(
                QueryCode.CHECKSUM,
                f"{fs_full_tapepath}?cks.type=adler32",  # Specify the type of checksum
            )
            if status.status != 0:
                self.log(
                    f"Could not query xrootd's checksum for tar file {tar_filename}.",
                    RK.LOG_WARNING,
                )
            else:
                try:
                    method, value = result.decode().split()
                    assert method == "adler32"
                    # Convert checksum from hex to int for comparison
                    checksum = int(value[:8], 16)
                    assert checksum == file_wrapper.checksum
                except ValueError as e:
                    self.log(
                        f"Exception {e} when attempting to parse tarfile "
                        f"checksum from xrootd",
                        RK.LOG_ERROR,
                    )
                except AssertionError as e:
                    # If it fails at this point then attempt to delete and start
                    # again.
                    reason = (
                        f"XRootD checksum {checksum} differs from that "
                        f"calculated block-wise {file_wrapper.checksum}."
                    )
                    self.log(
                        f"{reason}. Deleting file from disk-cache before it "
                        "gets written to tape.",
                        RK.LOG_ERROR,
                    )
                    self.remove_file(fs_full_tapepath, fs_client)
                    raise TapeWriteError(
                        f"Failure occurred during tape-write " f"({reason})."
                    )

        self.log(
            "Archive complete, passing lists back to worker for re-routing"
            " and cataloguing.",
            RK.LOG_INFO,
        )

        # Send whatever remains after all items have been put
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist, rk_complete, body_json, mode="archived"
            )

        if len(self.failedlist) > 0:
            # Send message back to worker so catalog can be scrubbed of failed puts
            self.send_pathlist(
                self.failedlist,
                rk_failed,
                body_json,
                state=State.CATALOG_ARCHIVE_ROLLBACK,
            )

    def remove_file(self, full_tape_path: str, fs_client: XRDClient.FileSystem):
        """Part of the error handling process, if any error occurs during write
        we'll have to be very defensive and start the whole process again. If
        doing so we'll need to remove the file from the disk cache before it
        gets written to tape, hence this function.
        """
        status, _ = fs_client.rm(
            full_tape_path,
        )
        if status.status != 0:
            reason = "Could not delete file from disk-cache"
            self.log(
                f"{reason}, will need to be marked as deleted for future tape "
                "repacking",
                RK.LOG_ERROR,
            )
            raise CallbackError(reason)
        else:
            self.log(
                "Deleted errored file from disk-cache to prevent tape write.",
                RK.LOG_INFO,
            )

    @classmethod
    def get_holding_slug(cls, body: Dict[str, Any]) -> str:
        """Get the uneditable holding information from the message body to
        reproduce the holding slug made in the catalog"""
        try:
            holding_id = body[MSG.META][MSG.HOLDING_ID]
            user = body[MSG.DETAILS][MSG.USER]
            group = body[MSG.DETAILS][MSG.GROUP]
        except KeyError as e:
            raise ArchiveError(f"Could not make holding slug, original error: " f"{e}")

        return f"nlds.{holding_id}.{user}.{group}"


def main():
    consumer = PutArchiveConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
