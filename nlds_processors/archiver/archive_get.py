from typing import List, Dict
from pathlib import Path
import os
import tarfile

import minio
from minio.error import S3Error
from retry import retry
from urllib3.exceptions import HTTPError
from XRootD import client
from XRootD.client.flags import (DirListFlags, PrepareFlags, DirListFlags, 
                                 OpenFlags, MkDirFlags, QueryCode)

from nlds_processors.archiver.archive_base import (BaseArchiveConsumer, 
                                                   ArchiveError,
                                                   AdlerisingXRDFile)
from nlds.rabbit.consumer import FilelistType, State
from nlds.details import PathDetails
from nlds.errors import CallbackError

    
class GetArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_get_q"
    DEFAULT_ROUTING_KEY = (f"{BaseArchiveConsumer.RK_ROOT}."
                           f"{BaseArchiveConsumer.RK_TRANSFER_PUT}."
                           f"{BaseArchiveConsumer.RK_WILD}")
    DEFAULT_STATE = State.ARCHIVE_GETTING


    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)


    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, tape_url: str, filelist: List[PathDetails], 
                 rk_origin: str, body_json: Dict[str, str]):
        
        # Can call this with impunity as the url has been verified previously
        tape_server, tape_base_dir = self.split_tape_url(tape_url)

        # Create minio client
        s3_client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        # Create the FileSystem client at this point to verify the tape_base_dir 
        fs_client = client.FileSystem(f"root://{tape_server}")
        # Attempt to list the base-directory to ensure it exists, should succeed 
        # if everything is working properly but otherwise will be passed for 
        # message-level retry. 
        status, _ = fs_client.dirlist(tape_base_dir, DirListFlags.STAT)
        if status.status != 0:
            self.log(f"Full status object: {status}", self.RK_LOG_DEBUG)
            raise CallbackError(f"Base dir derived from tape_url "
                                f"({tape_base_dir}) does not seem to exist. "
                                f"Status message: {status.message}")
        
        # Declare useful variables
        bucket_name = None
        rk_complete = ".".join([rk_origin, self.RK_ARCHIVE_GET, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_ARCHIVE_GET, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_ARCHIVE_GET, self.RK_FAILED])

        # Ensure minimum part_size is met for put_object to function
        chunk_size = max(5*1024*1024, self.chunk_size)

        # Main loop. 
        # Each file_details will have an associated tar file which will need to 
        # be prepared/staged and scanned to make sure the file in question is in 
        # the tar file. Check for bucket existence too though as the file could 
        # have been retrieved as part of an prior retrieval.
        for path_details in filelist:
            item_path = path_details.path

            # First check whether index item has failed too many times
            if path_details.retries.count > self.max_retries:
                self.failedlist.append(path_details)
                continue
            
            # Get the bucket_name and path from the object_name 
            if len(path_details.object_name.split(':')) == 2:
                bucket_name, object_name = path_details.object_name.split(':')
            # Otherwise, log error and queue for retry
            else:
                reason = "Unable to get bucket_name from message info"
                self.log(f"{reason}, adding "
                         f"{path_details.object_name} to retry list.", 
                         self.RK_LOG_INFO)
                path_details.retries.increment(reason=reason)
                self.retrylist.append(path_details)
                continue
            
            try:
                # Check that bucket exists, and create if not
                if not s3_client.bucket_exists(bucket_name):
                    s3_client.make_bucket(bucket_name)
                    self.log(f"Creating bucket ({bucket_name}) for this"
                             " transaction", self.RK_LOG_INFO)
                else:
                    self.log(f"Bucket for this transaction ({bucket_name}) "
                             f"already exists", self.RK_LOG_INFO)
                    objects = list([obj.object_name for obj 
                                    in s3_client.list_objects(bucket_name)])
                    # Look for object in bucket, continue if not present
                    assert object_name not in objects
            except HTTPError as e:   
                # If bucket can't be created then pass for retry and continue
                reason = (f"Bucket {bucket_name} could not be created due to "
                          f"error connecting with tenancy {tenancy}")
                self.log(f"{reason}. Adding {object_name} to retry list.", 
                         self.RK_LOG_ERROR)
                path_details.retries.increment(reason=reason)
                self.retrylist.append(path_details)
                continue
            except AssertionError as e:
                # If it exists in the bucket then our job is done, we can just 
                # continue in the loop
                self.log(
                    f"Object {object_name} already exists in {bucket_name}. "
                    f"Skipping to next archive retrieval.", self.RK_LOG_INFO
                )
                self.completelist.append(path_details)
                continue

            tape_path = path_details.tape_path
            try:
                # Split out the root and path, passed from the Location
                tape_location_root, original_path = tape_path.split(":")
                # Split further to get the holding_slug and the tar filename
                holding_slug, filelist_hash = tape_location_root.split("_")
            except ValueError as e:
                reason = f"Could not unpack mandatory info from path_details"
                self.log(f"{reason}. Adding {item_path} to retry list.", 
                         self.RK_LOG_ERROR)
                path_details.retries.increment(reason=reason)
                self.retrylist.append(path_details)
                continue

            tar_filename = f"{filelist_hash}.tar"
            holding_tape_path = (f"root://{tape_server}/{tape_base_dir}/"
                                 f"{holding_slug}")
            full_tape_path = (f"{holding_tape_path}/{tar_filename}")

            # Check bucket folder exists on tape
            status, _ = fs_client.dirlist(f"{tape_base_dir}/{holding_slug}", 
                                          DirListFlags.STAT)
            if status.status != 0:
                # If bucket tape-folder can't be found then pass for retry
                reason = (f"Tape holding folder ({tape_base_dir}/{holding_slug})"
                          f" could not be found, cannot retrieve from archive")
                self.log(f"{reason}. Adding {item_path} to retry list.", 
                         self.RK_LOG_ERROR)
                path_details.retries.increment(reason=reason)
                self.retrylist.append(path_details)
                continue
            
            self.log(f"Attempting to stream contents of tar file "
                     f"{full_tape_path} directly from tape archive to object "
                     "storage", self.RK_LOG_INFO)
            
            # Attempt to stream directly from the tar file to object store            
            try:
                result = None
                # Prepare the file for reading. The tar_filename must be encoded 
                # into a list of byte strings for prepare, as of pyxrootd v5.5.3
                prepare_bytes = (f"{tape_base_dir}/"
                                 f"{holding_slug}/"
                                 f"{tar_filename}").encode("utf_8")
                # NOTE: This may take a while as it's being copied to a disk 
                # cache, should benchmark.
                status, _ = fs_client.prepare([prepare_bytes, ], 
                                              PrepareFlags.STAGE)
                if status.status != 0:
                    # If tarfile can't be staged then pass for retry
                    raise ArchiveError(f"Tar file ({prepare_bytes.decode()}) "
                                       f"could not be prepared.")
                
                with client.File() as f:
                    # Open the tar file with READ
                    status, _ = f.open(full_tape_path, OpenFlags.READ)
                    if status.status != 0:
                        raise ArchiveError("Failed to open file for reading")

                    # Wrap the xrootd File handler so minio can use it 
                    fw = AdlerisingXRDFile(f)

                    self.log(f"Opening tar file {tar_filename}", self.RK_LOG_INFO)
                    with tarfile.open(mode='r:', copybufsize=chunk_size, 
                                      fileobj=fw) as tar:
                        members = tar.getmembers()
                        if original_path not in [ti.name for ti in members]:
                            # Something has gone wrong if the file is not 
                            # actually in the tar
                            continue
                        for tarinfo in members:
                            self.log(f"Starting stream of {tarinfo.name} to "
                                     f"object store.", self.RK_LOG_DEBUG)
                            # Extract the file as a file object, this makes a 
                            # thin wrapper around the filewrapper we're already 
                            # using and maintains chunked reading
                            f = tar.extractfile(tarinfo)
                            result = s3_client.put_object(
                                bucket_name, 
                                tarinfo.name, 
                                f, 
                                -1, 
                                part_size=chunk_size,
                            )
                            self.log(f"Minio put result: {result}", 
                                     self.RK_LOG_DEBUG)

            except (HTTPError, ArchiveError) as e:
                reason = f"Stream-time exception occurred: {e}"
                self.log(reason, self.RK_LOG_DEBUG)
                self.log(f"Exception encountered during stream, adding "
                         f"{object_name} to retry-list.", self.RK_LOG_ERROR)
                path_details.retries.increment(reason=str(e))
                self.retrylist.append(path_details)
            else:
                # Log successful 
                self.log(f"Successfully got {path_details.original_path}", 
                         self.RK_LOG_DEBUG)
                self.completelist.append(path_details)
            finally:
                # NOTE: This block may be redundant, but could also be why the 
                # writes were failing
                result.close()
                result.release_conn()
                continue

        self.log("Archive complete, passing lists back to worker for "
                 "re-routing and cataloguing.", self.RK_LOG_INFO)

        # Send whatever remains after all items have been (attempted to be) put
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist, rk_complete, body_json, 
                mode=FilelistType.archived,
            )
        if len(self.retrylist) > 0:
            self.send_pathlist(
                self.retrylist, rk_retry, body_json, 
                mode=FilelistType.retry
            )
        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist, rk_failed, body_json, 
                mode=FilelistType.failed
            )

def main():
    consumer = GetArchiveConsumer()
    consumer.run()

if __name__ == "__main__":
    main()