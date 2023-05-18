from typing import List, Dict
from pathlib import Path
import os

import minio
from minio.error import S3Error
from retry import retry
from urllib3.exceptions import HTTPError
from XRootD import client
from XRootD.client.flags import (DirListFlags, PrepareFlags, DirListFlags, 
                                 OpenFlags, MkDirFlags, QueryCode)

from nlds_processors.archiver.archive_base import (BaseArchiveConsumer, 
                                                   ArchiveError)
from nlds.rabbit.consumer import FilelistType, State
from nlds.details import PathDetails
from nlds.errors import CallbackError

class XRDFileWrapper():
    """Wrapper class around the XRootD.File object to make it act more like a 
    regular python IO object, specifically a BytesIO, which allows the 
    put_object() method from minio to be used.
    """

    def __init__(self, f: client.File, offset=0, length=0):
        self.f = f
        self.offset = offset
        self.length = length
        self.pointer = 0

    def read(self, size):
        status, result = self.f.read(offset=self.pointer, size=size)
        if status.status != 0:
            raise IOError(f"Unable to read from file f ({self.f})")
        self.pointer += size
        return result
    
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
        # Attempt to create it, should succeed if everything is working properly
        # but otherwise will be passed for message-level retry. 
        status, _ = fs_client.mkdir(tape_base_dir, MkDirFlags.MAKEPATH)
        if status.status != 0:
            raise CallbackError(f"Base dir derived from tape_url ({tape_url}) "
                                f"could not be created or verified.")
        
        # Declare useful variables
        bucket_name = None
        rk_complete = ".".join([rk_origin, self.RK_ARCHIVE_GET, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_ARCHIVE_GET, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_ARCHIVE_GET, self.RK_FAILED])

        # Main loop, for each file. 
        for path_details in filelist:
            # First check whether index item has failed too many times
            if path_details.retries.count > self.max_retries:
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="failed"
                )
                continue
            elif len(path_details.object_name.split(':')) == 2:
                bucket_name, object_name = path_details.object_name.split(':')
            # Otherwise, log error and queue for retry
            else:
                reason = "Unable to get bucket_name from message info"
                self.log(f"{reason}, adding "
                         f"{path_details.object_name} to retry list.", 
                         self.RK_LOG_INFO)
                path_details.retries.increment(reason=reason)
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="retry"
                )
                continue

            # If bucket doesn't exist then pass for retry
            if bucket_name and not client.bucket_exists(bucket_name):
                reason = (f"transaction_id {transaction_id} doesn't match any "
                           "buckets")
                self.log(f"{reason}. Adding {object_name} to retry list.", 
                         self.RK_LOG_ERROR)
                path_details.retries.increment(reason=reason)
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="retry"
                )
                continue

            # we're not concerned about permissions checking here.

            # Check bucket folder exists on tape
            status, _ = fs_client.mkdir(f"{tape_base_dir}/{bucket_name}", 
                                        MkDirFlags.MAKEPATH)
            
            self.log(f"Attempting to stream file {path_details.original_path} "
                     "directly from tape archive to object storage", 
                     self.RK_LOG_DEBUG)
            
            # NOTE: Do we want to use the object name or the original path here?
            tape_full_path = (f"root://{tape_server}//{tape_base_dir}/"
                         f"{bucket_name}/{object_name}")
            
            # Attempt to stream the object directly into the File object            
            try:
                # NOTE: Slightly unclear which way round to nest this, going to 
                # keep it this way round for now but it may be that in the 
                # future, if we decide that we should be pre-allocating chunks 
                # to write at the policy level, we might decide to change it 
                # round
                
                with client.File() as f:
                    # Open the file as NEW, to avoid having to prepare it
                    status, _ = f.open(tape_full_path, OpenFlags.READ)
                    if status.status != 0:
                        raise ArchiveError("Failed to open file for reading")

                    # Wrap the File handler so minio can use it 
                    fw = XRDFileWrapper(f)

                    chunk_size = min(5*1024*1024, self.chunk_size)

                    s3_client.put_object(
                        bucket_name, 
                        object_name, 
                        fw, 
                        -1, 
                        part_size=chunk_size, # This is the minimum part size.
                    )

            except (HTTPError, ArchiveError) as e:
                reason = f"Stream-time exception occurred: {e}"
                self.log(reason, self.RK_LOG_DEBUG)
                self.log(f"Exception encountered during stream, adding "
                         f"{object_name} to retry-list.", self.RK_LOG_INFO)
                path_details.retries.increment(reason=reason)
                self.append_and_send(path_details, rk_retry, body_json, 
                                     list_type=FilelistType.retry)
            # TODO: Another error here for CTA exceptions? It just seems to 
            # return a non-zero status object, not explicit exceptions. 
            else:
                # Log successful 
                self.log(f"Successfully got {path_details.original_path}", 
                         self.RK_LOG_DEBUG)
                self.append_and_send(path_details, rk_complete, body_json, 
                                     list_type=FilelistType.archived)
            finally:
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