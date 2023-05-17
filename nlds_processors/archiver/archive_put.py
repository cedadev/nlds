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

class PutArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_put_q"
    DEFAULT_ROUTING_KEY = (f"{BaseArchiveConsumer.RK_ROOT}."
                           f"{BaseArchiveConsumer.RK_TRANSFER_PUT}."
                           f"{BaseArchiveConsumer.RK_WILD}")
    DEFAULT_STATE = State.ARCHIVE_PUTTING


    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)


    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, tape_url: str, filelist: List[PathDetails], 
                 rk_origin: str, body_json: Dict[str, str]):
        # Grab message details for ease of access. This shouldn't fail becuase
        # of checking done in the worker, might make sense to do it here/in the 
        # basetransfer too though
        msg_details = body_json[self.MSG_DETAILS]

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
            self.log(f"Failed status message: {status.message}", 
                     self.RK_LOG_ERROR)
            raise CallbackError(f"Base dir {tape_base_dir} derived from "
                                f"tape_url ({tape_url}) could not be created or"
                                f" verified.")

        rk_complete = ".".join([rk_origin, self.RK_ARCHIVE_PUT, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_ARCHIVE_PUT, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_ARCHIVE_PUT, self.RK_FAILED])

        for path_details in filelist:
            item_path = path_details.path

            # First check whether index item has failed too many times
            if path_details.retries.count > self.max_retries:
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="failed"
                )
                continue

            # If bucketname inserted into object path (i.e. from catalogue) then 
            # extract both
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

            if bucket_name and not s3_client.bucket_exists(bucket_name):
                # If bucket doesn't exist then fail for retrying
                reason = (f"bucket_name {bucket_name} doesn't match any "
                          "exisiting buckets in object store.")
                self.log(f"{reason}. Adding {object_name} to retry list.", 
                         self.RK_LOG_ERROR)
                path_details.retries.increment(reason=reason)
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="retry"
                )
                continue
            
            # Make bucket folder and retry if it can't be created. 
            status, _ = fs_client.mkdir(f"{tape_base_dir}/{bucket_name}", 
                                        MkDirFlags.MAKEPATH)
            if status.status != 0:
                # If bucket directory couldn't be created then fail for retrying
                reason = (f"Couldn't create or find bucket directory "
                          f"({tape_base_dir}/{bucket_name})")
                self.log(f"{reason}. Adding {object_name} to retry list.", 
                         self.RK_LOG_ERROR)
                path_details.retries.increment(reason=reason)
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="retry"
                )
                continue
                

            self.log(f"Attempting to stream file {path_details.original_path} "
                     "directly to tape archive", 
                     self.RK_LOG_DEBUG)
            
            # NOTE: Do we want to use the object name or the original path here?
            tape_full_path = (f"root://{tape_server}/{tape_base_dir}/"
                              f"{bucket_name}/{object_name}")

            # Attempt to stream the object directly into the File object            
            try:
                # NOTE: Slightly unclear which way round to nest this, going to 
                # keep it this way round for now but it may be that in the 
                # future, if we decide that we should be pre-allocating chunks 
                # to write at the policy level, we might decide to change it 
                # round
                result = s3_client.get_object(
                    bucket_name, object_name,
                )
                with client.File() as f:
                    # Open the file as NEW, to avoid having to prepare it
                    status, _ = f.open(tape_full_path, 
                                       OpenFlags.NEW | OpenFlags.MAKEPATH)
                    if status.status != 0:
                        raise ArchiveError("Failed to open file for writing")

                    # Stream the file one chunk at a time
                    # NOTE: this could take a while if the file is large, might 
                    # need a plan to deal with that 
                    pos = 0
                    for chunk in result.stream(self.chunk_size):
                        to_write = min(self.chunk_size, result.length_remaining)
                        f.write(chunk, offset=pos, size=to_write)
                        pos += to_write
                    
                    if (result.length_remaining != 0 
                            and result.length_remaining <= self.chunk_size):
                        f.write(result.read(result.length_remaining)) 
                    else:
                        self.log(f"remaining = {result.length_remaining}, "
                                 f"chunk_size = {self.chunk_size}, pos = {pos}", 
                                 self.RK_LOG_DEBUG)
                        raise ArchiveError("Streaming seems to have failed due "
                                           "to incomplete stream read.")

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
                result.close()
                result.release_conn()
                continue

        self.log("Archive complete, passing lists back to worker for "
                 "re-routing and cataloguing.", self.RK_LOG_INFO)
        
        # Send whatever remains after all items have been (attempted to be) put
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist, rk_complete, body_json, mode="archived"
            )
        if len(self.retrylist) > 0:
            self.send_pathlist(
                self.retrylist, rk_retry, body_json, mode="retry"
            )
        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist, rk_failed, body_json, mode="failed"
            )

    
def main():
    consumer = PutArchiveConsumer()
    consumer.run()

if __name__ == "__main__":
    main()