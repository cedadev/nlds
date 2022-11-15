from typing import List, Dict
from pathlib import Path
import os
from uuid import UUID

import minio
from minio.error import S3Error
from retry import retry

from nlds_processors.transferers.base_transfer import BaseTransferConsumer
from nlds.rabbit.consumer import FilelistType, State
from nlds.details import PathDetails


class GetTransferConsumer(BaseTransferConsumer):
    DEFAULT_QUEUE_NAME = "transfer_get_q"
    DEFAULT_ROUTING_KEY = (f"{BaseTransferConsumer.RK_ROOT}."
                           f"{BaseTransferConsumer.RK_TRANSFER_GET}."
                           f"{BaseTransferConsumer.RK_WILD}")
    DEFAULT_STATE = State.TRANSFER_GETTING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
    
    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, filelist: List[PathDetails], rk_origin: str,
                 body_json: Dict[str, str]) -> None:
        # Grab message details for ease of access. This shouldn't fail becuase
        # of checking done in the worker, might make sense to do it here/in the 
        # basetransfer too though
        msg_details = body_json[self.MSG_DETAILS]
        
        # Get target and verify it can be written to
        target = msg_details[self.MSG_TARGET]
        if target:
            target_path = Path(target)
            if not self.check_path_access(target_path, access=os.W_OK):
                # NOTE: should we retry here?
                self.log("Unable to copy, given target path is inaccessible. "
                        "Exiting callback", 
                        self.RK_LOG_ERROR)
                return

        # Create client!
        client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        rk_complete = ".".join([rk_origin, self.RK_TRANSFER_GET, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_TRANSFER_GET, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_TRANSFER_GET, self.RK_FAILED])

        for path_details in filelist:
            if path_details.retries > self.max_retries:
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
                self.log("Unable to get bucket_name from message info, adding "
                         f"{path_details.object_name} to retry list.", 
                         self.RK_LOG_INFO)
                path_details.increment_retry(retry_reason="non-existent-bucket")
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="retry"
                )
                continue

            if bucket_name and not client.bucket_exists(bucket_name):
                # If bucket doesn't exist then pass for retry
                self.log("Transaction_id doesn't match any buckets. Adding "
                         f"{object_name} to retry list.", self.RK_LOG_ERROR)
                path_details.increment_retry(retry_reason="non-existent-bucket")
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="retry"
                )
                continue

            self.log(f"Attempting to get file {object_name} from {bucket_name}", 
                     self.RK_LOG_DEBUG)

            if (self.remove_root_slash_fl and 
                    object_name[0] == "/"):
                object_name = object_name[1:]

            # Decide whether to prepend target path or download directly to it.
            if not target:
                # In the case of no given target, we just download the files 
                # back to their original location. 
                download_path = path_details.original_path
                # Check we have permission to write to the parent folder of the 
                # original location. If the parent folder doesn't exist this 
                # will fail.
                if not self.check_path_access(path_details.path.parent, 
                                              access=os.W_OK):
                    self.log(f"Unable to download {download_path}, given target"
                             " path is inaccessible. Adding to retry-list.", 
                             self.RK_LOG_INFO)
                    path_details.increment_retry(retry_reason="inaccessible")
                    self.append_and_send(path_details, rk_retry, body_json, 
                                         list_type=FilelistType.retry)
                    continue
            elif target_path.is_dir():
                # In the case of a given target, we remove the leading slash on 
                # the original path and prepend the target_path
                if path_details.original_path[0] == '/':
                    download_path = target_path / path_details.original_path[1:]
                else:
                    download_path = target_path / path_details.original_path
            else:
                # TODO (2022-09-20): This probably isn't appropriate for getlist
                self.log("Target path is not a valid directory, renaming files "
                         "gotten to target path.", self.RK_LOG_WARNING)
                download_path = target_path

            # Attempt the download!
            try:
                result = client.fget_object(
                    bucket_name, object_name, str(download_path),
                )
            except Exception as e:
                self.log(f"Download-time exception occurred: {e}", 
                         self.RK_LOG_DEBUG)
                self.log(f"Exception encountered during download, adding "
                         f"{object_name} to retry-list.", self.RK_LOG_INFO)
                path_details.increment_retry(retry_reason="exception at download "
                                                          "time")
                self.append_and_send(path_details, rk_retry, body_json, 
                                     list_type=FilelistType.retry)
                continue

            self.log(f"Successfully got {path_details.original_path}", 
                     self.RK_LOG_DEBUG)
            self.append_and_send(path_details, rk_complete, body_json, 
                                 list_type=FilelistType.transferred)

        # Send whatever remains after all items have been (attempted to be) put
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist, rk_complete, body_json, 
                mode=FilelistType.transferred,
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
    consumer = GetTransferConsumer()
    consumer.run()

if __name__ == "__main__":
    main()