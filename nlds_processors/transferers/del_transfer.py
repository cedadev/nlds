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
from nlds.errors import CallbackError


class DelTransferConsumer(BaseTransferConsumer):
    DEFAULT_QUEUE_NAME = "transfer_del_q"
    DEFAULT_ROUTING_KEY = (f"{BaseTransferConsumer.RK_ROOT}."
                           f"{BaseTransferConsumer.RK_TRANSFER_DEL}."
                           f"{BaseTransferConsumer.RK_WILD}")
    DEFAULT_STATE = State.TRANSFER_DELETING

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
        # Create client!
        client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        rk_complete = ".".join([rk_origin, self.RK_TRANSFER_DEL, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_TRANSFER_DEL, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_TRANSFER_DEL, self.RK_FAILED])

        for path_details in filelist:
            # check if this has failed too often
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

            if bucket_name and not client.bucket_exists(bucket_name):
                # If bucket doesn't exist then pass for retry
                reason = (f"Bucket {bucket_name} doesn't seem to exist")
                self.log(f"{reason}. Adding {object_name} to retry list.", 
                         self.RK_LOG_ERROR)
                path_details.retries.increment(reason=reason)
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="retry"
                )
                continue

            # try to do the deletion
            try:
                raise Exception("Hello failure")
            except Exception as e:
                reason = f"Delete-time exception occurred: {e}"
                self.log(reason, self.RK_LOG_DEBUG)
                self.log(f"Exception encountered during deletion, adding "
                         f"{object_name} to retry-list.", self.RK_LOG_INFO)
                path_details.retries.increment(reason=reason)
                self.append_and_send(path_details, rk_retry, body_json, 
                                     list_type=FilelistType.retry)
                continue

            self.log(f"Successfully got {path_details.original_path}", 
                     self.RK_LOG_DEBUG)
            self.append_and_send(path_details, rk_complete, body_json, 
                                 list_type=FilelistType.deleted)


        # Send whatever remains after all items have been (attempted to be) put
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist, rk_complete, body_json, 
                mode=FilelistType.deleted,
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
    consumer = DelTransferConsumer()
    consumer.run()

if __name__ == "__main__":
    main()