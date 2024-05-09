from typing import List, Dict, Any

import minio
from minio.error import S3Error
from retry import retry
from urllib3.exceptions import HTTPError

from nlds_processors.transferers.base_transfer import BaseTransferConsumer
from nlds.rabbit.consumer import FilelistType, State
from nlds.details import PathDetails
import nlds.rabbit.routing_keys as RK


class PutTransferConsumer(BaseTransferConsumer):
    DEFAULT_QUEUE_NAME = "transfer_put_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.TRANSFER_PUT}." f"{RK.WILD}"
    DEFAULT_STATE = State.TRANSFER_PUTTING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, Any],
    ):
        client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        rk_complete = ".".join([rk_origin, RK.TRANSFER_PUT, RK.COMPLETE])
        rk_retry = ".".join([rk_origin, RK.TRANSFER_PUT, RK.START])
        rk_failed = ".".join([rk_origin, RK.TRANSFER_PUT, RK.FAILED])

        # First check for transaction-level message failure and boot back to
        # catalog if necessary.
        retries = self.get_retries(body_json)
        if retries is not None and retries.count > self.max_retries:
            # Mark the message as 'processed' so it can be failed more safely.
            self.send_pathlist(
                filelist, rk_failed, body_json, state=State.CATALOG_ROLLBACK
            )
            return

        bucket_name = f"nlds.{transaction_id}"

        # Check that bucket exists, and create if not
        # TODO: Does this create a failure mode whereby if a transaction fails
        # at the transaction level then no failure message is sent.
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            self.log(
                f"Creating bucket ({bucket_name}) for this" " transaction", RK.LOG_INFO
            )
        else:
            self.log(
                f"Bucket for this transaction ({transaction_id}) " f"already exists",
                RK.LOG_INFO,
            )

        for path_details in filelist:
            item_path = path_details.path

            # First check whether index item has failed too many times
            if path_details.retries.count > self.max_retries:
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="failed"
                )
                continue

            # If check_permissions active then check again that file exists and
            # is accessible.
            if self.check_permissions_fl and not self.check_path_access(item_path):
                reason = f"Path:{path_details.path} is inaccessible."
                self.log(reason, RK.LOG_DEBUG)
                path_details.retries.increment(reason=reason)
                self.append_and_send(
                    path_details, rk_retry, body_json, list_type="retry"
                )
                continue

            self.log(
                f"Attempting to upload file {path_details.original_path}", RK.LOG_DEBUG
            )

            # NOTE: (2023-01-19) Removing the remove_root_slash_fl options as it
            # causes problems with the reorganisation of the workflow for
            # duplicate file catching. Given we're not even using the feature
            # this seems a better solution than having to update the location in
            # the catalog once transfer has completed. In the future we may want
            # to encode the object name differently but this can be dealt with
            # when that occurs.
            # TODO: This begs the question of whether we need to store the
            # object-name at all
            path_details.object_name = path_details.original_path
            try:
                result = client.fput_object(
                    bucket_name,
                    path_details.object_name,
                    path_details.original_path,
                )
                self.log(
                    f"Successfully uploaded {path_details.original_path}", RK.LOG_DEBUG
                )
                self.append_and_send(
                    path_details,
                    rk_complete,
                    body_json,
                    list_type=FilelistType.transferred,
                )
            except HTTPError as e:
                reason = (
                    f"Error uploading {path_details.path} to object " f"store: {e}."
                )
                self.log(f"{reason} Adding to retry list.", RK.LOG_ERROR)
                path_details.retries.increment(reason=reason)
                self.append_and_send(
                    path_details, rk_retry, body_json, list_type="retry"
                )
                continue

        self.log(
            "Transfer complete, passing lists back to worker for "
            "re-routing and cataloguing.",
            RK.LOG_INFO,
        )

        # Send whatever remains after all items have been (attempted to be) put
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist, rk_complete, body_json, mode="transferred"
            )
        if len(self.retrylist) > 0:
            self.send_pathlist(self.retrylist, rk_retry, body_json, mode="retry")
        if len(self.failedlist) > 0:
            self.send_pathlist(self.failedlist, rk_failed, body_json, mode="failed")


def main():
    consumer = PutTransferConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
