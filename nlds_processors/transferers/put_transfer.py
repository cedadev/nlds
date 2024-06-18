from typing import List, Dict, Any

import minio
from minio.error import S3Error
from retry import retry
from urllib3.exceptions import HTTPError

from nlds_processors.transferers.base_transfer import BaseTransferConsumer
from nlds.rabbit.consumer import State
from nlds.details import PathDetails
import nlds.rabbit.routing_keys as RK
from nlds_processors.transferers.transfer_error import TransferError


class PutTransferConsumer(BaseTransferConsumer):
    DEFAULT_QUEUE_NAME = "transfer_put_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.TRANSFER_PUT}." f"{RK.WILD}"
    DEFAULT_STATE = State.TRANSFER_PUTTING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
        self.client = None

    @staticmethod
    def _get_bucket_name(transaction_id: str):
        bucket_name = f"nlds.{transaction_id}"
        return bucket_name

    def _make_bucket(self, transaction_id: str):
        assert self.client is not None

        bucket_name = self._get_bucket_name(transaction_id)
        # Check that bucket exists, and create if not
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                self.log(
                    f"Creating bucket ({bucket_name}) for this" " transaction",
                    RK.LOG_INFO,
                )
            else:
                self.log(
                    f"Bucket for this transaction ({transaction_id}) "
                    f"already exists",
                    RK.LOG_INFO,
                )
            return None
        except minio.error.S3Error as e:
            raise TransferError(message=str(e))

    def _transfer_files(
        self,
        transaction_id: str,
        tenancy: str,
        filelist: list,
        rk_origin: str,
        body_json: Dict[str, Any],
    ):
        """Transfer the files to the Object Storage"""
        rk_complete = ".".join([rk_origin, RK.TRANSFER_PUT, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.TRANSFER_PUT, RK.FAILED])
        bucket_name = self._get_bucket_name(transaction_id)

        assert self.client is not None

        for path_details in filelist:
            item_path = path_details.path

            # If check_permissions active then check again that file exists and
            # is accessible.
            if self.check_permissions_fl and not self.check_path_access(item_path):
                reason = f"Path:{path_details.path} is inaccessible."
                self.log(reason, RK.LOG_DEBUG)
                path_details.failure_reason = reason
                self.append_and_send(
                    self.failedlist,
                    path_details,
                    routing_key=rk_failed,
                    body_json=body_json,
                    state=State.FAILED,
                )
                continue

            self.log(
                f"Attempting to upload file {path_details.original_path}", RK.LOG_DEBUG
            )

            # Add this to the PathDetails as the StorageLocation
            pl = path_details.set_object_store(tenancy=tenancy, bucket=transaction_id)
            try:
                result = self.client.fput_object(
                    bucket_name, pl.path, path_details.original_path,
                )
                self.log(
                    f"Successfully uploaded {path_details.original_path} to "
                    f"bucket {bucket_name} with object_name {pl.path}", 
                    RK.LOG_DEBUG
                )
                self.append_and_send(
                    self.completelist,
                    path_details,
                    routing_key=rk_complete,
                    body_json=body_json,
                    state=State.TRANSFER_PUTTING,
                )
            except (HTTPError, minio.error.S3Error) as e:
                reason = (
                    f"Error uploading {path_details.path} to object " f"store: {e}."
                )
                self.log(f"{reason} Adding to failed list.", RK.LOG_ERROR)
                path_details.failure_reason = reason
                self.append_and_send(
                    self.failedlist,
                    path_details,
                    routing_key=rk_failed,
                    body_json=body_json,
                    state=State.FAILED,
                )
                continue

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
        self.client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        rk_complete = ".".join([rk_origin, RK.TRANSFER_PUT, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.TRANSFER_PUT, RK.FAILED])

        try:
            self._make_bucket(transaction_id)
        except TransferError as e:
            # If the bucket cannot be created, due to a S3 error, then fail all the
            # files in the transaction
            for f in filelist:
                f.failure_reason = f"S3 error: {e.message} when creating bucket"
                self.failedlist.append(f)
        else:
            self._transfer_files(
                transaction_id=transaction_id,
                tenancy=tenancy,
                filelist=filelist,
                rk_origin=rk_origin,
                body_json=body_json,
            )

        self.log(
            "Transfer complete, passing lists back to worker for "
            "re-routing and cataloguing.",
            RK.LOG_INFO,
        )

        # Send whatever remains after all items have been (attempted to be) put
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body_json,
                state=State.TRANSFER_PUTTING,
            )
        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body_json,
                state=State.FAILED,
            )


def main():
    consumer = PutTransferConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
