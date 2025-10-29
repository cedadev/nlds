# encoding: utf-8
"""
put_transfer.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import List, Dict, Any

import minio
from minio.error import S3Error
from retry import retry
from urllib3.exceptions import HTTPError, MaxRetryError

from nlds_processors.transfer.base_transfer import BaseTransferConsumer
from nlds.rabbit.consumer import State
from nlds.details import PathDetails, PathType
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG
from nlds_processors.transfer.transfer_error import TransferError
from nlds_processors.bucket_mixin import BucketMixin, BucketError


class PutTransferConsumer(BaseTransferConsumer, BucketMixin):
    DEFAULT_QUEUE_NAME = "transfer_put_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.TRANSFER_PUT}." f"{RK.WILD}"
    DEFAULT_STATE = State.TRANSFER_PUTTING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
        self.s3_client = None

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
        try:
            bucket_name = self._get_bucket_name(transaction_id)
        except BucketError as e:
            raise RuntimeError(e.message)

        if self.s3_client is None:
            raise RuntimeError("self.s3_client is None")

        for path_details in filelist:
            # Don't transfer symbolic links, but do acknowledge them
            if path_details.path_type == PathType.LINK:
                self.completelist.append(path_details)
                continue

            # Get the path
            item_path = path_details.path

            # If check_permissions active then check again that file exists and
            # is accessible.
            if not self.check_path_access(item_path):
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
                result = self.s3_client.fput_object(
                    bucket_name,
                    pl.path,
                    path_details.original_path,
                )
                self.log(
                    f"Successfully uploaded {path_details.original_path} to "
                    f"bucket {bucket_name} with object_name {pl.path}",
                    RK.LOG_DEBUG,
                )
                self.log(f"Uploaded {path_details.original_path}", RK.LOG_INFO)
                self.append_and_send(
                    self.completelist,
                    path_details,
                    routing_key=rk_complete,
                    body_json=body_json,
                    state=State.TRANSFER_PUTTING,
                )
            except (HTTPError, MaxRetryError, PermissionError) as e:
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

    def _parse_group(self, body_json: Dict[str, Any]):
        # get the group from the details section of the message
        try:
            group = body_json[MSG.DETAILS][MSG.GROUP]
            if group is None:
                raise ValueError
        except (KeyError, ValueError):
            msg = "Group not in message, exiting callback."
            self.log(msg, RK.LOG_ERROR)
            raise TransferError(message=msg)
        return group

    @retry((S3Error, BucketError), tries=5, delay=10, backoff=10, logger=None)
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
        self.s3_client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        rk_complete = ".".join([rk_origin, RK.TRANSFER_PUT, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.TRANSFER_PUT, RK.FAILED])

        group = self._parse_group(body_json=body_json)

        try:
            bucket_name = self._get_bucket_name(transaction_id=transaction_id)
            # set access policies only if bucket is created
            if (self._make_bucket(bucket_name)):
                self._set_access_policies(bucket_name=bucket_name, group=group)
        except BucketError as e:
            # If the bucket cannot be created, due to a S3 error, then fail all the
            # files in the transaction
            for f in filelist:
                f.failure_reason = f"S3 error: {e.message} when creating bucket"
                self.append_and_send(
                    self.failedlist,
                    f,
                    routing_key=rk_failed,
                    body_json=body_json,
                    state=State.FAILED,
                )
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
