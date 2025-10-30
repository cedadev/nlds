from abc import ABC
from typing import List, Any, Dict
from retry import retry
import minio

from nlds_processors.transfer.transfer_error import TransferError
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG
from nlds_processors.transfer.base_transfer import BaseTransferConsumer
from nlds.rabbit.consumer import State
from nlds.details import PathDetails
from minio.error import S3Error
from nlds_processors.bucket_mixin import BucketError, BucketMixin

class BucketTransferConsumer(BaseTransferConsumer, BucketMixin, ABC):
    """Class for transfers that need to create a bucket"""

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

    @retry((S3Error, BucketError), tries=5, delay=1, backoff=2, logger=None)
    def setup(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, Any],
    ):
        """Setup creates the bucket"""
        group = self._parse_group(body_json=body_json)
        
        rk_complete = ".".join([rk_origin, RK.TRANSFER_SETUP, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.TRANSFER_SETUP, RK.FAILED])

        # create a new client as the access key and secret key could (probably will)
        # change between messages
        self.s3_client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        try:
            bucket_name = self._get_bucket_name(transaction_id=transaction_id)
            # set access policies only if bucket is created
            if self._make_bucket(bucket_name):
                self._set_access_policies(bucket_name=bucket_name, group=group)
            self.log(
                f"Created bucket on tenancy: {tenancy} with name: {bucket_name}",
                RK.LOG_INFO,
            )
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

        # either all files fail or it completes
        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body_json,
                state=State.FAILED,
            )
        else:
            # complete
            self.publish_message(rk_complete, body_json)
