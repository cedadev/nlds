from abc import ABC
from typing import List, Any, Dict
from retry import retry
import minio
import urllib3
import certifi
from urllib3.util import Timeout, Retry

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

    def _create_s3_client(
        self, tenancy: str, access_key: str, secret_key: str, secure: bool
    ):
        # Create a minio S3 client with a custom http client with a timeout of 24
        # hours
        _http = urllib3.PoolManager(
            timeout=Timeout(connect=self.http_timeout, read=self.http_timeout),
            maxsize=10,
            cert_reqs="CERT_NONE",
            ca_certs=certifi.where(),
            retries=Retry(
                total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]
            ),
        )
        # create Minio client with supplied info and custom http client created above
        _client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            http_client=_http,
        )
        return _client

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
        self.s3_client = self._create_s3_client(
            tenancy=tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        failed = False
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
            failed = True
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
        elif not failed:
            # complete
            self.publish_message(rk_complete, body_json)
