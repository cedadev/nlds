from typing import List, Dict, Any
from pathlib import Path
import os
import subprocess

import minio
from minio.error import S3Error
from retry import retry

from nlds_processors.transferers.base_transfer import BaseTransferConsumer
from nlds.rabbit.consumer import State
from nlds.details import PathDetails
from nlds.errors import CallbackError
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


class GetTransferConsumer(BaseTransferConsumer):
    DEFAULT_QUEUE_NAME = "transfer_get_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.TRANSFER_GET}." f"{RK.WILD}"
    DEFAULT_STATE = State.TRANSFER_GETTING

    _CHOWN_COMMAND = "chown_cmd"
    _CHOWN_FL = "chown_fl"
    TRANSFER_GET_CONSUMER_CONFIG = {_CHOWN_COMMAND: "chown", _CHOWN_FL: False}
    DEFAULT_CONSUMER_CONFIG = (
        TRANSFER_GET_CONSUMER_CONFIG | BaseTransferConsumer.DEFAULT_CONSUMER_CONFIG
    )

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.chown_cmd = self.load_config_value(self._CHOWN_COMMAND)
        self.chown_fl = self.load_config_value(self._CHOWN_FL)
        self.client = None

    def _get_target_path(self, body_json: Dict):
        # Get target and verify it can be written to
        target = body_json[MSG.DETAILS][MSG.TARGET]
        if target:
            target_path = Path(target)
            if not self.check_path_access(target_path, access=os.W_OK):
                self.log(f"Full path: {target_path}", RK.LOG_DEBUG)
                raise CallbackError(
                    "Unable to copy, given target path is "
                    "inaccessible. Passing for retry."
                )
        else:
            target_path = None
        return target_path

    def _get_and_check_bucket_name_object_name(self, path_details):
        """Get the bucket and object name and perform an existence check on the
        bucket"""
        assert(self.client is not None)

        if len(path_details.object_name.split(":")) == 2:
            bucket_name, object_name = path_details.object_name.split(":")
        # Otherwise, log error and queue for retry
        else:
            reason = "Unable to get bucket_name from message info"
            self.log(
                f"{reason}, adding " f"{path_details.object_name} to failed list.",
                RK.LOG_INFO,
            )
            return None, None, reason

        if bucket_name and not self.client.bucket_exists(bucket_name):
            # If bucket doesn't exist then pass for failure
            reason = f"Bucket {bucket_name} does not exist"
            self.log(f"{reason}. Adding {object_name} to failed list.", RK.LOG_ERROR)
            return None, None, reason

        return bucket_name, object_name, None

    def _get_download_path(self, path_details, target_path):
        # Decide whether to prepend target path or download directly to it.
        if not target_path:
            # In the case of no given target, we just download the files
            # back to their original location.
            download_path = path_details.path
            # Check we have permission to write to the parent folder of the
            # original location. If the parent folder doesn't exist this
            # will fail.
            if not self.check_path_access(path_details.path.parent, access=os.W_OK):
                reason = (
                    f"Unable to download {download_path}.  Target "
                    "path is inaccessible."
                )
                self.log(f"{reason}. Adding to failed list.", RK.LOG_INFO)
                return None, reason

        elif target_path.is_dir():
            # In the case of a given target, we remove the leading slash on
            # the original path and prepend the target_path
            if path_details.original_path[0] == "/":
                download_path = target_path / path_details.original_path[1:]
            else:
                download_path = target_path / path_details.original_path
        else:
            # TODO (2022-09-20): This probably isn't appropriate for getlist
            self.log(
                "Target path is not a valid directory, renaming files "
                "got to target path.",
                RK.LOG_WARNING,
            )
            download_path = target_path
        return download_path, None

    def _transfer(self, bucket_name, object_name, download_path):
        assert(self.client is not None)
        download_path_str = str(download_path)
        # Attempt the download!
        try:
            resp = self.client.fget_object(
                bucket_name,
                object_name,
                download_path_str,
            )
            return None
        except Exception as e:
            reason = f"Download-time exception occurred: {e}"
            self.log(reason, RK.LOG_DEBUG)
            self.log(
                f"Exception encountered during download, adding "
                f"{object_name} to failed list.",
                RK.LOG_INFO,
            )
            return reason

    def _change_permissions(self, download_path, path_details):
        """Change the permission of the downloaded files"""
        download_path_str = str(download_path)
        self.log(
            f"Changing permissions and ownership of file " f"{download_path}",
            RK.LOG_ERROR,
        )
        try:
            # Attempt to change back to original permissions
            os.chmod(download_path, mode=path_details.permissions)
        except (KeyError, PermissionError) as e:
            self.log("Couldn't change permissions of downloaded file", RK.LOG_WARNING)
            self.log(f"Original error: {e}", RK.LOG_DEBUG)

        # Attempt to chown the path back to the requesting user using the
        # configured binary/command.
        try:
            subprocess.run([self.chown_cmd, str(self.uid), download_path_str])
        except (KeyError, PermissionError) as e:
            self.log("Couldn't change owner of downloaded file", RK.LOG_WARNING)
            self.log(f"Original error: {e}", RK.LOG_DEBUG)

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
    ) -> None:
        # get the target
        target_path = self._get_target_path(body_json)
        # set the ids for the
        if self.chown_fl:
            self.set_ids(body_json)

        # Create client
        self.client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        rk_complete = ".".join([rk_origin, RK.TRANSFER_GET, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.TRANSFER_GET, RK.FAILED])

        for path_details in filelist:
            # If bucketname inserted into object path (i.e. from catalogue) then
            # extract both
            bucket_name, object_name, failure_reason = (
                self._get_and_check_bucket_name_object_name(path_details)
            )
            if failure_reason:
                path_details.failure_reason = failure_reason
                self.append_and_send(
                    self.failedlist,
                    path_details,
                    routing_key=rk_failed,
                    body_json=body_json,
                    state=State.FAILED,
                )
                continue

            self.log(
                f"Attempting to get file {object_name} from {bucket_name}", RK.LOG_DEBUG
            )

            # get the download path from the path details
            download_path, failure_reason = self._get_download_path(
                path_details, target_path
            )
            if failure_reason:
                path_details.failure_reason = failure_reason
                self.append_and_send(
                    self.failedlist,
                    path_details,
                    routing_key=rk_failed,
                    body_json=body_json,
                    state=State.FAILED,
                )
                continue

            # do the download
            failure_reason = self._transfer(bucket_name, object_name, download_path)
            if failure_reason:
                path_details.failure_reason = failure_reason
                self.append_and_send(
                    self.failedlist,
                    path_details,
                    routing_key=rk_failed,
                    body_json=body_json,
                    state=State.FAILED,
                )
                continue

            # change ownership and permissions
            self._change_permissions(download_path)

            # all finished successfully!
            self.log(f"Successfully got {path_details.original_path}", RK.LOG_DEBUG)
            self.append_and_send(
                self.completelist,
                path_details,
                routing_key=rk_complete,
                body_json=body_json,
                state=State.TRANSFER_GETTING,
            )

        # Send whatever remains after all items have been (attempted to be) put
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body_json,
                state=State.TRANSFER_GETTING,
            )
        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body_json,
                state=State.FAILED,
            )


def main():
    consumer = GetTransferConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
