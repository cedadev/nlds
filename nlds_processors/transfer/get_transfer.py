# encoding: utf-8
"""
get_transfer.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import List, Dict, Any
from pathlib import Path
import os
import subprocess

import minio
from minio.error import S3Error
from retry import retry

from nlds_processors.transfer.base_transfer import BaseTransferConsumer
from nlds.rabbit.consumer import State
from nlds.details import PathDetails, PathType
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG
from nlds_processors.transfer.transfer_error import TransferError
from nlds_processors.bucket_mixin import BucketMixin, BucketError


class GetTransferConsumer(BaseTransferConsumer, BucketMixin):
    DEFAULT_QUEUE_NAME = "transfer_get_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.TRANSFER_GET}." f"{RK.WILD}"
    DEFAULT_STATE = State.TRANSFER_GETTING

    _CHOWN_COMMAND = "chown_cmd"
    _CHOWN_FL = "chown_fl"
    _CHOWN_USER = "nlds"
    TRANSFER_GET_CONSUMER_CONFIG = {
        _CHOWN_COMMAND: "chown",
        _CHOWN_FL: False,
        _CHOWN_USER: "nlds",
    }
    DEFAULT_CONSUMER_CONFIG = (
        TRANSFER_GET_CONSUMER_CONFIG | BaseTransferConsumer.DEFAULT_CONSUMER_CONFIG
    )

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.chown_cmd = self.load_config_value(self._CHOWN_COMMAND)
        self.chown_fl = self.load_config_value(self._CHOWN_FL)
        self.chown_user = self.load_config_value(self._CHOWN_USER)
        self.s3_client = None

    def _get_target_path(self, body_json: Dict):
        # Get target and verify it can be written to
        target = body_json[MSG.DETAILS][MSG.TARGET]
        if target:
            target_path = Path(target)
            if not self.check_path_access(target_path, access=os.W_OK):
                msg = f"Unable to copy, target path {target_path} is inaccessible."
                self.log(msg, RK.LOG_ERROR)
                raise TransferError(msg)
        else:
            target_path = None
        return target_path

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
                    f"Unable to download {download_path}. Target path is inaccesible."
                )
                self.log(f"{reason}. Adding to failed list.", RK.LOG_INFO)
                raise TransferError(message=reason)

        elif target_path.is_dir():
            # In the case of a given target, we remove the leading slash on
            # the original path and prepend the target_path
            if path_details.original_path[0] == "/":
                download_path = target_path / path_details.original_path[1:]
            else:
                download_path = target_path / path_details.original_path
        else:
            reason = "Target path is not a valid directory."
            self.log(f"{reason}", RK.LOG_WARNING)
            raise TransferError(message=reason)
        return download_path

    def _get_linked_path(self, path_details, target_path):
        # If there is no target_path then the it is just the link path converted to
        # a path object
        if not target_path:
            linked_path = Path(path_details.link_path)
            if not self.check_path_access(path_details.path.parent, access=os.W_OK):
                reason = f"Unable to access {linked_path}. Linked path is inaccesible."
                raise TransferError(message=reason)
        elif target_path.is_dir():
            # In the case of a given target, we remove the leading slash on
            # the original path and prepend the target_path
            if path_details.link_path[0] == "/":
                download_path = target_path / path_details.link_path[1:]
            else:
                download_path = target_path / path_details.link_path
        else:
            reason = "Linked path is not valid."
            self.log(f"{reason}", RK.LOG_WARNING)
            raise TransferError(message=reason)
        return download_path

    def _transfer(self, bucket_name, object_name, download_path):
        if self.s3_client is None:
            raise RuntimeError("self.s3_client is None")
        download_path_str = str(download_path)
        # Attempt the download!
        try:
            resp = self.s3_client.fget_object(
                bucket_name,
                object_name,
                download_path_str,
            )
        except Exception as e:
            reason = (
                f"Download-time exception occurred: {e}. "
                f"Bucket name: {bucket_name}. "
                f"Object name: {object_name}. "
                f"Download path: {download_path}. "
            )
            raise TransferError(message=reason)

    def _change_permissions(self, download_path, path_details):
        """Change the permission of the downloaded file"""
        download_path_str = str(download_path)
        self.log(
            f"Changing permissions and ownership of file " f"{download_path}",
            RK.LOG_INFO,
        )
        try:
            # Attempt to change back to original permissions
            os.chmod(download_path, mode=path_details.permissions)
        except (KeyError, PermissionError) as e:
            self.log("Couldn't change permissions of downloaded file", RK.LOG_WARNING)
            self.log(f"Original error: {e}", RK.LOG_DEBUG)
        self._change_owner(download_path_str)

    def _change_owner(self, path_str):
        # Attempt to chown the path back to the requesting user using the
        # configured binary/command.
        try:
            cmd = [
                self.chown_cmd,
                str(self.uid),
                str(self.gids[0]),
                path_str,
            ]
            try:
                P = subprocess.run(cmd)
                P.check_returncode()
            except subprocess.CalledProcessError as e:
                raise TransferError(
                    f"Could not change owner of downloaded file: {path_str}"
                )
        except (KeyError, PermissionError) as e:
            self.log("Couldn't change owner of downloaded file", RK.LOG_WARNING)
            self.log(f"Original error: {e}", RK.LOG_DEBUG)

    def _get_parent_dirs(self, download_path, target_path):
        # get all the directories from the download_path up to the target_path
        pardirs = []
        if not target_path:
            term_path = Path("/")
        else:
            term_path = target_path
        pardir = download_path
        while pardir != term_path:
            # only change the directories owned by the chown_user, but do traverse all
            # the way to the top - this is why we build a list of directories to change
            # but we continue all the way up to term_path
            pardir = pardir.parent.absolute()
            if pardir != target_path and pardir.owner() == self.chown_user:
                pardirs.append(pardir)
        return pardirs

    def _transfer_files(
        self,
        tenancy: str,
        filelist: list,
        rk_origin: str,
        body_json: Dict[str, Any],
        access_key: str,
        secret_key: str,
        target_path: str,
    ):
        # build the routing keys
        rk_complete = ".".join([rk_origin, RK.TRANSFER_GET, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.TRANSFER_GET, RK.FAILED])
        # set the ids for the files
        if self.chown_fl:
            try:
                self.set_ids(body_json)
            except KeyError as e:
                msg = "Problem running set_ids in _transfer_files"
                self.log(msg, RK.LOG_ERROR)
                self._fail_all(self.filelist, self.rk_parts, self.body_json, msg)
                return

        # Create client
        self.s3_client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )
        # keep a list of created paths so that we can change all the directories below
        # the download path to have the same ownership
        created_paths = []
        # keep a list of all the links so that they can be recreated later
        file_is_link_paths = []
        for path_details in filelist:
            # Don't transfer symbolic links
            if path_details.path_type == PathType.LINK:
                file_is_link_paths.append(path_details)
                continue
            try:
                # get the bucket and object name and check the bucket exists
                bucket_name, object_name = self._get_bucket_name_object_name(
                    path_details
                )
                if not self._bucket_exists(bucket_name):
                    raise TransferError(
                        f"Bucket {bucket_name} does not exist in get_transfer."
                        f"_transfer_files"
                    )
                self.log(
                    f"Attempting to get file {object_name} from {bucket_name}",
                    RK.LOG_DEBUG,
                )
                download_path = self._get_download_path(path_details, target_path)
                self._transfer(bucket_name, object_name, download_path)
            except (BucketError, TransferError) as e:
                path_details.failure_reason = e.message
                self.log(e.message, RK.LOG_DEBUG)
                self.append_and_send(
                    self.failedlist,
                    path_details,
                    routing_key=rk_failed,
                    body_json=body_json,
                    state=State.FAILED,
                )
            else:
                # change ownership and permissions
                try:
                    self._change_permissions(download_path, path_details)
                except TransferError as e:
                    self.log(f"Error downloading: {e}", RK.LOG_ERROR)
                # all finished successfully!
                self.log(f"Successfully got {path_details.original_path}", RK.LOG_DEBUG)
                self.append_and_send(
                    self.completelist,
                    path_details,
                    routing_key=rk_complete,
                    body_json=body_json,
                    state=State.TRANSFER_GETTING,
                )
                # get the parent directories all the way up to the target_path
                par_dirs = self._get_parent_dirs(download_path, target_path)
                for p in par_dirs:
                    if p not in created_paths:
                        created_paths.append(p)

        # change the permissions on the created paths
        for cp in created_paths:
            try:
                self._change_owner(cp)
            except TransferError as e:
                self.log(f"Error changing directory owner: {e}", RK.LOG_ERROR)

        # add the links for the linked paths
        link_warnings = []
        complete_link_paths = []
        failed_link_paths = []
        for lp in file_is_link_paths:
            try:
                # original_path is the symlink, link_path is the actual path
                symlink = self._get_download_path(lp, target_path)
                actual_path = self._get_linked_path(lp, target_path)
                symlink.symlink_to(actual_path)
                complete_link_paths.append(lp)
            except (
                TransferError,
                FileExistsError,
                PermissionError,
                FileNotFoundError,
            ) as e:
                warning = f"Error creating symlink: {e}"
                link_warnings.append(warning)
                self.log(warning, RK.LOG_WARNING)
                failed_link_paths.append(lp)
        # we need to acknowledge that the links have been created as well,
        # otherwise the job won't complete
        if len(complete_link_paths) > 0:
            self.send_pathlist(
                complete_link_paths,
                routing_key=rk_complete,
                body_json=body_json,
                state=State.TRANSFER_GETTING,
                warning=link_warnings,
            )

        if len(failed_link_paths) > 0:
            self.send_pathlist(
                failed_link_paths,
                routing_key=rk_failed,
                body_json=body_json,
                state=State.FAILED,
                warning=link_warnings
            )

    @retry(S3Error, tries=5, delay=10, backoff=10, logger=None)
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

        # build the routing keys
        rk_complete = ".".join([rk_origin, RK.TRANSFER_GET, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.TRANSFER_GET, RK.FAILED])

        # get the target directory and fail all the transfers if it cannot be created
        try:
            target_path = self._get_target_path(body_json)
        except TransferError as e:
            for path_details in filelist:
                path_details.failure_reason = e.message
                self.append_and_send(
                    self.failedlist,
                    path_details,
                    routing_key=rk_failed,
                    body_json=body_json,
                    state=State.FAILED,
                )
        else:
            self._transfer_files(
                tenancy=tenancy,
                filelist=filelist,
                rk_origin=rk_origin,
                body_json=body_json,
                access_key=access_key,
                secret_key=secret_key,
                target_path=target_path,
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
