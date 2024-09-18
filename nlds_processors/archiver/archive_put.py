# encoding: utf-8
"""
archive_put.py
NOTE: This module is imported into a revision, and so should be very defensive 
with how it imports external modules (like xrootd). 
"""
__author__ = "Jack Leland and Neil Massey"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import List, Dict, Any
import os

from nlds_processors.archiver.archive_base import (
    BaseArchiveConsumer,
    ArchiveError,
)

USE_DISKTAPE = True
if USE_DISKTAPE:
    from nlds_processors.archiver.s3_to_tarfile_disk import S3ToTarfileDisk
else:
    from nlds_processors.archiver.s3_to_tarfile_tape import S3ToTarfileTape
from nlds_processors.archiver.s3_to_tarfile_stream import S3StreamError

from nlds.rabbit.consumer import State
from nlds.details import PathDetails
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


class PutArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_put_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.TRANSFER_PUT}." f"{RK.WILD}"
    DEFAULT_STATE = State.ARCHIVE_PUTTING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

    def transfer(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        tape_url: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, str],
    ):
        # Make the routing keys now
        rk_complete = ".".join([rk_origin, RK.ARCHIVE_PUT, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.ARCHIVE_PUT, RK.FAILED])

        # Create the S3 to tape or disk streamer
        try:
            if USE_DISKTAPE:
                disk_loc = os.path.expanduser("~/DISKTAPE")
                self.log(
                    f"Starting disk transfer between {disk_loc} and object store " 
                    f"{tenancy}",
                    RK.LOG_INFO,
                )
                streamer = S3ToTarfileDisk(
                    s3_tenancy=tenancy,
                    s3_access_key=access_key,
                    s3_secret_key=secret_key,
                    disk_location=disk_loc,
                    logger=self.log,
                )
            else:
                self.log(
                    f"Starting tape transfer between {tape_url} and object store " 
                    f"{tenancy}",
                    RK.LOG_INFO,
                )
                streamer = S3ToTarfileTape(
                    s3_tenancy=tenancy,
                    s3_access_key=access_key,
                    s3_secret_key=secret_key,
                    tape_url=tape_url,
                    logger=self.log,
                )
        except S3StreamError as e:
            # if a S3StreamError occurs then all files have failed
            for path_details in filelist:
                path_details.failure_reason = e.message
                self.failedlist.append(path_details)
            checksum = None
        else:
            # NOTE: For the purposes of tape reading and writing, the holding prefix
            # has 'nlds.' prepended
            holding_prefix = self.get_holding_prefix(body_json)

            try:
                self.completelist, self.failedlist, checksum = streamer.put(
                    holding_prefix, filelist
                )
            except S3StreamError as e:
                # if a S3StreamError occurs then all files have failed
                for path_details in filelist:
                    path_details.failure_reason = e.message
                    self.failedlist.append(path_details)
                checksum = None
            # assign the return data
            body_json[MSG.DATA][MSG.CHECKSUM] = checksum

        # Send whatever remains after all items have been put
        if len(self.completelist) > 0:
            self.log(
                "Archive complete, passing lists back to worker for re-routing and "
                "cataloguing.",
                RK.LOG_INFO,
            )
            self.send_pathlist(
                self.completelist, rk_complete, body_json, state=State.ARCHIVE_PUTTING
            )

        if len(self.failedlist) > 0:
            # Send message back to worker so catalog can be scrubbed of failed puts
            self.send_pathlist(
                self.failedlist,
                rk_failed,
                body_json,
                state=State.FAILED,
            )

    @classmethod
    def get_holding_prefix(cls, body: Dict[str, Any]) -> str:
        """Get the uneditable holding information from the message body to
        reproduce the holding prefix made in the catalog"""
        try:
            holding_id = body[MSG.META][MSG.HOLDING_ID]
            user = body[MSG.DETAILS][MSG.USER]
            group = body[MSG.DETAILS][MSG.GROUP]
        except KeyError as e:
            raise ArchiveError(
                f"Could not make holding prefix, original error: " f"{e}"
            )

        return f"nlds.{holding_id}.{user}.{group}"


def main():
    consumer = PutArchiveConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
