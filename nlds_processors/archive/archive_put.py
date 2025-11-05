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
from minio.error import S3Error
from retry import retry

from nlds_processors.archive.archive_base import BaseArchiveConsumer

from nlds_processors.archive.s3_to_tarfile_stream import S3StreamError

from nlds.rabbit.consumer import State
from nlds.details import PathDetails
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


class PutArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_put_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.ARCHIVE_PUT}." f"{RK.WILD}"
    DEFAULT_STATE = State.ARCHIVE_PUTTING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

    @retry((S3Error, S3StreamError), tries=5, delay=10, backoff=10, logger=None)
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
            streamer = self._create_streamer(
                tenancy=tenancy,
                access_key=access_key,
                secret_key=secret_key,
                tape_url=tape_url,
            )
        except S3StreamError as e:
            # if a S3StreamError occurs then all files have failed
            for path_details in filelist:
                path_details.failure_reason = e.message
                self.failedlist.append(path_details)
            checksum = None
            tarfile = None
        else:
            # NOTE: For the purposes of tape reading and writing, the holding prefix
            # has 'nlds.' prepended
            holding_prefix = self.get_holding_prefix(body_json)
            try:
                self.completelist, self.failedlist, tarfile, checksum = streamer.put(
                    holding_prefix, filelist, self.chunk_size
                )
            except S3StreamError as e:
                # if a S3StreamError occurs then all files have failed
                for path_details in filelist:
                    path_details.failure_reason = e.message
                    self.failedlist.append(path_details)
                checksum = None
                tarfile = None
        # assign the return data for the aggregation.  Need to know the tarfile name
        # and its checksum
        body_json[MSG.DATA][MSG.CHECKSUM] = checksum
        body_json[MSG.DATA][MSG.TARFILE] = tarfile

        # Send whatever remains after all items have been put
        if len(self.completelist) > 0:
            self.log(
                "Archive put complete, passing lists back to worker for cataloguing.",
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

    def prepare(
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
        """Put shouldn't have a prepare method"""
        raise NotImplementedError

    def prepare_check(
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
        """Put shouldn't have a prepare check method"""
        raise NotImplementedError
    
    def setup(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, str],
    ):
        raise NotImplementedError

def main():
    consumer = PutArchiveConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
