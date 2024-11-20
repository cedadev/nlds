# encoding: utf-8
"""
archive_get.py
NOTE: This module is imported into a revision, and so should be very defensive 
with how it imports external modules (like xrootd). 
"""
__author__ = "Jack Leland and Neil Massey"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import List, Dict, Any
from copy import copy
from minio.error import S3Error
from retry import retry

from nlds_processors.archive.archive_base import BaseArchiveConsumer

from nlds_processors.archive.s3_to_tarfile_stream import S3StreamError

from nlds.rabbit.consumer import State
from nlds.details import PathDetails
from nlds_processors.catalog.catalog_worker import build_retrieval_dict
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


class GetArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_get_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.ARCHIVE_GET}." f"{RK.WILD}"
    DEFAULT_STATE = State.ARCHIVE_GETTING
    PREPARE_DELAY = 60 * 1000 # 60 seconds delay between PREPARE_check requests

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        self.preparelist = []
        super().__init__(queue=queue)

    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        tape_url: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, Any],
    ) -> None:
        # Make the routing keys
        rk_complete = ".".join([rk_origin, RK.ARCHIVE_GET, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.ARCHIVE_GET, RK.FAILED])

        # create the S3 to tape or disk streamer
        try:
            streamer = self._create_streamer(
                tenancy=tenancy,
                access_key=access_key,
                secret_key=secret_key,
                tape_url=tape_url,
            )
        except S3StreamError as e:
            self.log(f"Could not create streamer. Reason: {e}.", RK.LOG_ERROR)
            # if a S3StreamError occurs then all files have failed
            for path_details in filelist:
                path_details.failure_reason = e.message
                self.failedlist.append(path_details)
        else:
            # For archive_get, we build a retrieval dictionary from the filelist,
            # which contains a tarfile as a key, then a filelist as items per key
            retrieval_dict = build_retrieval_dict(filelist)
            # looping over the aggregates
            for tarfile, item in retrieval_dict.items():
                # get the holding id and build the holding_prefix
                holding_id = item[MSG.HOLDING_ID]
                holding_prefix = self.get_holding_prefix(
                    body_json, holding_id=holding_id
                )
                # get the list of files to retrieve from the tarfile / aggregate
                aggregate_filelist = item[MSG.FILELIST]
                # empty streamer.filelist for new aggregate
                streamer.filelist.clear()
                try:
                    # stream the tarfile from tape (cache) to the object store
                    completelist, failedlist = streamer.get(
                        holding_prefix, tarfile, aggregate_filelist, self.chunk_size
                    )
                    # dispatch any completed tarfiles to the next stage
                    for path_details in completelist:
                        self.append_and_send(
                            self.completelist,
                            path_details,
                            routing_key=rk_complete,
                            body_json=body_json,
                            state=State.ARCHIVE_GETTING,
                        )
                    # dispatch any failed tarfiles
                    for path_details in failedlist:
                        self.append_and_send(
                            self.failedlist,
                            path_details,
                            routing_key=rk_failed,
                            body_json=body_json,
                            state=State.FAILED,
                        )
                except S3StreamError as e:
                    # if a S3StreamError occurs then all files in the aggregate have
                    # failed
                    self.log(
                        f"Error when streaming file {tarfile}. Reason: {e.message}",
                        RK.LOG_ERROR,
                    )
                    # add failure message and dispatch
                    for path_details in aggregate_filelist:
                        path_details.failure_reason = e.message
                        self.append_and_send(
                            self.failedlist,
                            path_details,
                            routing_key=rk_failed,
                            body_json=body_json,
                            state=State.FAILED,
                        )
            # try to evict the retrieved files from any cache the storage system has
            try:
                # retrieval_dict.keys() is the list of tarfiles
                streamer.evict(retrieval_dict.keys())
            except S3StreamError as e:
                # just log the error message as a warning - failure to evict shouldn't
                # be too critical
                self.log(e.message, RK.LOG_WARNING)

        if len(self.completelist) > 0:
            # Send whatever remains after all items have been got
            self.log(
                "Archive get complete, passing lists back to worker for transfer.",
                RK.LOG_INFO,
            )
            self.send_pathlist(
                self.completelist,
                routing_key=rk_complete,
                body_json=body_json,
                state=State.ARCHIVE_GETTING,
            )

        if len(self.failedlist) > 0:
            # Send message back to worker so catalog can be scrubbed of failed gets
            self.send_pathlist(
                self.failedlist,
                rk_failed,
                body_json,
                state=State.FAILED,
            )

    @retry(S3Error, tries=5, delay=1, logger=None)
    def prepare(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        tape_url: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, Any],
    ) -> None:
        """Use the streamer object to prepare files for staging, if it is required
        1. Those that do need staging will get a prepare_id and passed back to the
           message queue with ARCHIVE_GET.PREPARE_CHECK as the routing key.
           They will be checked for completed staging when this message is processed in
           the `prepare_check` function below.
        2. Those that do not need staging will be passed to the message queue with
           ARCHIVE_GET.START and will be processed by the `transfer` function above.
        """
        # Make the routing keys
        rk_complete = ".".join([rk_origin, RK.ARCHIVE_GET, RK.START])
        rk_check = ".".join([rk_origin, RK.ARCHIVE_GET, RK.PREPARE_CHECK])
        rk_failed = ".".join([rk_origin, RK.ARCHIVE_GET, RK.FAILED])

        # clear lists
        self.failedlist.clear()
        self.completelist.clear()
        self.preparelist.clear()

        # create the S3 to tape or disk streamer
        try:
            streamer = self._create_streamer(
                tenancy=tenancy,
                access_key=access_key,
                secret_key=secret_key,
                tape_url=tape_url,
            )
        except S3StreamError as e:
            # if a S3StreamError occurs then all files have failed
            self.log(f"Could not create streamer. Reason: {e}.", RK.LOG_ERROR)
            for path_details in filelist:
                path_details.failure_reason = e.message
                self.failedlist.append(path_details)
        else:
            # For archive_prepare, the message is structured as a dictionary stored in
            retrieval_dict = build_retrieval_dict(filelist)
            for tarfile, item in retrieval_dict.items():
                # get the list of files to retrieve from the tarfile / aggregate
                # this will be used for the completelist, the prepare_check list
                # or the failedlist. Convert to PathDetails object
                aggregate_filelist = item[MSG.FILELIST]
                try:
                    # check for prepare on this tarfile
                    if streamer.prepare_required(tarfile):
                        self.preparelist.extend(aggregate_filelist)
                    else:
                        self.completelist.extend(aggregate_filelist)
                except S3StreamError as e:
                    self.log(
                        f"Error preparing file {tarfile}. Reason: {e}.", RK.LOG_ERROR
                    )
                    for path_details in aggregate_filelist:
                        path_details.failure_reason = e.message
                        self.failedlist.append(path_details)

        if len(self.completelist) > 0:
            self.log(
                "Archive prepare not required, passing lists back to archive_get for "
                "transfer.",
                RK.LOG_INFO,
            )
            self.send_pathlist(
                self.completelist,
                rk_complete,
                body_json,
                state=State.ARCHIVE_PREPARING,
            )

        if len(self.preparelist) > 0:
            # In this codepath we have a list of tarfiles we need to prepare in the
            # prepare_dict keys
            try:
                prepare_dict = build_retrieval_dict(self.preparelist)
                agg_prepare_list = list(prepare_dict.keys())
                prepare_id = streamer.prepare_request(agg_prepare_list)
            except S3StreamError as e:
                # fail all in the prepare dict if the prepare_id failed
                self.log(f"Error preparing request. Reason: {e}.", RK.LOG_ERROR)
                for tarfile, item in prepare_dict.items():
                    aggregate_filelist = item[MSG.FILELIST]
                    for path_details in aggregate_filelist:
                        path_details.failure_reason = e.message
                        self.failedlist.append(path_details)
            else:
                self.log(
                    "Archive prepare required, passing lists back to archive_get for "
                    "checking prepare is complete.",
                    RK.LOG_INFO,
                )
                # put the prepare_id in the dictionary
                body_json_check = copy(body_json)
                body_json_check[MSG.DATA][MSG.PREPARE_ID] = str(prepare_id)
                self.send_pathlist(
                    self.preparelist,
                    routing_key=rk_check,
                    body_json=body_json_check,
                    state=State.ARCHIVE_PREPARING,
                    delay=GetArchiveConsumer.PREPARE_DELAY
                )

        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body_json,
                state=State.FAILED,
            )

    @retry(S3Error, tries=5, delay=1, logger=None)
    def prepare_check(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
        tape_url: str,
        filelist: List[PathDetails],
        rk_origin: str,
        body_json: Dict[str, Any],
    ) -> None:
        """Use the streamer object to check whether the prepared files have completed
           staging.
        1. Those that have not completed staging will be passed back to the message
           queue with ARCHIVE_GET.PREPARE_CHECK as the routing key.
           They will be checked again for completed staging when this message is
           processed subsequently in this function.
        2. Those that have completed will be passed to the message queue with
           ARCHIVE_GET.START and will be subsequently processed by the `transfer`
           function above.
        """
        # Make the routing keys
        rk_complete = ".".join([rk_origin, RK.ARCHIVE_GET, RK.START])
        rk_check = ".".join([rk_origin, RK.ARCHIVE_GET, RK.PREPARE_CHECK])
        rk_failed = ".".join([rk_origin, RK.ARCHIVE_GET, RK.FAILED])

        # clear lists
        self.failedlist.clear()
        self.completelist.clear()
        self.preparelist.clear()

        # create the S3 to tape or disk streamer
        try:
            streamer = self._create_streamer(
                tenancy=tenancy,
                access_key=access_key,
                secret_key=secret_key,
                tape_url=tape_url,
            )
        except S3StreamError as e:
            # if a S3StreamError occurs then all files have failed
            self.log(f"Could not create streamer. Reason: {e}.", RK.LOG_ERROR)
            for path_details in filelist:
                path_details.failure_reason = e.message
                self.failedlist.append(path_details)
        else:
            retrieval_dict = build_retrieval_dict(filelist)
            prepare_id = body_json[MSG.DATA][MSG.PREPARE_ID]
            # need to convert the retrieval_dict keys to a list of tarfiles
            tarfile_list = list(retrieval_dict.keys())
            try:
                complete = streamer.prepare_complete(prepare_id, tarfile_list)
            except S3StreamError as e:
                self.log(
                    f"Could not check prepare id: {prepare_id}. Reason: {e}.",
                    RK.LOG_ERROR,
                )
                # fail all in the prepare dict if the prepare_id failed
                for _, item in retrieval_dict.items():
                    aggregate_filelist = item[MSG.FILELIST]
                    for path_details in aggregate_filelist:
                        path_details.failure_reason = e.message
                        self.failedlist.append(path_details)

        # only three outcomes here - 1. either all the tarfiles (and, by extension, all
        # files) are complete, 2. are not complete, or 3. everything failed
        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist,
                routing_key=rk_failed,
                body_json=body_json,
                state=State.FAILED,
            )
        else:
            if complete:
                self.log(
                    "Archive prepare complete, passing lists back to archive_get for "
                    "transfer.",
                    RK.LOG_INFO,
                )
                # Split the messages so that a message is sent per tarfile. This
                # increases the parallelisation and allows multiple consumers to perform
                # the streaming from tape cache to object storage
                for _, item in retrieval_dict.items():
                    aggregate_filelist = item[MSG.FILELIST]
                    self.send_pathlist(
                        aggregate_filelist,
                        rk_complete,
                        body_json,
                        state=State.ARCHIVE_PREPARING,
                    )
            else:
                self.log(
                    "Archive prepare not complete, passing lists back to prepare_check "
                    "for additional waiting.",
                    RK.LOG_INFO,
                )
                self.send_pathlist(
                    filelist,  # send the filelist again
                    routing_key=rk_check,
                    body_json=body_json,
                    state=State.ARCHIVE_PREPARING,
                    delay=GetArchiveConsumer.PREPARE_DELAY
                )


def main():
    consumer = GetArchiveConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
