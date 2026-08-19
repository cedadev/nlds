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

from nlds_processors.archive.archive_base import BaseArchiveConsumer, ArchiveError

from nlds_processors.archive.s3_to_tarfile_stream import S3StreamError

from nlds.rabbit.consumer import State
from nlds.details import PathDetails
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG
from nlds_processors.archive.s3_to_tarfile_stream import S3ToTarfileStream


def build_retrieval_dict(filelist: list[PathDetails], fullpath: bool = False):
    """Build a retrieval dict from the filelist.  The retrieval dict contains a
    tarfile name, a holding id, and the list of files to retrieve from the tarfile.
    """
    retrieval_dict = {}
    for file in filelist:
        # get the tape url - i.e. where the server is located
        tape_url = file.tape_url
        # start a new dictionary with this tape_url if it does not already exist
        if not tape_url in retrieval_dict:
            retrieval_dict[tape_url] = {}
        # get the tape location for File or FileSystem
        tarfile = file.tape_name
        # if it has not been added before then create a new record
        if not tarfile in retrieval_dict[tape_url]:
            tape_loc = file.get_tape()
            retrieval_dict[tape_url][tarfile] = {
                "holding_id": file.holding_id,
                "checksum": tape_loc.checksum,
                "checksum_method": tape_loc.checksum_method,
                "filelist": [file],
            }
        else:
            # if it has been added before then append to the filelist
            retrieval_dict[tape_url][tarfile]["filelist"].append(file)
    return retrieval_dict


class GetArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_get_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.ARCHIVE_GET}." f"{RK.WILD}"
    DEFAULT_STATE = State.ARCHIVE_GETTING
    PREPARE_DELAY = 60  # 60 seconds delay between PREPARE_check requests

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        self.preparelist = []
        super().__init__(queue=queue)

    @retry((S3Error, S3StreamError), tries=5, delay=10, backoff=10, logger=None)
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
        # Make the routing keys
        rk_complete = ".".join([rk_origin, RK.ARCHIVE_GET, RK.COMPLETE])
        rk_failed = ".".join([rk_origin, RK.ARCHIVE_GET, RK.FAILED])

        # For archive_get, we build a retrieval dictionary from the filelist,
        # which contains a tape_url as a key to a further dictionary:
        #   which contains a tarfile as a key, then a filelist as items per key
        retrieval_dict = build_retrieval_dict(filelist)
        # looping over the tape_urls
        for tape_url, aggregates in retrieval_dict.items():
            # clear lists - per tape_url
            self.failedlist.clear()
            self.completelist.clear()
            # create the stream to the tape_url (either tape or disktape handled in
            # _create_streamer function)
            try:
                streamer = self._create_streamer(
                    tenancy=tenancy,
                    access_key=access_key,
                    secret_key=secret_key,
                    tape_url=tape_url,
                )
            except (S3StreamError, ArchiveError) as e:
                # if a S3StreamError occurs then all files have failed
                self.log(f"Could not create streamer. Reason: {e}.", RK.LOG_ERROR)
                for path_details in filelist:
                    path_details.failure_reason = e.message
                    self.failedlist.append(path_details)

            # looping over the aggregates
            for tarfile, item in aggregates.items():
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
                        holding_prefix,
                        tarfile,
                        aggregate_filelist,
                        self.chunk_size,
                        self.num_parallel_uploads,
                        checksum_method=item["checksum_method"],
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
                except (S3StreamError, ArchiveError) as e:
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
            except (S3StreamError, ArchiveError) as e:
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

    def _build_complete_prepare_lists(
        self,
        tarfile: str,
        aggregate_details: dict,
        streamer: S3ToTarfileStream,
    ):
        # get the list of files to retrieve from the tarfile / aggregate
        # this will be used for the completelist, the prepare_check list
        # or the failedlist. Convert to PathDetails object
        aggregate_filelist = aggregate_details[MSG.FILELIST]
        try:
            # check for prepare on this tarfile
            if streamer.prepare_required(tarfile):
                self.preparelist.extend(aggregate_filelist)
            else:
                self.completelist.extend(aggregate_filelist)
        except (S3StreamError, ArchiveError) as e:
            self.log(f"Error preparing file {tarfile}. Reason: {e}.", RK.LOG_ERROR)
            for path_details in aggregate_filelist:
                path_details.failure_reason = e.message
                self.failedlist.append(path_details)

    def _request_prepare_list(
        self,
        tarfiles_to_prepare: list[str],
        body_json: Dict[str, Any],
        streamer: S3ToTarfileStream,
    ):
        # send the list of tarfiles to prepare_request
        # making multiple requests is the fastest way to do it on tape
        try:
            prepare_id = streamer.prepare_request(tarfiles_to_prepare)
        except (S3StreamError, ArchiveError) as e:
            # fail all in the prepare dict if the prepare_id failed
            self.log(f"Error preparing request. Reason: {e}.", RK.LOG_ERROR)
            for _, tarfile_details in tarfiles_to_prepare.items():
                prepare_filelist = tarfile_details[MSG.FILELIST]
                for path_details in prepare_filelist:
                    path_details.failure_reason = e.message
                    self.failedlist.append(path_details)
        else:
            self.log(
                "Archive prepare required, passing lists back to archive_get for "
                "checking prepare is complete.",
                RK.LOG_INFO,
            )
            # put the prepare_id in the dictionary, copy first
            body_json_check = copy(body_json)
            body_json_check[MSG.DATA][MSG.PREPARE_ID] = str(prepare_id)
        return body_json_check

    @retry((S3Error, S3StreamError), tries=5, delay=10, backoff=10, logger=None)
    def prepare(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
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

        # build_retrieval_dict takes a list of files (in PathDetails form) and extracts
        # the tape_url and tarfile information for each file, and then rearranges the
        # dictionary so it has the form of [tape_url][tarfile]:[files]
        # this ensures that the tape_url and tarfile only exist once in the retrieval
        # process, and only one streamer per tape_url is created
        retrieval_dict = build_retrieval_dict(filelist)
        # looping over the tape_urls
        for tape_url, tape_details in retrieval_dict.items():
            # clear lists - per url
            self.failedlist.clear()
            self.completelist.clear()
            self.preparelist.clear()
            # create the stream to the tape_url (either tape or disktape handled in
            # _create_streamer function)
            try:
                streamer = self._create_streamer(
                    tenancy=tenancy,
                    access_key=access_key,
                    secret_key=secret_key,
                    tape_url=tape_url,
                )
            except (S3StreamError, ArchiveError) as e:
                # if a S3StreamError occurs then all files have failed
                self.log(f"Could not create streamer. Reason: {e}.", RK.LOG_ERROR)
                for path_details in filelist:
                    path_details.failure_reason = e.message
                    self.failedlist.append(path_details)

            # get the list of tarfiles and their aggregate details
            for tarfile_name, tarfile_details in tape_details.items():
                # this function assigns the
                # 1. Files that are already on the tape cache to self.completelist
                # 2. Files that need fetching from tape to the cache to self.preparelist
                # 3. Any failures to ascertain the status or successfully prepare a file
                #    to self.failedlist
                # These lists are built and processed per tape_url
                self._build_complete_prepare_lists(
                    tarfile_name, tarfile_details, streamer
                )

            if len(self.completelist) > 0:
                self.log(
                    "Archive prepare not required, passing lists back to archive_get "
                    "for transfer.",
                    RK.LOG_INFO,
                )
                self.send_pathlist(
                    self.completelist,
                    rk_complete,
                    body_json,
                    state=State.ARCHIVE_PREPARING,
                )

            if len(self.preparelist) > 0:
                # We now have a list of tarfiles we need to prepare in self.preparelist
                # This needs to be converted to the dictionary containing the tape_url
                # and tarfile for each file - we use build_retrieval_dict again, but
                # only for those files that need preparing
                prepare_dict = build_retrieval_dict(self.preparelist)
                # we can just index the prepare_dict by the tape_url, as this should be
                # the only key anyway
                prepare_details = prepare_dict[tape_url]
                # the tarfile names we want to prepare are supplied as a list, so they
                # are just the keys of the prepare_details
                tarfiles_to_prepare = list(prepare_details.keys())
                # do the request
                body_json_check = self._request_prepare_list(
                    tarfiles_to_prepare=tarfiles_to_prepare,
                    body_json=body_json,
                    streamer=streamer,
                )
                # send the result of the prepare for each file in the prepare list
                self.send_pathlist(
                    self.preparelist,
                    routing_key=rk_check,
                    body_json=body_json_check,
                    state=State.ARCHIVE_PREPARING,
                    delay=GetArchiveConsumer.PREPARE_DELAY,
                )

            # send any failed files
            if len(self.failedlist) > 0:
                self.send_pathlist(
                    self.failedlist,
                    routing_key=rk_failed,
                    body_json=body_json,
                    state=State.FAILED,
                )

    @retry((S3Error, S3StreamError), tries=5, delay=10, backoff=10, logger=None)
    def prepare_check(
        self,
        transaction_id: str,
        tenancy: str,
        access_key: str,
        secret_key: str,
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

        # clear lists - there is one message per tape URL, so we don't need to
        # loop over the tape urls when clearing the lists and examining the PREPARE_ID
        self.failedlist.clear()
        self.completelist.clear()
        self.preparelist.clear()
        prepare_id = body_json[MSG.DATA][MSG.PREPARE_ID]

        retrieval_dict = build_retrieval_dict(filelist)

        # looping over the tape_urls
        for tape_url, tape_details in retrieval_dict.items():
            # create the stream to the tape_url (either tape or disktape handled in
            # _create_streamer function)
            try:
                streamer = self._create_streamer(
                    tenancy=tenancy,
                    access_key=access_key,
                    secret_key=secret_key,
                    tape_url=tape_url,
                )
            except (S3StreamError, ArchiveError) as e:
                # if a S3StreamError occurs then all files have failed
                self.log(f"Could not create streamer. Reason: {e}.", RK.LOG_ERROR)
                for path_details in filelist:
                    path_details.failure_reason = e.message
                    self.failedlist.append(path_details)

            # need to convert the retrieval_dict keys to a list of tarfiles
            # tape_details is a dictionary with keys that are the paths of tarfiles
            tarfile_list = list(tape_details.keys())
            try:
                complete = streamer.prepare_complete(prepare_id, tarfile_list)
            except (S3StreamError, ArchiveError) as e:
                self.log(
                    f"Could not check prepare id: {prepare_id}. Reason: {e}.",
                    RK.LOG_ERROR,
                )
                # fail all in the prepare dict if the prepare_id failed
                for _, tarfile_details in retrieval_dict.items():
                    failed_filelist = tarfile_details[MSG.FILELIST]
                    for path_details in failed_filelist:
                        path_details.failure_reason = e.message
                        self.failedlist.append(path_details)

            # only three outcomes here -
            # 1. either all the tarfiles (and, by extension, all files) are complete
            # 2. are not complete, or
            # 3. everything failed
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
                        "Archive prepare complete, passing lists back to archive_get "
                        "for transfer.",
                        RK.LOG_INFO,
                    )
                    # Split the messages so that a message is sent per tarfile. This
                    # increases the parallelisation and allows multiple consumers to
                    # perform the streaming from tape cache to object storage
                    for _, tarfile_details in tape_details.items():
                        aggregate_filelist = tarfile_details[MSG.FILELIST]
                        self.send_pathlist(
                            aggregate_filelist,
                            rk_complete,
                            body_json,
                            state=State.ARCHIVE_PREPARING,
                        )
                else:
                    self.log(
                        "Archive prepare not complete, passing lists back to "
                        "prepare_check for additional waiting.",
                        RK.LOG_INFO,
                    )
                    self.send_pathlist(
                        filelist,  # send the filelist again
                        routing_key=rk_check,
                        body_json=body_json,
                        state=State.ARCHIVE_PREPARING,
                        delay=GetArchiveConsumer.PREPARE_DELAY,
                    )

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
    consumer = GetArchiveConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
