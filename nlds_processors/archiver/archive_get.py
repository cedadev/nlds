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

from nlds_processors.archiver.archive_base import BaseArchiveConsumer

from nlds_processors.archiver.s3_to_tarfile_stream import S3StreamError

from nlds.rabbit.consumer import State
from nlds.details import PathDetails
from nlds_processors.catalog.catalog_worker import build_retrieval_dict
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


class GetArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_get_q"
    DEFAULT_ROUTING_KEY = f"{RK.ROOT}." f"{RK.ARCHIVE_GET}." f"{RK.WILD}"
    DEFAULT_STATE = State.ARCHIVE_GETTING

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
                    completelist, failedlist = streamer.get(
                        holding_prefix, tarfile, aggregate_filelist, self.chunk_size
                    )
                    for path_details in completelist:
                        self.append_and_send(
                            self.completelist,
                            path_details,
                            routing_key=rk_complete,
                            body_json=body_json,
                            state=State.ARCHIVE_GETTING,
                        )
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
                        RK.LOG_ERROR
                    )
                    for path_details in aggregate_filelist:
                        path_details.failure_reason = e.message
                        self.append_and_send(
                            self.failedlist,
                            path_details,
                            routing_key=rk_failed,
                            body_json=body_json,
                            state=State.FAILED,
                        )

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
                    RK.LOG_ERROR
                )
                # fail all in the prepare dict if the prepare_id failed
                for tarfile, item in retrieval_dict:
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
                self.send_pathlist(
                    self.filelist,
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
                    self.preparelist,
                    routing_key=rk_check,
                    body_json=body_json,
                    state=State.ARCHIVE_PREPARING,
                )

    # def transfer_old(
    #     self,
    #     transaction_id: str,
    #     tenancy: str,
    #     access_key: str,
    #     secret_key: str,
    #     tape_url: str,
    #     filelist: List[PathDetails],
    #     rk_origin: str,
    #     body_json: Dict[str, Any],
    # ):
    #     # Make the routing keys
    #     rk_complete = ".".join([rk_origin, RK.ARCHIVE_GET, RK.COMPLETE])
    #     rk_failed = ".".join([rk_origin, RK.ARCHIVE_GET, RK.FAILED])

    #     # Can call this as the url has been verified previously
    #     tape_server, tape_base_dir = self.split_tape_url(tape_url)

    #     try:
    #         raw_rd = dict(body_json[MSG.DATA][MSG.RETRIEVAL_FILELIST])
    #         retrieval_dict = {
    #             tarname: [PathDetails.from_dict(pd_dict) for pd_dict in pd_dicts]
    #             for tarname, pd_dicts in raw_rd.items()
    #         }
    #     except TypeError as e:
    #         self.log(
    #             "Failed to reformat retrieval filelist into PathDetails "
    #             "objects. Retrievallist in message does not appear to be "
    #             "in the correct format.",
    #             RK.LOG_ERROR,
    #         )
    #         raise e

    #     # Declare useful variables
    #     bucket_name = None
    #     rk_complete = ".".join([rk_origin, RK.ARCHIVE_GET, RK.COMPLETE])
    #     rk_retry = ".".join([rk_origin, RK.ARCHIVE_GET, RK.START])
    #     rk_failed = ".".join([rk_origin, RK.ARCHIVE_GET, RK.FAILED])

    #     try:
    #         prepare_id = body_json[MSG.DATA][MSG.PREPARE_ID]
    #     except KeyError:
    #         self.log(
    #             "Could not get prepare_id from message info, continuing " "without",
    #             RK.LOG_INFO,
    #         )
    #         prepare_id = None

    #     # Create minio client
    #     s3_client = Minio(
    #         tenancy,
    #         access_key=access_key,
    #         secret_key=secret_key,
    #         secure=self.require_secure_fl,
    #     )

    #     # Create the FileSystem client at this point to verify the tape_base_dir
    #     fs_client = XRDClient.FileSystem(f"root://{tape_server}")
    #     # Attempt to verify that the base-directory exists
    #     self.verify_tape_server(fs_client, tape_server, tape_base_dir)

    #     # Ensure minimum part_size is met for put_object to function
    #     chunk_size = max(5 * 1024 * 1024, self.chunk_size)

    #     #################
    #     # Pre-read loop:
    #     # Verify the filelist contents and create the necessary lists for the
    #     # tape preparation request

    #     # TODO: refactor this?
    #     tar_list = []
    #     tar_originals_map = {}
    #     for path_details in filelist:
    #         # First check whether path_details has failed too many times
    #         if path_details.retries.count > self.max_retries:
    #             self.failedlist.append(path_details)
    #             # TODO: do these actually get skipped?
    #             continue

    #         try:
    #             holding_prefix, filelist_hash = self.parse_tape_details(path_details)
    #         except ArchiveError as e:
    #             self.process_retry(str(e), path_details)
    #             continue

    #         tar_filename = f"{filelist_hash}.tar"
    #         try:
    #             # Check that filelist hash is in the full retrieval list
    #             assert tar_filename in retrieval_dict
    #         except AssertionError as e:
    #             reason = f"Tar file name not found in full retrieval list"
    #             self.process_retry(reason, path_details)
    #             continue

    #         holding_tape_path = (
    #             f"root://{tape_server}/{tape_base_dir}/" f"{holding_prefix}"
    #         )
    #         full_tape_path = f"{holding_tape_path}/{tar_filename}"

    #         # Check bucket folder exists on tape
    #         status, _ = fs_client.dirlist(
    #             f"{tape_base_dir}/{holding_prefix}", DirListFlags.STAT
    #         )
    #         if status.status != 0:
    #             # If bucket tape-folder can't be found then pass for retry
    #             reason = (
    #                 f"Tape holding folder ({tape_base_dir}/{holding_prefix}) "
    #                 f"could not be found, cannot retrieve from archive"
    #             )
    #             self.process_retry(reason, path_details)
    #             continue

    #         # The tar_filenames must be encoded into a list of byte strings for
    #         # prepare to work, as of pyxrootd v5.5.3. We group them together to
    #         # ensure only a single transaction is passed to the tape server.
    #         prepare_item = f"{tape_base_dir}/{holding_prefix}/{tar_filename}"
    #         tar_list.append(prepare_item)

    #         # Create the tar_originals_map, which maps from tar file to the
    #         # path_details in the original filelist (i.e. the files in the
    #         # original request) for easy retry/failure if something goes wrong.
    #         if tar_filename not in tar_originals_map:
    #             tar_originals_map[tar_filename] = [
    #                 path_details,
    #             ]
    #         else:
    #             tar_originals_map[tar_filename].append(path_details)

    #     # If tar_list is empty at this point everything is either in need of
    #     # retrying or failing so we can just start a TLR with an exception
    #     if len(tar_list) == 0:
    #         raise CallbackError(
    #             "All paths failed verification, nothing to " "prepare or get."
    #         )

    #     ###########
    #     # Prepare:
    #     # Make a preparation request and/or check on the status of an existing
    #     # preparation request

    #     # Deduplicate the list of tar files
    #     tar_list = list(set(tar_list))
    #     # Convert to bytes (if necessary)
    #     prepare_list = self.prepare_preparelist(tar_list)

    #     if prepare_id is None:
    #         # Prepare all the tar files at once. This can take a while so requeue
    #         # the message afterwards to free up the consumer and periodicaly check
    #         # the status of the prepare command.
    #         # TODO: Utilise the prepare result for eviction later?
    #         status, prepare_result = fs_client.prepare(prepare_list, PrepareFlags.STAGE)
    #         if status.status != 0:
    #             # If tarfiles can't be staged then pass for transaction-level
    #             # retry. TODO: Probably need to figure out which files have failed
    #             # this step as it won't necessarily be all of them.
    #             raise CallbackError(f"Tar files ({tar_list}) could not be " "prepared.")

    #         prepare_id = prepare_result.decode()[:-1]
    #         body_json[MSG.DATA][MSG.PREPARE_ID] = prepare_id

    #     query_resp = self.query_prepare_request(prepare_id, tar_list, fs_client)
    #     # Check whether the file is still offline, if so then requeue filelist

    #     for response in query_resp["responses"]:
    #         tar_path = response["path"]
    #         # First verify file integrity.
    #         if not response["on_tape"] or not response["path_exists"]:
    #             tarname = tar_path.split("/")[-1]
    #             # Big problem if the file is not on tape as it will never be
    #             # prepared properly. Need to fail it and remove it from filelist
    #             # i.e split off the files from the workflow
    #             failed_pds = retrieval_dict[tarname]
    #             self.failedlist.extend(failed_pds)
    #             del retrieval_dict[tarname]

    #         # Check if the preparation has finished for this file
    #         if response["online"]:
    #             self.log(f"Prepare has completed for {tar_path}", RK.LOG_INFO)
    #             continue
    #         elif not response["requested"]:
    #             # If the file is not online and not requested anymore,
    #             # another request needs to be made.
    #             # NOTE: we can either throw an exception here and let this only
    #             # happen a set number of times before failure, or we can just
    #             # requeue without a preapre_id and let the whole process happen
    #             # again. I don't really know the specifics of why something
    #             # would refuse to be prepared, so I don't know what the best
    #             # strategy would be...
    #             raise CallbackError(
    #                 f"Prepare request could not be fulfilled for request "
    #                 f"{prepare_id}, sending for retry."
    #             )
    #         else:
    #             self.log(
    #                 f"Prepare request with id {prepare_id} has not completed "
    #                 f"for {tar_path}, requeuing message with a delay of "
    #                 f"{self.prepare_requeue_delay}",
    #                 RK.LOG_INFO,
    #             )
    #             self.send_pathlist(
    #                 filelist,
    #                 rk_retry,
    #                 body_json,
    #                 state=State.CATALOG_ARCHIVE_AGGREGATING,
    #             )
    #             return

    #     #############
    #     # Main loop:
    #     # Loop through retrieval dict and put the contents of each tar file into
    #     # its appropriate bucket on the objectstore.
    #     for tarname, tar_filelist in retrieval_dict.items():
    #         original_filelist = tar_originals_map[tarname]

    #         # Get the holding_prefix from the first path_details in the
    #         # original filelist. This is guaranteed not to error as it would
    #         # have above otherwise?
    #         holding_prefix, filelist_hash = self.parse_tape_details(original_filelist[0])

    #         tar_filename = f"{filelist_hash}.tar"
    #         if tar_filename != tarname:
    #             raise TarError(
    #                 "Mismatch between tarname parsed from "
    #                 "retrieval_dict and from path_details"
    #             )

    #         holding_tape_path = (
    #             f"root://{tape_server}/{tape_base_dir}/" f"{holding_prefix}"
    #         )
    #         full_tape_path = f"{holding_tape_path}/{tar_filename}"

    #         self.log(
    #             f"Attempting to stream contents of tar file "
    #             f"{full_tape_path} directly from tape archive to object "
    #             "storage",
    #             RK.LOG_INFO,
    #         )

    #         # Stream directly from the tar file to object store, one tar-member
    #         # at a time
    #         try:
    #             path_details = None
    #             with XRDClient.File() as f:
    #                 # Open the tar file with READ
    #                 status, _ = f.open(full_tape_path, OpenFlags.READ)
    #                 if status.status != 0:
    #                     raise XRootDError(
    #                         f"Failed to open file {full_tape_path}"
    #                         f" for reading. Status: {status}"
    #                     )

    #                 # Wrap the xrootd File handler so minio can use it
    #                 fw = Adler32File(f)

    #                 self.log(f"Opening tar file {tar_filename}", RK.LOG_INFO)
    #                 with tarfile.open(
    #                     mode="r:", copybufsize=chunk_size, fileobj=fw
    #                 ) as tar:
    #                     # Check the tar file contains the originally requested
    #                     # files. NOTE: Commented out due to open questions about
    #                     # the workflow.
    #                     # self.validate_tar(tar, original_filelist)

    #                     # for tarinfo in members:
    #                     for path_details in tar_filelist:
    #                         try:
    #                             _ = self.stream_tarmember(
    #                                 path_details, tar, s3_client, chunk_size=chunk_size
    #                             )
    #                         except (HTTPError, S3Error) as e:
    #                             # Handle objectstore exception. Check whether
    #                             # it's from the original list or not, retry/fail
    #                             # as appropriate
    #                             reason = (
    #                                 f"Stream-time exception occurred: "
    #                                 f"({type(e).__name__}: {e})"
    #                             )
    #                             self.log(reason, RK.LOG_DEBUG)
    #                             if path_details in original_filelist:
    #                                 # Retry if one of the original files failed
    #                                 self.process_retry(reason, path_details)
    #                             else:
    #                                 # Fail and move on with our lives
    #                                 self.failedlist.append(path_details)
    #                         except TarMemberError as e:
    #                             # This exception is only raised when retrying
    #                             # will not help, so fail the file (regardless of
    #                             # origin)
    #                             self.log(
    #                                 "TarMemberError encountered at stream "
    #                                 f"time {e}, failing whole tar file "
    #                                 f"({tar_filename})",
    #                                 RK.LOG_INFO,
    #                             )
    #                             self.failedlist.append(path_details)
    #                         except FileAlreadyRetrieved as e:
    #                             # Handle specific case of a file already having
    #                             # been retrieved.
    #                             self.log(
    #                                 f"Tar member {path_details.path} has "
    #                                 "already been retrieved, passing to "
    #                                 "completelist",
    #                                 RK.LOG_INFO,
    #                             )
    #                             self.completelist.append(path_details)
    #                         except Exception as e:
    #                             self.log(
    #                                 "Unexpected exception occurred during "
    #                                 f"stream time {e}",
    #                                 RK.LOG_ERROR,
    #                             )
    #                             raise e
    #                         else:
    #                             # Log successful
    #                             self.log(
    #                                 f"Successfully retrieved {path_details.path}"
    #                                 f" from the archive and streamed to object "
    #                                 "store",
    #                                 RK.LOG_INFO,
    #                             )
    #                             self.completelist.append(path_details)
    #         except (TarError, ArchiveError) as e:
    #             # Handle whole-tar error, i.e. fail whole list
    #             self.log(
    #                 f"Tar-level error raised: {e}, failing whole tar and "
    #                 "continuing to next.",
    #                 RK.LOG_ERROR,
    #             )
    #             # Give each a failure reason for creating FailedFiles
    #             for pd in tar_filelist:
    #                 pd.retries.increment(reason=str(e))
    #             self.failedlist.extend(tar_filelist)
    #         except XRootDError as e:
    #             # Handle xrootd error, where file failed to be read. Retry the
    #             # whole tar.
    #             reason = "Failed to open file with XRootD"
    #             self.log(f"{e}. Passing for retry.", RK.LOG_ERROR)
    #             for pd in tar_filelist:
    #                 self.process_retry(reason, pd)
    #         except Exception as e:
    #             self.log(
    #                 f"Unexpected exception occurred during stream time {e}",
    #                 RK.LOG_ERROR,
    #             )
    #             raise e
    #         else:
    #             # Log successful retrieval
    #             self.log(f"Successfully streamed tar file {tarname}", RK.LOG_INFO)

    #     # Evict the files at the end to ensure the EOS cache doesn't fill up.
    #     # First need to check whether the files are in another prepare request
    #     self.log(
    #         "Querying prepare request to check whether it should be "
    #         f"evicted from the disk-cache",
    #         RK.LOG_INFO,
    #     )
    #     query_resp = self.query_prepare_request(prepare_id, tar_list, fs_client)
    #     evict_list = [
    #         response["path"]
    #         for response in query_resp["responses"]
    #         if not response["requested"]
    #     ]
    #     evict_list = self.prepare_preparelist(evict_list)

    #     status, _ = fs_client.prepare(evict_list, PrepareFlags.EVICT)
    #     if status.status != 0:
    #         self.log(f"Could not evict tar files from tape cache.", RK.LOG_WARNING)

    #     self.log(
    #         "Archive read complete, passing lists back to worker for "
    #         "re-routing and cataloguing.",
    #         RK.LOG_INFO,
    #     )
    #     self.log(
    #         f"List lengths: complete:{len(self.completelist)}, "
    #         f"retry:{len(self.retrylist)}, failed:{len(self.failedlist)}",
    #         RK.LOG_DEBUG,
    #     )

    #     # Reorganise the lists and send the necessary message info for each mode
    #     if len(self.completelist) > 0:
    #         # Going to transfer, so only include original files in the sent
    #         # pathlist.
    #         originals_to_complete = [pd for pd in self.completelist if pd in filelist]
    #         self.send_pathlist(
    #             originals_to_complete,
    #             rk_complete,
    #             body_json,
    #             mode=FilelistType.archived,
    #         )
    #     if len(self.retrylist) > 0:
    #         # This will return to here, so we need to make sure the retrieval
    #         # dict that's passed back is set correctly. Make a list of the
    #         # originals that need retrying
    #         originals_to_retry = [pd for pd in self.retrylist if pd in filelist]

    #         # Combine everything that has either completed or failed, i.e. which
    #         # shouldn't be retried.
    #         retry_retrieval_dict = {}
    #         not_to_retry = self.completelist + self.failedlist
    #         # Horrible nested bastard to rebuild the minimum version of the
    #         # retrieval_dict. TODO: Re-look at this if we end up using it,
    #         # could probably be more efficient
    #         for tarname, tar_filelist in retrieval_dict:
    #             for pd in originals_to_retry:
    #                 if pd in tar_filelist:
    #                     trimmed_tarlist = [
    #                         pd for pd in tar_filelist if pd not in not_to_retry
    #                     ]
    #                     retry_retrieval_dict[tarname] = trimmed_tarlist

    #         body_json[MSG.DATA][MSG.RETRIEVAL_FILELIST] = retry_retrieval_dict
    #         self.send_pathlist(
    #             originals_to_retry, rk_retry, body_json, mode=FilelistType.retry
    #         )
    #     if len(self.failedlist) > 0:
    #         # This will eventually go to CATALOG_ARCHIVE_ROLLBACK, so can send
    #         # all path_details in this.
    #         self.send_pathlist(
    #             self.failedlist,
    #             rk_failed,
    #             body_json,
    #             state=State.CATALOG_ARCHIVE_ROLLBACK,
    #         )

    # def parse_tape_details(self, path_details):
    #     """Get the tape information from a given path_details object
    #     (holding_prefix and filelist_hash) for constructing the full tape path
    #     """
    #     # Then parse tape path information in preparedness for file
    #     # preparation
    #     tape_path = path_details.tape_path
    #     try:
    #         # Split out the root and path, passed from the Location
    #         tape_location_root, original_path = tape_path.split(":")
    #         # Split further to get the holding_prefix and the tar filename
    #         holding_prefix, filelist_hash = tape_location_root.split("_")
    #         return holding_prefix, filelist_hash
    #     except ValueError as e:
    #         reason = f"Could not unpack mandatory info from path_details. {e}"
    #         self.log(reason, RK.LOG_ERROR)
    #         raise ArchiveError(reason)

    # def prepare_preparelist(self, tar_list: List[str]) -> List[str | bytes]:
    #     # Check the version of xrootd and convert the list to bytes if necessary
    #     if version("XRootD") < "5.5.5":
    #         tar_list = [i.decode("utf_8") for i in tar_list]

    #     return tar_list

    # def query_prepare_request(
    #     self, prepare_id: str, tar_list: List[str], fs_client: XRDClient.FileSystem
    # ) -> Dict[str, str]:
    #     # Generate the arg string for the prepare query with new line characters
    #     query_args = "\n".join([prepare_id, *tar_list])
    #     status, query_resp = fs_client.query(QueryCode.PREPARE, query_args)
    #     if status.status != 0:
    #         raise CallbackError(
    #             f"Could not check status of prepare request " f"{prepare_id}."
    #         )

    #     # Convert into a dictionary
    #     return json.loads(query_resp.decode())

    # def validate_tar(self, tar: TarFile, original_filelist: List[PathDetails]) -> None:
    #     # Convert to sets for quick calculation of missing subset
    #     members_set = {ti.name for ti in tar.getmembers()}
    #     orig_fl_set = {pd.original_path for pd in original_filelist}
    #     missing_files = orig_fl_set - members_set
    #     if missing_files:
    #         # Something has gone wrong if the file is not actually in the tar.
    #         reason = (
    #             f"Some originally requested files ({missing_files}) do not "
    #             f"appear to be in the tarfile at {tar.name}."
    #         )
    #         self.log(reason, RK.LOG_ERROR)
    #         # NOTE: Does this close the file properly?
    #         # NOTE: Do we want to try and read the files that are there?
    #         # Probably...
    #         raise TarError(reason)

    # def get_bucket_info(self, path_details: PathDetails) -> Tuple[str]:
    #     """Get the bucket_name and path from the object_name of a given
    #     path_details object.
    #     """
    #     if path_details.bucket_name is not None:
    #         bucket_name = path_details.bucket_name
    #         object_name = path_details.object_name
    #     # Otherwise, log error and queue for retry
    #     else:
    #         reason = "Unable to get bucket_name from message info"
    #         self.process_retry(reason, path_details)
    #         raise TarMemberError(reason)
    #     return bucket_name, object_name

    # def validate_bucket(self, path_details: PathDetails, s3_client: Minio):
    #     """Extract bucket info from path details object and then check it's
    #     valid, i.e. that it exists and whether the object specified is already
    #     in it. Will raise an Exception as necessary.

    #     :raises: FileAlreadyRetrieved, TarMemberError, HTTPError, S3Error
    #     """
    #     bucket_name, object_name = self.get_bucket_info(path_details)

    #     try:
    #         # Check that bucket exists, and create if not
    #         if not s3_client.bucket_exists(bucket_name):
    #             self.log(
    #                 f"Creating bucket ({bucket_name}) for this tarfile "
    #                 f"member ({path_details})",
    #                 RK.LOG_INFO,
    #             )
    #             s3_client.make_bucket(bucket_name)
    #         else:
    #             self.log(
    #                 f"Bucket for this transaction ({bucket_name}) already " f"exists",
    #                 RK.LOG_INFO,
    #             )
    #             objects_iter = s3_client.list_objects(
    #                 bucket_name, prefix=f"/{object_name}"
    #             )
    #             objects = [obj.object_name for obj in objects_iter]
    #             # Look for object in bucket, continue if not present
    #             assert object_name not in objects
    #     except (HTTPError, S3Error) as e:
    #         # If bucket can't be created then pass for retry and continue
    #         self.log(
    #             f"Bucket {bucket_name} could not be validated due to error "
    #             f"connecting with tenancy. ({e})"
    #         )
    #         raise e
    #     except AssertionError as e:
    #         # If it exists in the bucket then our job is done, we can just
    #         # continue in the loop
    #         self.log(
    #             f"Object {object_name} already exists in {bucket_name}. "
    #             f"Skipping to next archive retrieval.",
    #             RK.LOG_WARNING,
    #         )
    #         raise FileAlreadyRetrieved(f"File {path_details} already archived.")
    #     return bucket_name, object_name

    # @retry((S3Error, HTTPError), tries=5, delay=1, backoff=2)
    # def stream_tarmember(
    #     self, path_details: PathDetails, tar: TarFile, s3_client: Minio, chunk_size=None
    # ) -> ObjectWriteResult:
    #     """The inner loop of actions to be performed on each memeber of the tar
    #     file"""
    #     try:
    #         tarinfo = tar.getmember(path_details.original_path)
    #     except KeyError:
    #         reason = (
    #             f"Could not find tar member for path details object "
    #             f"{path_details}. Cannot continue."
    #         )
    #         raise TarMemberError(reason)

    #     # Get bucket info and check that it exists/doesn't already contain the
    #     # file
    #     bucket_name, object_name = self.validate_bucket(path_details, s3_client)

    #     self.log(
    #         f"Starting stream of {tarinfo.name} to object store bucket "
    #         f"{bucket_name}.",
    #         RK.LOG_INFO,
    #     )

    #     # Extract the file as a file object, this makes a thin wrapper around
    #     # the filewrapper we're already using and maintains chunked reading
    #     f = tar.extractfile(tarinfo)
    #     write_result = s3_client.put_object(
    #         bucket_name,
    #         object_name,
    #         f,
    #         -1,
    #         part_size=chunk_size,
    #     )
    #     self.log(f"Finsihed stream of {tarinfo.name} to object store", RK.LOG_INFO)

    #     return write_result

    # def process_retry(self, reason: str, path_details: PathDetails) -> None:
    #     """Convenience function for logging and setting in motion a retry for a
    #     given path_details object.
    #     """
    #     self.log(f"{reason}. Adding {path_details.path} to retry list.", RK.LOG_ERROR)
    #     path_details.retries.increment(reason=reason)
    #     self.retrylist.append(path_details)


def main():
    consumer = GetArchiveConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
