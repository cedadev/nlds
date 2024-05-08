from typing import List, Dict, Tuple, Any
import json
from importlib.metadata import version
import tarfile
from tarfile import TarFile
from datetime import timedelta

from minio import Minio
from minio.helpers import ObjectWriteResult
from minio.error import S3Error
from retry import retry
from urllib3.exceptions import HTTPError
from XRootD import client
from XRootD.client.flags import (
    DirListFlags, 
    PrepareFlags, 
    OpenFlags, 
    StatInfoFlags,
    QueryCode,
)

from nlds_processors.archiver.archive_base import (BaseArchiveConsumer, 
                                                   ArchiveError,
                                                   AdlerisingXRDFile)
from nlds.rabbit.consumer import FilelistType, State
from nlds.details import PathDetails
from nlds.errors import CallbackError


class TarError(Exception):
    """Exception class to distinguish problems with whole tar (i.e. missing 
    files) from other problems (i.e. broken buckets)
    """
    pass


class TarMemberError(Exception):
    """Exception class to distinguish problems with individual tar members (i.e. 
    broken bucket, ) from larger tar-related problems
    """
    pass


class XRootDError(Exception):
    """Exception class to distinguish the specific problem of a prepared file 
    not being able to be read by xrootd
    """
    pass


class FileAlreadyRetrieved(Exception):
    """Exception class to distinguish the specific situation where a file has
    already been retrieved and exists on the object store
    """
    pass

    
class GetArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_get_q"
    DEFAULT_ROUTING_KEY = (f"{BaseArchiveConsumer.RK_ROOT}."
                           f"{BaseArchiveConsumer.RK_TRANSFER_PUT}."
                           f"{BaseArchiveConsumer.RK_WILD}")
    DEFAULT_STATE = State.ARCHIVE_GETTING

    _PREPARE_REQUEUE_DELAY = "prepare_requeue"
    ARCHIVE_GET_CONSUMER_CONFIG = {
        _PREPARE_REQUEUE_DELAY: timedelta(seconds=30).total_seconds() * 1000,
    }
    DEFAULT_CONSUMER_CONFIG = (BaseArchiveConsumer.DEFAULT_CONSUMER_CONFIG 
                               | ARCHIVE_GET_CONSUMER_CONFIG)


    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.prepare_requeue_delay = self.load_config_value(
            self._PREPARE_REQUEUE_DELAY
        )


    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, tape_url: str, filelist: List[PathDetails], 
                 rk_origin: str, body_json: Dict[str, Any]):
        
        # Can call this with impunity as the url has been verified previously
        tape_server, tape_base_dir = self.split_tape_url(tape_url)

        try:
            raw_rd = dict(body_json[self.MSG_DATA][self.MSG_RETRIEVAL_FILELIST])
            retrieval_dict = {
                tarname: [PathDetails.from_dict(pd_dict) for pd_dict in pd_dicts] 
                for tarname, pd_dicts in raw_rd.items()
            }
        except TypeError as e:
            self.log("Failed to reformat retrieval filelist into PathDetails "
                     "objects. Retrievallist in message does not appear to be "
                     "in the correct format.", self.RK_LOG_ERROR)
            raise e

        # Declare useful variables
        bucket_name = None
        rk_complete = ".".join([rk_origin, self.RK_ARCHIVE_GET, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_ARCHIVE_GET, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_ARCHIVE_GET, self.RK_FAILED])
        
        try:
            prepare_id = body_json[self.MSG_DATA][self.MSG_PREPARE_ID]
        except KeyError:
            self.log("Could not get prepare_id from message info, continuing "
                     "without", self.RK_LOG_INFO)
            prepare_id = None

        # Create minio client
        s3_client = Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        # Create the FileSystem client at this point to verify the tape_base_dir 
        fs_client = client.FileSystem(f"root://{tape_server}")
        # Attempt to verify that the base-directory exists
        self.verify_tape_server(fs_client, tape_server, tape_base_dir) 
        
        # Ensure minimum part_size is met for put_object to function
        chunk_size = max(5*1024*1024, self.chunk_size)

        #################
        # Pre-read loop:
        # Verify the filelist contents and create the necessary lists for the 
        # tape preparation request

        # TODO: refactor this?
        tar_list = []
        tar_originals_map = {}
        for path_details in filelist:
            # First check whether path_details has failed too many times
            if path_details.retries.count > self.max_retries:
                self.failedlist.append(path_details)
                # TODO: do these actually get skipped?
                continue
            
            try:
                holding_slug, filelist_hash = self.parse_tape_details(
                    path_details)
            except ArchiveError as e:
                self.process_retry(str(e), path_details)
                continue
            
            tar_filename = f"{filelist_hash}.tar"
            try:
                # Check that filelist hash is in the full retrieval list
                assert tar_filename in retrieval_dict
            except AssertionError as e:
                reason = f"Tar file name not found in full retrieval list"
                self.process_retry(reason, path_details)
                continue

            holding_tape_path = (f"root://{tape_server}/{tape_base_dir}/"
                                 f"{holding_slug}")
            full_tape_path = (f"{holding_tape_path}/{tar_filename}")

            # Check bucket folder exists on tape
            status, _ = fs_client.dirlist(f"{tape_base_dir}/{holding_slug}", 
                                          DirListFlags.STAT)
            if status.status != 0:
                # If bucket tape-folder can't be found then pass for retry
                reason = (
                    f"Tape holding folder ({tape_base_dir}/{holding_slug}) "
                    f"could not be found, cannot retrieve from archive")
                self.process_retry(reason, path_details)
                continue
            
            # The tar_filenames must be encoded into a list of byte strings for 
            # prepare to work, as of pyxrootd v5.5.3. We group them together to 
            # ensure only a single transaction is passed to the tape server. 
            prepare_item = f"{tape_base_dir}/{holding_slug}/{tar_filename}"
            tar_list.append(prepare_item)

            # Create the tar_originals_map, which maps from tar file to the
            # path_details in the original filelist (i.e. the files in the 
            # original request) for easy retry/failure if something goes wrong.
            if tar_filename not in tar_originals_map:
                tar_originals_map[tar_filename] = [path_details, ]
            else:
                tar_originals_map[tar_filename].append(path_details)

        # If tar_list is empty at this point everything is either in need of 
        # retrying or failing so we can just start a TLR with an exception
        if len(tar_list) == 0:
            raise CallbackError("All paths failed verification, nothing to "
                                "prepare or get.")
        
        ###########
        # Prepare:
        # Make a preparation request and/or check on the status of an existing 
        # preparation request

        # Deduplicate the list of tar files
        tar_list = list(set(tar_list))
        # Convert to bytes (if necessary)
        prepare_list = self.prepare_preparelist(tar_list)

        if prepare_id is None:
            # Prepare all the tar files at once. This can take a while so requeue 
            # the message afterwards to free up the consumer and periodicaly check 
            # the status of the prepare command. 
            # TODO: Utilise the prepare result for eviction later? 
            status, prepare_result = fs_client.prepare(prepare_list, 
                                                       PrepareFlags.STAGE)
            if status.status != 0:
                # If tarfiles can't be staged then pass for transaction-level 
                # retry. TODO: Probably need to figure out which files have failed 
                # this step as it won't necessarily be all of them.
                raise CallbackError(f"Tar files ({tar_list}) could not be "
                                    "prepared.")
        
            prepare_id = prepare_result.decode()[:-1]
            body_json[self.MSG_DATA][self.MSG_PREPARE_ID] = prepare_id
        
        query_resp = self.query_prepare_request(prepare_id, tar_list, fs_client)
        # Check whether the file is still offline, if so then requeue filelist
            
        for response in query_resp['responses']:
            tar_path = response['path']
            # First verify file integrity.
            if not response['on_tape'] or not response['path_exists']:
                tarname = tar_path.split("/")[-1]
                # Big problem if the file is not on tape as it will never be 
                # prepared properly. Need to fail it and remove it from filelist
                # i.e split off the files from the workflow
                failed_pds = retrieval_dict[tarname]
                self.failedlist.extend(failed_pds)
                del retrieval_dict[tarname]
                
            # Check if the preparation has finished for this file
            if response['online']:
                self.log(f"Prepare has completed for {tar_path}", 
                         self.RK_LOG_INFO)
                continue
            elif not response['requested']:
                # If the file is not online and not requested anymore, 
                # another request needs to be made. 
                # NOTE: we can either throw an exception here and let this only 
                # happen a set number of times before failure, or we can just 
                # requeue without a preapre_id and let the whole process happen 
                # again. I don't really know the specifics of why something 
                # would refuse to be prepared, so I don't know what the best 
                # strategy would be...
                raise CallbackError(
                    f"Prepare request could not be fulfilled for request "
                    f"{prepare_id}, sending for retry."
                )
            else:
                # TODO: split list at this point? Probably want to keep all 
                # members of the same request together?
                self.log(
                    f"Prepare request with id {prepare_id} has not completed "
                    f"for {tar_path}, requeuing message with a delay of "
                    f"{self.prepare_requeue_delay}", self.RK_LOG_INFO)
                self.send_pathlist(
                    filelist, rk_retry, body_json, mode=FilelistType.retry,
                    state=State.CATALOG_ARCHIVE_AGGREGATING, 
                    delay=self.prepare_requeue_delay,
                )
                return

        #############
        # Main loop:
        # Loop through retrieval dict and put the contents of each tar file into 
        # its appropriate bucket on the objectstore.
        for tarname, tar_filelist in retrieval_dict.items():
            original_filelist = tar_originals_map[tarname]
            
            # Get the holding_slug from the first path_details in the 
            # original filelist. This is guaranteed not to error as it would 
            # have above otherwise?
            holding_slug, filelist_hash = self.parse_tape_details(
                original_filelist[0])

            tar_filename = f"{filelist_hash}.tar"
            if tar_filename != tarname:
                raise TarError("Mismatch between tarname parsed from "
                               "retrieval_dict and from path_details")
            
            holding_tape_path = (f"root://{tape_server}/{tape_base_dir}/"
                                 f"{holding_slug}")
            full_tape_path = (f"{holding_tape_path}/{tar_filename}")

            self.log(f"Attempting to stream contents of tar file "
                     f"{full_tape_path} directly from tape archive to object "
                     "storage", self.RK_LOG_INFO)
            
            # Stream directly from the tar file to object store, one tar-member 
            # at a time 
            try:
                path_details = None
                with client.File() as f:
                    # Open the tar file with READ
                    status, _ = f.open(full_tape_path, OpenFlags.READ)
                    if status.status != 0:
                        raise XRootDError(f"Failed to open file {full_tape_path}"
                                          f" for reading. Status: {status}")

                    # Wrap the xrootd File handler so minio can use it 
                    fw = AdlerisingXRDFile(f)

                    self.log(f"Opening tar file {tar_filename}", 
                             self.RK_LOG_INFO)
                    with tarfile.open(mode='r:', copybufsize=chunk_size, 
                                      fileobj=fw) as tar:
                        # Check the tar file contains the originally requested 
                        # files. NOTE: Commented out due to open questions about 
                        # the workflow.
                        # self.validate_tar(tar, original_filelist)

                        # for tarinfo in members:
                        for path_details in tar_filelist:
                            try:
                                _ = self.stream_tarmember(
                                    path_details, 
                                    tar, 
                                    s3_client, 
                                    chunk_size=chunk_size
                                )
                            except (HTTPError, S3Error) as e:
                                # Handle objectstore exception. Check whether 
                                # it's from the original list or not, retry/fail
                                # as appropriate
                                reason = (f"Stream-time exception occurred: "
                                          f"({type(e).__name__}: {e})")
                                self.log(reason, self.RK_LOG_DEBUG)
                                if path_details in original_filelist:
                                    # Retry if one of the original files failed
                                    self.process_retry(reason, path_details)
                                else: 
                                    # Fail and move on with our lives 
                                    self.failedlist.append(path_details)
                            except TarMemberError as e:
                                # This exception is only raised when retrying 
                                # will not help, so fail the file (regardless of
                                # origin)
                                self.log("TarMemberError encountered at stream "
                                         f"time {e}, failing whole tar file "
                                         f"({tar_filename})", self.RK_LOG_INFO)
                                self.failedlist.append(path_details)
                            except FileAlreadyRetrieved as e:
                                # Handle specific case of a file already having
                                # been retrieved.
                                self.log(f"Tar member {path_details.path} has "
                                         "already been retrieved, passing to "
                                         "completelist", self.RK_LOG_INFO)
                                self.completelist.append(path_details)
                            except Exception as e:
                                self.log("Unexpected exception occurred during "
                                         f"stream time {e}", self.RK_LOG_ERROR)
                                raise
                            else:
                                # Log successful 
                                self.log(
                                    f"Successfully retrieved {path_details.path}"
                                    f" from the archive and streamed to object "
                                    "store", self.RK_LOG_INFO
                                )
                                self.completelist.append(path_details)
            except (TarError, ArchiveError) as e:
                # Handle whole-tar error, i.e. fail whole list 
                self.log(f"Tar-level error raised: {e}, failing whole tar and "
                         "continuing to next.", self.RK_LOG_ERROR)
                # Give each a failure reason for creating FailedFiles
                for pd in tar_filelist:
                    pd.retries.increment(reason=str(e))
                self.failedlist.extend(tar_filelist)
            except XRootDError as e:
                # Handle xrootd error, where file failed to be read. Retry the 
                # whole tar.
                reason = "Failed to open file with XRootD"
                self.log(f"{e}. Passing for retry.", self.RK_LOG_ERROR)
                for pd in tar_filelist:
                    self.process_retry(reason, pd)
            except Exception as e:
                self.log(f"Unexpected exception occurred during stream time {e}", 
                         self.RK_LOG_ERROR)
                raise
            else:
                # Log successful retrieval
                self.log(f"Successfully streamed tar file {tarname}", 
                         self.RK_LOG_INFO)
        
        # Evict the files at the end to ensure the EOS cache doesn't fill up. 
        # First need to check whether the files are in another prepare request
        self.log("Querying prepare request to check whether it should be "
                 f"evicted from the disk-cache", self.RK_LOG_INFO)
        query_resp = self.query_prepare_request(prepare_id, tar_list, fs_client)
        evict_list = [response['path'] for response in query_resp['responses']
                      if not response['requested']]
        evict_list = self.prepare_preparelist(evict_list)

        status, _ = fs_client.prepare(evict_list, PrepareFlags.EVICT)
        if status.status != 0:
            self.log(f"Could not evict tar files from tape cache.", 
                     self.RK_LOG_WARNING)

        self.log("Archive read complete, passing lists back to worker for "
                 "re-routing and cataloguing.", self.RK_LOG_INFO)
        self.log(
            f"List lengths: complete:{len(self.completelist)}, "
            f"retry:{len(self.retrylist)}, failed:{len(self.failedlist)}", 
            self.RK_LOG_DEBUG
        )

        # Reorganise the lists and send the necessary message info for each mode
        if len(self.completelist) > 0:
            # Going to transfer, so only include original files in the sent 
            # pathlist.
            originals_to_complete = [pd for pd in self.completelist 
                                     if pd in filelist]
            self.send_pathlist(
                originals_to_complete, rk_complete, body_json, 
                mode=FilelistType.archived,
            )
        if len(self.retrylist) > 0:
            # This will return to here, so we need to make sure the retrieval 
            # dict that's passed back is set correctly. Make a list of the 
            # originals that need retrying 
            originals_to_retry = [pd for pd in self.retrylist if pd in filelist]
            
            # Combine everything that has either completed or failed, i.e. which 
            # shouldn't be retried. 
            retry_retrieval_dict = {}
            not_to_retry = self.completelist + self.failedlist
            # Horrible nested bastard to rebuild the minimum version of the 
            # retrieval_dict. TODO: Re-look at this if we end up using it, 
            # could probably be more efficient
            for tarname, tar_filelist in retrieval_dict:
                for pd in originals_to_retry:
                    if pd in tar_filelist:
                        trimmed_tarlist = [pd for pd in tar_filelist 
                                            if pd not in not_to_retry]
                        retry_retrieval_dict[tarname] = trimmed_tarlist
            
            body_json[self.MSG_DATA][self.MSG_RETRIEVAL_FILELIST] = (
                retry_retrieval_dict
            )        
            self.send_pathlist(
                originals_to_retry, rk_retry, body_json, 
                mode=FilelistType.retry
            )
        if len(self.failedlist) > 0:
            # This will eventually go to CATALOG_ARCHIVE_ROLLBACK, so can send 
            # all path_details in this.
            self.send_pathlist(
                self.failedlist, rk_failed, body_json, 
                state=State.CATALOG_ARCHIVE_ROLLBACK
            )

    def parse_tape_details(self, path_details):
        """Get the tape information from a given path_details object 
        (holding_slug and filelist_hash) for constructing the full tape path
        """
        # Then parse tape path information in preparedness for file 
        # preparation
        tape_path = path_details.tape_path
        try:
            # Split out the root and path, passed from the Location
            tape_location_root, original_path = tape_path.split(":")
            # Split further to get the holding_slug and the tar filename
            holding_slug, filelist_hash = tape_location_root.split("_")
            return holding_slug, filelist_hash
        except ValueError as e:
            reason = (f"Could not unpack mandatory info from path_details. {e}")
            self.log(reason, self.RK_LOG_ERROR)
            raise ArchiveError(reason)
        

    def prepare_preparelist(self, tar_list: List[str]) -> List[str | bytes]:
        # Check the version of xrootd and convert the list to bytes if necessary
        if version("XRootD") < "5.5.5":
            tar_list = [i.decode("utf_8") for i in tar_list]

        return tar_list
    

    def query_prepare_request(self, prepare_id: str, tar_list: List[str], 
                              fs_client: client.FileSystem) -> Dict[str, str]:
        # Generate the arg string for the prepare query with new line characters
        query_args = "\n".join([prepare_id, *tar_list])
        status, query_resp = fs_client.query(QueryCode.PREPARE, query_args)
        if status.status != 0:
            raise CallbackError(f"Could not check status of prepare request "
                                f"{prepare_id}.")
        
        # Convert into a dictionary
        return json.loads(query_resp.decode())
        

    def validate_tar(self, tar: TarFile, original_filelist: List[PathDetails]
                     ) -> None:
        # Convert to sets for quick calculation of missing subset
        members_set = {ti.name for ti in tar.getmembers()}
        orig_fl_set = {pd.original_path for pd in original_filelist}
        missing_files = orig_fl_set - members_set
        if missing_files:
            # Something has gone wrong if the file is not actually in the tar.
            reason = (
                f"Some originally requested files ({missing_files}) do not "
                f"appear to be in the tarfile at {tar.name}."
            )
            self.log(reason, self.RK_LOG_ERROR)
            # NOTE: Does this close the file properly?
            # NOTE: Do we want to try and read the files that are there? 
            # Probably...
            raise TarError(reason)
        

    def get_bucket_info(self, path_details: PathDetails) -> Tuple[str]:
        """Get the bucket_name and path from the object_name of a given 
        path_details object.
        """
        if len(path_details.object_name.split(':')) == 2:
            bucket_name, object_name = path_details.object_name.split(':')
        # Otherwise, log error and queue for retry
        else:
            reason = "Unable to get bucket_name from message info"
            self.process_retry(reason, path_details)
            raise TarMemberError(reason)
        return bucket_name, object_name
        

    def validate_bucket(self, path_details: PathDetails, s3_client: Minio):
        """Extract bucket info from path details object and then check it's 
        valid, i.e. that it exists and whether the object specified is already 
        in it. Will raise an Exception as necessary.

        :raises: FileAlreadyRetrieved, TarMemberError, HTTPError, S3Error 
        """
        bucket_name, object_name = self.get_bucket_info(path_details)
        
        try:
            # Check that bucket exists, and create if not
            if not s3_client.bucket_exists(bucket_name):
                self.log(f"Creating bucket ({bucket_name}) for this tarfile "
                         f"member ({path_details})", self.RK_LOG_INFO)
                s3_client.make_bucket(bucket_name)
            else:
                self.log(f"Bucket for this transaction ({bucket_name}) already "
                         f"exists", self.RK_LOG_INFO)
                objects_iter = s3_client.list_objects(bucket_name, 
                                                      prefix=f"/{object_name}")
                objects = [obj.object_name for obj in objects_iter]
                # Look for object in bucket, continue if not present
                assert object_name not in objects
        except (HTTPError, S3Error) as e:
            # If bucket can't be created then pass for retry and continue
            self.log(f"Bucket {bucket_name} could not be validated due to error "
                     f"connecting with tenancy. ({e})")
            raise e
        except AssertionError as e:
            # If it exists in the bucket then our job is done, we can just 
            # continue in the loop
            self.log(
                f"Object {object_name} already exists in {bucket_name}. "
                f"Skipping to next archive retrieval.", self.RK_LOG_WARNING
            )
            raise FileAlreadyRetrieved(f"File {path_details} already archived.")
        return bucket_name, object_name
    

    @retry((S3Error, HTTPError) , tries=5, delay=1, backoff=2)
    def stream_tarmember(self, path_details: PathDetails, tar: TarFile, 
                         s3_client: Minio, chunk_size=None) -> ObjectWriteResult:
        """The inner loop of actions to be performed on each memeber of the tar 
        file"""
        try:
            tarinfo = tar.getmember(
                path_details.original_path
            )
        except KeyError:
            reason = (f"Could not find tar member for path details object "
                      f"{path_details}. Cannot continue.")
            raise TarMemberError(reason)

        # Get bucket info and check that it exists/doesn't already contain the 
        # file
        bucket_name, object_name = self.validate_bucket(path_details, s3_client)

        self.log(f"Starting stream of {tarinfo.name} to object store bucket "
                 f"{bucket_name}.", self.RK_LOG_INFO)
        
        # Extract the file as a file object, this makes a thin wrapper around 
        # the filewrapper we're already using and maintains chunked reading
        f = tar.extractfile(tarinfo)
        write_result = s3_client.put_object(
            bucket_name, 
            object_name, 
            f, 
            -1, 
            part_size=chunk_size,
        )
        self.log(f"Finsihed stream of {tarinfo.name} to object store", 
                 self.RK_LOG_INFO)
        
        return write_result
    

    def process_retry(self, reason: str, path_details: PathDetails) -> None:
        """Convenience function for logging and setting in motion a retry for a 
        given path_details object.
        """
        self.log(f"{reason}. Adding {path_details.path} to retry list.", 
                 self.RK_LOG_ERROR)
        path_details.retries.increment(reason=reason)
        self.retrylist.append(path_details)


def main():
    consumer = GetArchiveConsumer()
    consumer.run()

if __name__ == "__main__":
    main()