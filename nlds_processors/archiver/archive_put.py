from typing import List, Dict, Any
from pathlib import Path
import os
import tarfile
from hashlib import shake_256

import minio
from minio.error import S3Error
from retry import retry
from urllib3.exceptions import HTTPError
from XRootD import client
from XRootD.client.flags import (DirListFlags, PrepareFlags, DirListFlags, 
                                 OpenFlags, MkDirFlags, QueryCode)

from nlds_processors.archiver.archive_base import (BaseArchiveConsumer, 
                                                   ArchiveError, 
                                                   AdlerisingXRDFile)
from nlds.rabbit.consumer import FilelistType, State
from nlds.details import PathDetails
from nlds.errors import CallbackError


class PutArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_put_q"
    DEFAULT_ROUTING_KEY = (f"{BaseArchiveConsumer.RK_ROOT}."
                           f"{BaseArchiveConsumer.RK_TRANSFER_PUT}."
                           f"{BaseArchiveConsumer.RK_WILD}")
    DEFAULT_STATE = State.ARCHIVE_PUTTING


    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)


    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, tape_url: str, filelist: List[PathDetails], 
                 rk_origin: str, body_json: Dict[str, str]):
        # Grab message sections for ease of access. This shouldn't fail becuase
        # of checking done in the worker, might make sense to do it here/in the 
        # basetransfer too though
        msg_details = body_json[self.MSG_DETAILS]
        msg_meta = body_json[self.MSG_META]

        # Can call this with impunity as the url has been verified previously
        tape_server, tape_base_dir = self.split_tape_url(tape_url)
        holding_slug = self.get_holding_slug(body_json)

        # Create minio client
        s3_client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        # Create the FileSystem client at this point to verify the tape_base_dir 
        fs_client = client.FileSystem(f"root://{tape_server}")
        # Attempt to create it, should succeed if everything is working properly
        # but otherwise will be passed for message-level retry. 
        status, _ = fs_client.mkdir(tape_base_dir, MkDirFlags.MAKEPATH)
        if status.status != 0:
            self.log(f"Failed status message: {status.message}", 
                     self.RK_LOG_ERROR)
            raise CallbackError(f"Base dir {tape_base_dir} derived from "
                                f"tape_url ({tape_url}) could not be created or"
                                f" verified.")

        rk_complete = ".".join([rk_origin, self.RK_ARCHIVE_PUT, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_ARCHIVE_PUT, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_ARCHIVE_PUT, self.RK_FAILED])

        # Generate a name for the tarfile by hashing the combined filelist. 
        # Length of the hash will be 16
        filenames = [f.original_path for f in filelist]
        filelist_hash = shake_256("".join(filenames).encode()).hexdigest(8) 
        tar_filename = f"{filelist_hash}.tar"

        # All files are now supposed to be from a single aggregation according 
        # to the current implementation of the PUT workflow. Here we do an 
        # initial loop over all the files to verify contents before writing 
        # anything to tape. 
        for path_details in filelist:
            try:
                tape_path = path_details.tape_path
                # Split out the root and path, as they are in the Location
                check_root, _ = tape_path.split(":")
                # Split this further to get the holding_slug and the 
                # original_path
                check_holding_slug, check_hash = check_root.split("_")
                # Split the object path to get bucket and object path
                check_bucket, check_object = path_details.object_name.split(':')
            except ValueError as e:
                raise CallbackError("Could not unpack mandatory info from "
                                    "filelist, cannot continue.")
            try:
                # Check that the calculated filelist_hash and the constructed 
                # holding_slug match those stored for each of the files passed
                assert check_hash == filelist_hash
                assert check_holding_slug == holding_slug
            except AssertionError as e:
                raise CallbackError(
                    f"Could not verify calculated filehash ({filelist_hash}) "
                    f"and holding slug ({holding_slug}) against values passed "
                    f"from catalog ({check_holding_slug} and {check_hash})."
                )
            try:
                # Check the bucket and that the object is in the bucket and 
                # matches the metadata stored.
                assert s3_client.bucket_exists(check_bucket)
                objects = s3_client.list_objects(check_bucket, 
                                                 prefix=check_object)
                assert len(objects) == 1
                assert check_object == objects[0].object_name
                assert path_details.size == objects[0].size
            except AssertionError as e:
                raise CallbackError(
                    f"Could not verify that bucket {check_bucket} contained "
                    f"object {check_object} before writing to tape."
                )
            
            # NOTE: This is probably unnecessary given we're not doing 
            # file-level retries for tape writes to tar files now
            # First we can check whether each path_details has failed too many 
            # times
            # if path_details.retries.count > self.max_retries:
            #     self.append_and_send(
            #         path_details, rk_failed, body_json, list_type="failed"
            #     )
            #     continue
            # else:
            #     # Otherwise, log error and queue for retry
            #     reason = "Unable to get bucket_name from message info"
            #     self.log(f"{reason}, adding "
            #              f"{path_details.object_name} to retry list.", 
            #              self.RK_LOG_INFO)
            #     path_details.retries.increment(reason=reason)
            #     self.append_and_send(
            #         path_details, rk_failed, body_json, list_type="retry"
            #     )
            #     continue
            
        # After verifying the filelist integrity we can make the path + tar file
        holding_tape_path = (f"root://{tape_server}/{tape_base_dir}/"
                             f"{holding_slug}")
        full_tape_path = (f"{holding_tape_path}/{tar_filename}")

        # Make holding folder and retry if it can't be created. 
        status, _ = fs_client.mkdir(holding_tape_path, MkDirFlags.MAKEPATH)
        if status.status != 0:
            # If bucket directory couldn't be created then fail for retrying
            raise CallbackError(f"Couldn't create or find holding directory "
                                f"({holding_tape_path}).")
        
        with client.File() as f:
            # Open the file as NEW, to avoid having to prepare it
            status, _ = f.open(full_tape_path, (OpenFlags.NEW | 
                                                OpenFlags.MAKEPATH))
            if status.status != 0:
                raise CallbackError("Failed to open file for writing")
            
            file_wrapper = AdlerisingXRDFile(f)
            
            with tarfile.open(mode="w", fileobj=file_wrapper, 
                              copybufsize=self.chunk_size) as tar:
                for path_details in filelist:
                    item_path = path_details.path
                        
                    self.log(f"Attempting to stream file {item_path} "
                            "directly to tape archive", self.RK_LOG_DEBUG)
                    
                    # Get the relevant variables from the path_details. 
                    # NOTE: This can't fail as it's been verified above. 
                    bucket_name, object_name = path_details.object_name.split(':')

                    tar_info = tarfile.TarInfo(name=object_name)
                    tar_info.size = path_details.size
                    # TODO: add more file data into the tar_info?

                    # Attempt to stream the object directly into the File object            
                    try:
                        result = s3_client.get_object(
                            bucket_name, object_name,
                        )
                        tar.addfile(tar_info, fileobj=result)

                    # NOTE: Need to think about error handling here, any failure 
                    # will result in a corrupted tar file so probably can't just 
                    # pick up where we left off, will need to start the whole 
                    # thing again.
                    except (HTTPError, ArchiveError) as e:
                        reason = f"Stream-time exception occurred: {e}"
                        self.log(reason, self.RK_LOG_DEBUG)
                        self.log(f"Exception encountered during stream, adding "
                                 f"{object_name} to retry-list.", self.RK_LOG_INFO)
                        path_details.retries.increment(reason=reason)
                        self.append_and_send(path_details, rk_retry, body_json, 
                                             list_type=FilelistType.retry)
                    else:
                        # Log successful 
                        self.log(f"Successfully archived {item_path}", 
                                 self.RK_LOG_DEBUG)
                        self.completelist.append(path_details)
                    finally:
                        result.close()
                        result.release_conn()
                        continue

            # Finally get the checksum out of the file wrapper to pass back to 
            # the catalog 
            body_json[self.MSG_DETAILS][self.MSG_CHECKSUM] = \
                file_wrapper.checksum

        self.log("Archive complete, passing lists back to worker for "
                 "re-routing and cataloguing.", self.RK_LOG_INFO)
        
        # Send whatever remains after all items have been (attempted to be) put
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist, rk_complete, body_json, mode="archived"
            )
        if len(self.retrylist) > 0:
            self.send_pathlist(
                self.retrylist, rk_retry, body_json, mode="retry"
            )
        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist, rk_failed, body_json, mode="failed"
            )

    @classmethod
    def get_holding_slug(cls, body: Dict[str, Any]):
        """Get the uneditable holding information from the message body to 
           reproduce the holding slug made in the catalog"""
        try:
            holding_id = body[cls.MSG_META][cls.MSG_HOLDING_ID]
            user = body[cls.MSG_DETAILS][cls.MSG_USER]
            group = body[cls.MSG_DETAILS][cls.MSG_GROUP]
        except KeyError as e:
            raise ArchiveError(f"Could not make holding slug, original error: "
                               f"{e}")

        return f"{holding_id}.{user}.{group}"

    
def main():
    consumer = PutArchiveConsumer()
    consumer.run()

if __name__ == "__main__":
    main()