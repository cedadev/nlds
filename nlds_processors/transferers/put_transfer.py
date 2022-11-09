from typing import List, Dict

import minio
from minio.error import S3Error
from retry import retry

from nlds_processors.transferers.base_transfer import BaseTransferConsumer
from nlds.rabbit.consumer import FilelistType, State
from nlds.details import PathDetails


class PutTransferConsumer(BaseTransferConsumer):
    DEFAULT_QUEUE_NAME = "transfer_put_q"
    DEFAULT_ROUTING_KEY = (f"{BaseTransferConsumer.RK_ROOT}."
                           f"{BaseTransferConsumer.RK_TRANSFER_PUT}."
                           f"{BaseTransferConsumer.RK_WILD}")
    DEFAULT_STATE = State.TRANSFER_PUTTING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, filelist: List[PathDetails], rk_origin: str,
                 body_json: Dict[str, str]):
        client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )

        rk_complete = ".".join([rk_origin, self.RK_TRANSFER_PUT, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_TRANSFER_PUT, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_TRANSFER_PUT, self.RK_FAILED])

        bucket_name = f"nlds.{transaction_id}"

        # Check that bucket exists, and create if not
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            self.log(f"Creating bucket ({bucket_name}) for this"
                     " transaction", self.RK_LOG_INFO)
        else:
            self.log(f"Bucket for this transaction ({transaction_id}) "
                     f"already exists", self.RK_LOG_INFO)

        for path_details in filelist:
            item_path = path_details.path

            # First check whether index item has failed too many times
            if path_details.retries > self.max_retries:
                self.append_and_send(
                    path_details, rk_failed, body_json, list_type="failed"
                )
                continue

            # If check_permissions active then check again that file exists and 
            # is accessible. 
            if (self.check_permissions_fl and 
                not self.check_path_access(item_path)):

                self.log(f"{path_details.path} is inaccessible.", 
                         self.RK_LOG_DEBUG)
                path_details.increment_retry(retry_reason="inaccessible")
                self.append_and_send(
                    path_details, rk_retry, body_json, list_type="retry"
                )
                continue

            self.log(f"Attempting to upload file {path_details.original_path}", 
                     self.RK_LOG_DEBUG)
            
            # The minio client doesn't like file paths starting at root (i.e. 
            # with a slash at the very beginning) so it needs to be stripped if 
            # using minio, and added back on get.
            # TODO: Add this flag to the file details json metadata 
            if self.remove_root_slash_fl and path_details.original_path[0] == "/":
                path_details.object_name = path_details.original_path[1:]
            else:
                path_details.object_name = path_details.original_path
            
            result = client.fput_object(
                bucket_name, 
                path_details.object_name, 
                path_details.original_path,
            )
            self.log(f"Successfully uploaded {path_details.original_path}", 
                     self.RK_LOG_DEBUG)
            self.append_and_send(path_details, rk_complete, body_json, 
                                 list_type=FilelistType.transferred)

        self.log("Transfer complete, passing lists back to worker for "
                 "re-routing and cataloguing.", self.RK_LOG_INFO)
        
        # Send whatever remains after all items have been (attempted to be) put
        if len(self.completelist) > 0:
            self.send_pathlist(
                self.completelist, rk_complete, body_json, mode="transferred"
            )
        if len(self.retrylist) > 0:
            self.send_pathlist(
                self.retrylist, rk_retry, body_json, mode="retry"
            )
        if len(self.failedlist) > 0:
            self.send_pathlist(
                self.failedlist, rk_failed, body_json, mode="failed"
            )

    
def main():
    consumer = PutTransferConsumer()
    consumer.run()

if __name__ == "__main__":
    main()