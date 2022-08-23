from typing import List, Dict
import json
from datetime import datetime, timedelta

import minio
from minio.error import S3Error
from retry import retry

from .base_transfer import BaseTransferConsumer
from nlds.details import PathDetails


class PutTransferConsumer(BaseTransferConsumer):
    DEFAULT_QUEUE_NAME = "transfer_put_q"
    DEFAULT_ROUTING_KEY = (f"{BaseTransferConsumer.RK_ROOT}."
                           f"{BaseTransferConsumer.RK_TRANSFER_PUT}."
                           f"{BaseTransferConsumer.RK_WILD}")

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
        self.reset()

    def reset(self):
        super().reset()

        self.transferlist = []
        self.retrylist = []
        self.failedlist = []

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

        rk_complete = ".".join([rk_origin, self.RK_INDEX, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_INDEX, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_INDEX, self.RK_FAILED])

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
                    path_details, rk_failed, body_json, mode="failed"
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
                    path_details, rk_retry, body_json, mode="retry"
                )
                continue

            self.log(f"Attempting to upload file {path_details.item}", 
                     self.RK_LOG_DEBUG)
            
            # The minio client doesn't like file paths starting at root (i.e. 
            # with a slash at the very beginning) so it needs to be stripped if 
            # using minio, and added back on get.
            # TODO: Add this flag to the file details json metadata 
            if self.remove_root_slash_fl and path_details.original_path[0] == "/":
                object_name = path_details.original_path[1:]
            else:
                object_name = path_details.original_path
            
            result = client.fput_object(
                bucket_name, object_name, path_details.item,
            )
            self.log(f"Successfully uploaded {path_details.item}", 
                     self.RK_LOG_DEBUG)

        self.log("Transfer complete, passing lists back to worker for "
                 f"re-routing and cataloguing.", self.RK_LOG_INFO)
        
        # Send whatever remains after all directories have been walked
        if len(self.transferlist) > 0:
            self.send_transferlist(
                self.transferlist, rk_complete, body_json, mode="transferred"
            )
        if len(self.retrylist) > 0:
            self.send_transferlist(
                self.retrylist, rk_retry, body_json, mode="retry"
            )
        if len(self.failedlist) > 0:
            self.send_transferlist(
                self.failedlist, rk_failed, body_json, mode="failed"
            )

    def append_and_send(self, path_details: PathDetails, routing_key: str, 
                        body_json: Dict[str, str], mode: str = "transferred"
                        ) -> None:
        # Choose the correct indexlist for the mode of operation
        if mode == "transferred":
            transferlist = self.transferlist
        elif mode == "retry":
            transferlist = self.retrylist
        elif mode == "failed":
            transferlist = self.failedlist
        else: 
            raise ValueError(f"Invalid mode provided {mode}")
        
        transferlist.append(path_details)
        
        # The default message cap is the length of the index list. This applies
        # to failed or problem lists by default
        if len(transferlist) >= self.filelist_max_len:
            # Send directly to exchange and reset filelist
            self.send_transferlist(
                transferlist, routing_key, body_json, mode=mode
            )
            transferlist.clear()

    def send_transferlist(
            self, transferlist: List[PathDetails], routing_key: str, 
            body_json: Dict[str, str], mode: str = "transferred"
        ) -> None:
        """ Convenience function which sends the given list of PathDetails 
        objects to the exchange with the given routing key and message body. 
        Mode specifies what to put into the log message, as well as determining 
        whether the list should be retry-reset and whether the message should be 
        delayed.

        NOTE: this will be refactored into RabbitMQConsumer as a very similar 
        function exists on IndexerConsumer.

        """
        self.log(f"Sending {mode} list to exchange", self.RK_LOG_INFO)

        # TODO: might be worth using an enum here?
        # Reset the retries upon successful indexing. 
        if mode == "transferred":
            for path_details in transferlist:
                path_details.reset_retries()
        elif mode == "retry":
            # Delay the retry message depending on how many retries have been 
            # accumulated. All retries in a retry list _should_ be the same so 
            # base it off of the first one.
            delay = self.get_retry_delay(transferlist[0].retries)
            self.log(f"Adding {delay / 1000}s delay to retry. Should be sent at"
                     f" {datetime.now() + timedelta(milliseconds=delay)}", 
                     self.RK_LOG_DEBUG)
        
        body_json[self.MSG_DATA][self.MSG_FILELIST] = transferlist
        self.publish_message(routing_key, json.dumps(body_json), delay=delay)
    
def main():
    consumer = PutTransferConsumer()
    consumer.run()

if __name__ == "__main__":
    main()