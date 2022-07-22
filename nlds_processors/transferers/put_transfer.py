from typing import List, NamedTuple
import os

import minio
from minio.error import S3Error
from retry import retry

from .base_transfer import BaseTransferConsumer


class PutTransferConsumer(BaseTransferConsumer):
    DEFAULT_QUEUE_NAME = "transfer_put_q"
    DEFAULT_ROUTING_KEY = (f"{BaseTransferConsumer.RK_ROOT}."
                           f"{BaseTransferConsumer.RK_TRANSFER_PUT}."
                           f"{BaseTransferConsumer.RK_WILD}")

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, filelist: List[NamedTuple]):
        client = minio.Minio(
            tenancy,
            access_key=access_key,
            secret_key=secret_key,
            secure=self.require_secure_fl,
        )  

        bucket_name = f"{self.RK_ROOT}.{transaction_id}"

        # Check that bucket exists, and create if not
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            self.log(f"Creating bucket ({bucket_name}) for this"
                     " transaction", self.RK_LOG_INFO)
        else:
            self.log(f"Bucket for this transaction ({transaction_id}) "
                     f"already exists", self.RK_LOG_INFO)

        for indexitem in filelist:
            # If check_permissions active then check again that file exists and 
            # is accessible. 
            if self.check_permissions_fl and not os.access(indexitem.item, 
                                                           os.R_OK):
                self.log("File is inaccessible :(", self.RK_LOG_WARNING)
                # Do failed_list, retry_list stuff
                return

            self.log(f"Attempting to upload file {indexitem.item}", 
                     self.RK_LOG_DEBUG)
            
            # The minio client doesn't like file paths starting at root (i.e. 
            # with a slash at the very beginning) so it needs to be stripped if 
            # using minio, and added back on get.
            # TODO: Add this flag to the file details json metadata 
            if self.remove_root_slash_fl and indexitem.item[0] == "/":
                object_name = indexitem.item[1:]
            else:
                object_name = indexitem.item
            
            result = client.fput_object(
                bucket_name, object_name, indexitem.item,
            )
            self.log(f"Successfully uploaded {indexitem.item}", 
                     self.RK_LOG_DEBUG)

def main():
    consumer = PutTransferConsumer()
    consumer.run()

if __name__ == "__main__":
    main()