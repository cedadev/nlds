from typing import List, NamedTuple
import os

import minio
from minio.error import S3Error
from retry import retry

from .base_transfer import BaseTransferConsumer


class GetTransferConsumer(BaseTransferConsumer):
    DEFAULT_QUEUE_NAME = "transfer_get_q"
    DEFAULT_ROUTING_KEY = (f"{BaseTransferConsumer.RK_ROOT}."
                           f"{BaseTransferConsumer.RK_TRANSFER_GET}."
                           f"{BaseTransferConsumer.RK_WILD}")

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
    
    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, filelist: List[NamedTuple]):
        raise NotImplementedError()

def main():
    consumer = GetTransferConsumer()
    consumer.run()

if __name__ == "__main__":
    main()