from typing import List, Dict
from retry import retry
from minio.error import S3Error

from nlds.rabbit.consumer import State
from nlds.details import PathDetails

from nlds_processors.archiver.archive_base import (
    BaseArchiveConsumer, ArchiveError
)
                                                   
class DelArchiveConsumer(BaseArchiveConsumer):
    DEFAULT_QUEUE_NAME = "archive_del_q"
    DEFAULT_ROUTING_KEY = (f"{BaseArchiveConsumer.RK_ROOT}."
                           f"{BaseArchiveConsumer.RK_ARCHIVE_DEL}."
                           f"{BaseArchiveConsumer.RK_WILD}")
    DEFAULT_STATE = State.ARCHIVE_DELETING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)


    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, tape_url: str, filelist: List[PathDetails], 
                 rk_origin: str, body_json: Dict[str, str]):
        """All the transfer currently does is pass the filelist onto the 
        del_transfer consumer to delete from the Object Storage."""
        for f in filelist:
            print(f)


def main():
    consumer = DelArchiveConsumer()
    consumer.run()

if __name__ == "__main__":
    main()