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
        # the file might not even be on tape!
        # we just want to pass it back to the exchange in this case
        super().__init__(queue=queue)


    @retry(S3Error, tries=5, delay=1, logger=None)
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, tape_url: str, filelist: List[PathDetails], 
                 rk_origin: str, body_json: Dict[str, str]):
        """All the transfer currently does is pass the filelist onto the 
        del_transfer consumer to delete from the Object Storage.
        TODO - write some code in here that deletes from tape"""
        # the various lists
        completelist = []
        retrylist = []
        failedlist = []

        for path_details in filelist:
            if path_details.tape_url:
                print("Delete from tape here", path_details)
                # try / except here, adding to failed and retry lists
                try:
                    raise ArchiveError
                except ArchiveError:
                    # add to retry list and check if it's greater than max retries
                    reason = "Testing the retry"
                    path_details.retries.increment(reason=reason)
                    if path_details.retries.count > self.max_retries:
                        failedlist.append(path_details)
                    else:
                        retrylist.append(path_details)
                else:
                    completelist.append(path_details)
            else:
                print("Delete from object store", path_details)
                # add to successful files
                completelist.append(path_details)

        rk_complete = ".".join([rk_origin, self.RK_ARCHIVE_DEL, self.RK_COMPLETE])
        rk_retry = ".".join([rk_origin, self.RK_ARCHIVE_DEL, self.RK_START])
        rk_failed = ".".join([rk_origin, self.RK_ARCHIVE_DEL, self.RK_FAILED])

        if len(completelist) > 0:
            self.send_pathlist(
                completelist, rk_complete, body_json,
                state=State.ARCHIVE_DELETING
            )

        if len(retrylist) > 0:
            self.send_pathlist(
                retrylist, rk_retry, body_json, mode="retry"
            )

        if len(failedlist) > 0:
            # Send message back to worker so failed deletions can be inserted back into the catalog 
            self.send_pathlist(
                failedlist, rk_failed, body_json, 
                state=State.CATALOG_DELETE_ROLLBACK,
            )



def main():
    consumer = DelArchiveConsumer()
    consumer.run()

if __name__ == "__main__":
    main()