import json
import traceback
import os

import minio
from minio.error import S3Error

from nlds.rabbit.consumer import RabbitMQConsumer

class PutTransferConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "transfer_q"
    DEFAULT_ROUTING_KEY = (f"{RabbitMQConsumer.RK_ROOT}."
                           f"{RabbitMQConsumer.RK_TRANSFER}."
                           f"{RabbitMQConsumer.RK_WILD}")

    _TENANCY = "tenancy_url"
    _REQUIRE_SECURE = "require_secure_fl"
    _CHECK_PERMISSIONS = "check_permissions_fl"
    DEFAULT_CONSUMER_CONFIG = {
        _TENANCY: None,
        _REQUIRE_SECURE: True,
        _CHECK_PERMISSIONS: True,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.tenancy = self.load_config_value(self._TENANCY_URL)
        self.require_secure_fl = self.load_config_value(self._REQUIRE_SECURE)
        self.check_permissions_fl = self.load_config_value(
            self._CHECK_PERMISSIONS
        )
        
    def callback(self, ch, method, properties, body, connection):
        try:
            # Convert body from bytes to string for ease of manipulation
            body = body.decode("utf-8")
            body_json = json.loads(body)

            self.log(f"Received {body} from {self.queues[0].name} "
                    f"({method.routing_key})", 
                    self.RK_LOG_DEBUG)

            ### 
            # Verify and load message contents 

            try: 
                transaction_id = body_json[self.MSG_DETAILS][self.MSG_TRANSACT_ID]
            except KeyError:
                self.log(
                    "Transaction id unobtainable, exiting callback.", 
                    self.RK_LOG_ERROR
                )
                return

            filelist = self.parse_filelist(body_json)

            # TODO: (2022-07-13) Should we be checking filelist length here and 
            # reindexing / resizing the list if too long?
            filelist_len = len(filelist)

            try:
                access_key = body_json[self.MSG_DETAILS][self.MSG_ACCESS_KEY]
                secret_key = body_json[self.MSG_DETAILS][self.MSG_SECRET_KEY]
            except KeyError:
                self.log(
                    "Secret key or access key unobtainable, exiting callback.", 
                    self.RK_LOG_ERROR
                )
                return

            # If tenancy specified in message details then override the server-
            # config value
            if self.MSG_TENANCY in body_json[self.MSG_DETAILS]:
                self.tenancy = body_json[self.MSG_DETAILS][self.MSG_TENANCY]
            elif self.tenancy is None:
                self.log(
                    "No tenancy specified at server- or request-level, exiting "
                    "callback.", 
                    self.RK_LOG_ERROR
                )
                return 

            client = minio.Minio(
                self.tenancy,
                access_key=access_key,
                secret_key=secret_key,
                secure=self.require_secure_fl,
            )   

            bucket_name = f"{self.RK_ROOT}.{transaction_id}"

            # Check that bucket exists
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                self.log(f"Creating bucket ({bucket_name}) for this"
                         " transaction", self.RK_LOG_INFO)
            else:
                self.log(f"Bucket for this transaction ({transaction_id}) "
                         f"already exists", self.RK_LOG_INFO)

            for indexitem in filelist:
                if self.check_permissions_fl and not os.access(indexitem.item, os.R_OK):
                    print("file is inaccessible :(")
            # Loop through filelist
                # check file still exists and we still have permissions
                # check object not already in the bucket
                # move file to object store
        
        except (ValueError, TypeError, KeyError, PermissionError, S3Error) as e:
            if self.print_tracebacks:
                tb = traceback.format_exc()
                self.log(tb, self.RK_LOG_DEBUG)
            self.log(
                f"Encountered error ({e}), sending to logger.", 
                self.RK_LOG_ERROR, exc_info=e
            )
            body_json[self.MSG_DATA][self.MSG_ERROR] = str(e)
            new_routing_key = ".".join(
                [self.RK_ROOT, self.RK_LOG, self.RK_LOG_INFO]
            )
            self.publish_message(new_routing_key, json.dumps(body_json))


def main():
    consumer = PutTransferConsumer()
    consumer.run()

if __name__ == "__main__":
    main()