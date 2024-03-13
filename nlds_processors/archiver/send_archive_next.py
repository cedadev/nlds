from uuid import uuid4
import json

import click

from nlds.routers import rabbit_publisher
from nlds.rabbit.publisher import RabbitMQPublisher as RMQP
from nlds.rabbit.consumer import State
from nlds.details import Retries

@click.command()
def send_archive_next():
    CRONJOB_CONFIG_SECTION = "cronjob_publisher"
    DEFAULT_CONFIG = {
        RMQP.MSG_ACCESS_KEY: None,
        RMQP.MSG_SECRET_KEY: None,
        RMQP.MSG_TAPE_URL: None,
        RMQP.MSG_TENANCY: None,
    }
    # Load any cronjob config, if present
    cronjob_config = DEFAULT_CONFIG
    if CRONJOB_CONFIG_SECTION in rabbit_publisher.whole_config:
        cronjob_config |= rabbit_publisher.whole_config[CRONJOB_CONFIG_SECTION]
    
    msg_dict = {
        RMQP.MSG_DETAILS: {
            RMQP.MSG_TRANSACT_ID: str(uuid4()),
            RMQP.MSG_SUB_ID: str(uuid4()),
            RMQP.MSG_USER: "admin-placeholder",
            RMQP.MSG_GROUP: "admin-placeholder",
            RMQP.MSG_TARGET: None,
            RMQP.MSG_API_ACTION: "archive-put",
            RMQP.MSG_JOB_LABEL: "archive-next",
            RMQP.MSG_STATE: State.ARCHIVE_INIT.value,
            **cronjob_config,
        }, 
        RMQP.MSG_DATA: {
            # Convert to PathDetails for JSON serialisation
            RMQP.MSG_FILELIST: [],
        }, 
        **Retries().to_dict(),
        RMQP.MSG_META: {
            # Insert an empty meta dict
        },
        RMQP.MSG_TYPE: RMQP.MSG_TYPE_STANDARD,
    }
    routing_key = f'{RMQP.RK_ROOT}.{RMQP.RK_CATALOG_ARCHIVE_NEXT}.{RMQP.RK_START}'

    print(f"Sending message to {routing_key}: \n{json.dumps(msg_dict, indent=4)}")
    rabbit_publisher.publish_message(routing_key, msg_dict)
    print("Message sent!")

if __name__ == '__main__':
    send_archive_next()

