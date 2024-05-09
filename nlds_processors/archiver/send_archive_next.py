from uuid import uuid4
import json

import click

from nlds.routers import rabbit_publisher
from nlds.rabbit.consumer import State
from nlds.details import Retries

import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


@click.command()
def send_archive_next():
    CRONJOB_CONFIG_SECTION = "cronjob_publisher"
    DEFAULT_CONFIG = {
        MSG.ACCESS_KEY: None,
        MSG.SECRET_KEY: None,
        MSG.TAPE_URL: None,
        MSG.TENANCY: None,
    }
    # Load any cronjob config, if present
    cronjob_config = DEFAULT_CONFIG
    if CRONJOB_CONFIG_SECTION in rabbit_publisher.whole_config:
        cronjob_config |= rabbit_publisher.whole_config[CRONJOB_CONFIG_SECTION]

    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: str(uuid4()),
            MSG.SUB_ID: str(uuid4()),
            MSG.USER: "admin-placeholder",
            MSG.GROUP: "admin-placeholder",
            MSG.TARGET: None,
            MSG.API_ACTION: "archive-put",
            MSG.JOB_LABEL: "archive-next",
            MSG.STATE: State.ARCHIVE_INIT.value,
            **cronjob_config,
        },
        MSG.DATA: {
            # Convert to PathDetails for JSON serialisation
            MSG.FILELIST: [],
        },
        **Retries().to_dict(),
        MSG.META: {
            # Insert an empty meta dict
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    routing_key = f"{RK.ROOT}.{RK.CATALOG_ARCHIVE_NEXT}.{RK.START}"

    print(f"Sending message to {routing_key}: \n{json.dumps(msg_dict, indent=4)}")
    rabbit_publisher.publish_message(routing_key, msg_dict)
    print("Message sent!")


if __name__ == "__main__":
    send_archive_next()
