# encoding: utf-8
"""
nlds_worker.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "07 Dec 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import json
from typing import List, Dict, Tuple

# Typing imports
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

# NLDS imports
from nlds.rabbit.consumer import RabbitMQConsumer as RMQC
from nlds.rabbit.state import State
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG


class NLDSWorkerConsumer(RMQC):
    DEFAULT_QUEUE_NAME = "nlds_q"
    DEFAULT_ROUTING_KEY = (
        f"{RK.ROOT}.",
        f"{RK.ROUTE}.",
        f"{RK.WILD}",
    )
    DEFAULT_REROUTING_INFO = "->NLDS_Q"
    DEFAULT_STATE = State.ROUTING

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

    def _process_message(
        self, method: Method, body: bytes, properties: Header
    ) -> Tuple[List, Dict]:
        """Process the message to get the routing key parts, message data
        and message details"""

        body_json = json.loads(body)

        self.log(
            f"Appending rerouting information to message: "
            f"{self.DEFAULT_REROUTING_INFO} ",
            RK.LOG_DEBUG,
        )
        body_json = self.append_route_info(body_json)

        # Check the routing key is a valid, 3-piece key
        self.log(f"Checking routing_key: {method.routing_key}", RK.LOG_DEBUG)
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError:
            self.log(
                "Routing key inappropriate length, exiting callback.", RK.LOG_ERROR
            )
            return

        # Minimum verification of message contents - checking the two major
        # sections (details and data) are present
        for msg_section in (MSG.DATA, MSG.DETAILS):
            if msg_section not in body_json:
                self.log(
                    f"Invalid message received - missing {msg_section} "
                    f"section. \nExiting callback",
                    RK.LOG_ERROR,
                )
                return

        return rk_parts, body_json

    def _process_rk_put(self, body_json: Dict) -> None:
        self.log(f"Sending put command to be indexed", RK.LOG_INFO)

        new_routing_key = ".".join([RK.ROOT, RK.INDEX, RK.INITIATE])
        self.publish_and_log_message(new_routing_key, body_json)

        # Do initial monitoring update to ensure that a subrecord at ROUTING is
        # created before the first job
        self.log(f"Updating monitor", RK.LOG_INFO)
        new_routing_key = ".".join([RK.ROOT, RK.MONITOR_PUT, RK.START])
        body_json[MSG.DETAILS][MSG.STATE] = State.ROUTING.value
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_get(self, body_json: Dict[str, str]) -> None:
        # forward to catalog_get
        queue = f"{RK.CATALOG_GET}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

        # Do initial monitoring update to ensure that a subrecord at ROUTING is
        # created before the first job
        self.log(f"Updating monitor", RK.LOG_INFO)
        new_routing_key = ".".join([RK.ROOT, RK.MONITOR_PUT, RK.START])
        body_json[MSG.DETAILS][MSG.STATE] = State.ROUTING.value
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_list(self, body_json: Dict) -> None:
        # forward to catalog_get
        queue = f"{RK.CATALOG_GET}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.LIST])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_index_complete(self, body_json: Dict[str, str]) -> None:
        # forward to catalog-put on the catalog_q
        self.log(f"Index successful, sending file list for cataloguing.", RK.LOG_INFO)

        queue = f"{RK.CATALOG_PUT}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_catalog_put_complete(self, body_json: Dict) -> None:
        self.log(f"Catalog successful, sending filelist for transfer", RK.LOG_INFO)

        queue = f"{RK.TRANSFER_PUT}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_transfer_put_complete(self, body_json: Dict) -> None:
        # After a successfull TRANSFER_PUT, the catalog is updated with the locations
        # of the files on the OBJECT STORAGE
        self.log(
            f"Transfer successful, sending filelist with object storage locations to "
            "be inserted into the catalog",
            RK.LOG_INFO,
        )
        queue = f"{RK.CATALOG_UPDATE}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_transfer_put_failed(self, body_json: Dict) -> None:
        self.log(
            f"Transfer unsuccessful, sending failed files back to catalog "
            "for deletion",
            RK.LOG_INFO,
        )

        queue = f"{RK.CATALOG_DEL}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_catalog_get_complete(self, rk_parts: List, body_json: Dict) -> None:
        # forward confirmation to monitor
        self.log(f"Sending message to {RK.MONITOR} queue", RK.LOG_INFO)
        new_routing_key = ".".join([RK.ROOT, RK.MONITOR_PUT, RK.START])
        self.publish_and_log_message(new_routing_key, body_json)

        # forward to transfer_get
        queue = f"{RK.TRANSFER_GET}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.INITIATE])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_archive_get_complete(self, rk_parts: List, body_json: Dict) -> None:
        # Can simply call the same process used at catalog_get complete
        self._process_rk_catalog_get_complete(rk_parts, body_json)

    def _process_rk_catalog_get_reroute_archive(
        self, rk_parts: List, body_json: Dict
    ) -> None:
        # forward confirmation to monitor
        self.log(f"Sending message to {RK.MONITOR} queue", RK.LOG_INFO)
        new_routing_key = ".".join([RK.ROOT, RK.MONITOR_PUT, RK.START])
        self.publish_and_log_message(new_routing_key, body_json)

        # forward to archive_get
        queue = f"{RK.ARCHIVE_GET}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_archive_get_failed(self, body_json: Dict) -> None:
        self.log(
            "Archive retrieval unsuccessful, sending failed files back to "
            "catalog for objectstore-location deletion",
            RK.LOG_INFO,
        )

        queue = f"{RK.CATALOG_ARCHIVE_DEL}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    # def _process_rk_archive_put(self, rk_parts: List, body_json: Dict) -> None:
    #     # forward to catalog_get
    #     queue = f"{RK.CATALOG_GET}"
    #     new_routing_key = ".".join([RK.ROOT,
    #                                 queue,
    #                                 RK.START])
    #     self.log(f"Sending  message to {queue} queue with routing "
    #              f"key {new_routing_key}", RK.LOG_INFO)
    #     self.publish_and_log_message(new_routing_key, body_json)

    #     # Do initial monitoring update to ensure that a subrecord at ROUTING is
    #     # created before the first job
    #     self.log(f"Updating monitor", RK.LOG_INFO)
    #     new_routing_key = ".".join([RK.ROOT,
    #                                 RK.MONITOR_PUT,
    #                                 RK.START])
    #     body_json[MSG.DETAILS][MSG.STATE] = State.ROUTING.value
    #     self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_catalog_archive_next_complete(
        self, rk_parts: List, body_json: Dict
    ) -> None:
        self.log(
            f"Next archivable holding aggregated, sending aggregations "
            f"for archive-write",
            RK.LOG_INFO,
        )

        # Do initial monitoring update. NOTE: unclear whether this is necessary
        # as the entry point for the next message hasn't been decided yet, so
        # the monitoring entry may be created earlier
        self.log(f"Sending message to {RK.MONITOR} queue", RK.LOG_INFO)
        new_routing_key = ".".join([RK.ROOT, RK.MONITOR_PUT, RK.START])
        self.publish_and_log_message(new_routing_key, body_json)

        queue = f"{RK.ARCHIVE_PUT}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_archive_put_complete(self, rk_parts: List, body_json: Dict) -> None:
        self.log(
            "Aggregation successfully written to tape, sending checksum "
            "info back to catalog",
            RK.LOG_INFO,
        )

        # forward to catalog to update the checksum on the location
        queue = f"{RK.CATALOG_ARCHIVE_UPDATE}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_catalog_archive_update_complete(
        self, rk_parts: List, body_json: Dict
    ) -> None:
        self.log(
            "Checksum successfully updated for aggregation, archive-put "
            "is now complete.",
            RK.LOG_INFO,
        )

        # forward confirmation to monitor
        self.log(f"Sending message to {RK.MONITOR} queue", RK.LOG_INFO)
        new_routing_key = ".".join([RK.ROOT, RK.MONITOR_PUT, RK.START])
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_archive_put_failed(self, body_json: Dict) -> None:
        self.log(
            f"Archive-put unsuccessful, sending back to catalog to mark "
            "aggregation as failed.",
            RK.LOG_INFO,
        )

        queue = f"{RK.CATALOG_ARCHIVE_REMOVE}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def callback(
        self,
        ch: Channel,
        method: Method,
        properties: Header,
        body: bytes,
        connection: Connection,
    ) -> None:

        # Convert body from bytes to string for ease of manipulation
        body_json = json.loads(body)

        self.log(
            f"Received {json.dumps(body_json, indent=4)} \nwith "
            f"routing_key: {method.routing_key}",
            RK.LOG_INFO,
        )

        # If received system test message, reply to it (this is for system status check)
        if self._is_system_status_check(body_json=body_json, properties=properties):
            return

        rk_parts, body_json = self._process_message(method, body, properties)

        # If putting then first scan file/filelist
        if rk_parts[2] in (RK.PUT, RK.PUTLIST):
            self._process_rk_put(body_json)

        elif rk_parts[2] in (RK.GET, RK.GETLIST):
            self._process_rk_get(body_json)

        # If a task has completed, initiate new tasks
        elif rk_parts[2] == f"{RK.COMPLETE}":
            # If index completed then pass file list cataloguing before transfer
            if rk_parts[1] == f"{RK.INDEX}":
                self._process_rk_index_complete(body_json)

            # if catalog_put completed send for transfer
            elif rk_parts[1] == f"{RK.CATALOG_PUT}":
                self._process_rk_catalog_put_complete(body_json)

            # If transfer_put completed then finish put workflow
            elif rk_parts[1] == f"{RK.TRANSFER_PUT}":
                self._process_rk_transfer_put_complete(body_json)

            # if catalog_get completed then we need to decide whether it was
            # part of a regular get or an archive_put workflow
            elif rk_parts[1] == f"{RK.CATALOG_GET}":
                self._process_rk_catalog_get_complete(rk_parts, body_json)

            # If finished with archive retrieval then pass for transfer-get
            elif rk_parts[1] == f"{RK.ARCHIVE_GET}":
                self._process_rk_archive_get_complete(rk_parts, body_json)

            # If finished with aggregation of unarchived holding, then send for
            # archive write
            elif rk_parts[1] == RK.CATALOG_ARCHIVE_NEXT:
                self._process_rk_catalog_archive_next_complete(rk_parts, body_json)
            # If finished with archive write, then pass checksum info to catalog
            elif rk_parts[1] == f"{RK.ARCHIVE_PUT}":
                self._process_rk_archive_put_complete(rk_parts, body_json)

        # If a reroute has happened from the catalog then we need to get from
        # archive before we can do the transfer from object store.
        elif rk_parts[2] == f"{RK.REROUTE_ARCHIVE}":
            self._process_rk_catalog_get_reroute_archive(rk_parts, body_json)

        # If a transfer/archive task has failed, remove something from the
        # catalog
        elif rk_parts[2] == f"{RK.FAILED}":
            # If transfer_put failed then we need to remove the failed files
            # from the catalog
            if rk_parts[1] == f"{RK.TRANSFER_PUT}":
                self._process_rk_transfer_put_failed(body_json)

            # If archive_put failed then we need to remove the TAPE locations
            # from the catalog
            elif rk_parts[1] == f"{RK.ARCHIVE_PUT}":
                self._process_rk_archive_put_failed(body_json)

            # If archive_get failed then we need to remove the OBJECT_STORAGE
            # locations from the catalog
            elif rk_parts[1] == f"{RK.ARCHIVE_GET}":
                self._process_rk_archive_get_failed(body_json)

        self.log(f"Worker callback complete!", RK.LOG_INFO)

    def publish_and_log_message(self, routing_key: str, msg: dict, log_fl=True) -> None:
        """
        Wrapper around original publish message to additionally send message to
        logging queue. Useful for debugging purposes to be able to see the
        content being managed by the worker.
        """
        self.publish_message(routing_key, msg)

        if log_fl:
            # Additionally send same message to logging with debug priority.
            self.log(msg, RK.LOG_DEBUG)


def main():
    consumer = NLDSWorkerConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
