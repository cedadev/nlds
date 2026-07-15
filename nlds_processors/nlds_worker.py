# encoding: utf-8
"""
nlds_worker.py
"""

__author__ = "Neil Massey and Jack Leland"
__date__ = "07 Dec 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import List, Dict, Tuple

# Typing imports
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

# NLDS imports
from nlds.rabbit.consumer import RabbitMQConsumer as RMQC
from nlds.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds.rabbit.state import State
import nlds.rabbit.routing_keys as RK
import nlds.rabbit.message_keys as MSG
import asyncio


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
        self.rpc_publisher = RabbitMQRPCPublisher()
        self.rpc_publisher.get_connection()

    def _process_message(
        self, method: Method, body: bytes, properties: Header
    ) -> Tuple[List, Dict]:
        """Process the message to get the routing key parts, message data
        and message details"""

        body_json = self._deserialize(body)

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

    def _process_rk_put(self, body_json: Dict[str, str]) -> None:
        self.log(f"Sending put command to be indexed", RK.LOG_INFO)

        # create the holding
        new_routing_key = ".".join([RK.ROOT, RK.CATALOG_SETUP, RK.START])
        self.publish_and_log_message(new_routing_key, body_json)

        # Initialise the monitoring record to ensure that a subrecord at ROUTING is
        # created before the first job
        self.log(f"Initialising monitor", RK.LOG_INFO)
        new_routing_key = ".".join([RK.ROOT, RK.MONITOR_PUT, RK.INITIATE])
        body_json[MSG.DETAILS][MSG.STATE] = State.ROUTING.value
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_get(self, body_json: Dict[str, str]) -> None:
        # forward to catalog_get
        queue = f"{RK.CATALOG_GET}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

        # Initialise the monitoring record to ensure that a subrecord at ROUTING is
        # created before the first job
        self.log(f"Initialising monitor", RK.LOG_INFO)
        new_routing_key = ".".join([RK.ROOT, RK.MONITOR_PUT, RK.INITIATE])
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

    def _process_rk_catalog_setup_complete(self, body_json: Dict[str, str]) -> None:
        # create the bucket
        new_routing_key = ".".join([RK.ROOT, RK.TRANSFER_SETUP, RK.START])
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_transfer_setup_complete(self, body_json: Dict[str, str]) -> None:
        # start the indexing
        new_routing_key = ".".join([RK.ROOT, RK.INDEX, RK.INITIATE])
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
        new_routing_key = ".".join([RK.ROOT, queue, RK.INITIATE])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_transfer_put_complete(self, body_json: Dict) -> None:
        # After a successful TRANSFER_PUT, the catalog is updated with the locations
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

    def _process_rk_transfer_get_complete(
        self, rk_parts: List[str], body_json: Dict
    ) -> None:
        # After a successful TRANSFER_GET, the sub records in the Monitor need to be
        # notified that they have complete

        self.send_complete(rk_parts, body_json)

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
        queue = f"{RK.MONITOR_PUT}"
        self.log(f"Sending message to {queue} queue", RK.LOG_INFO)
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
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
        # After a successful ARCHIVE_GET, the catalog is updated with the locations
        # of the files on the OBJECT STORAGE
        self.log(
            f"Archive get successful, sending filelist with object storage locations "
            "to be modified in the catalog",
            RK.LOG_INFO,
        )
        queue = f"{RK.CATALOG_UPDATE}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_catalog_update_complete(
        self, rk_parts: List, body_json: Dict
    ) -> None:
        try:
            api_method = body_json[MSG.DETAILS][MSG.API_ACTION]
            # For the GET and GETLIST method, this code path occurs after the files have
            # been fetched from Tape.  They are now passed to TRANSFER_GET, which is
            # the same code path as after a CATALOG_GET, i.e. the files are now fetched
            # from the Object Store
            if api_method == RK.GET or api_method == RK.GETLIST:
                self._process_rk_catalog_get_complete(rk_parts, body_json)
            # For the PUT and PUTLIST method, this is the final state - i.e. the catalog
            # is updated to contain the new Object Store path
            elif api_method == RK.PUT or api_method == RK.PUTLIST:
                self.send_complete(rk_parts, body_json)

        except KeyError:
            self.log(
                f"Message did not contain an appropriate api_action.",
                RK.LOG_ERROR,
            )

    def _process_rk_catalog_delete_complete(
        self, rk_parts: List, body_json: Dict
    ) -> None:
        # after a catalog delete (which removes failed files from the catalog) we need
        # to indicate to the monitor that the deletion has completed
        # however (and this is a bit weird) - we need to indicate that it FAILED.
        # all files in this message will have a failure_reason and, for the monitor to
        # tag this as a failed sub-id, we need to send a FAILED message
        self.send_failed(rk_parts, body_json)

    def _process_rk_catalog_get_archive_restore(
        self, rk_parts: List, body_json: Dict
    ) -> None:
        # forward confirmation to monitor
        self.log(f"Sending message to {RK.MONITOR} queue", RK.LOG_INFO)
        new_routing_key = ".".join([RK.ROOT, RK.MONITOR_PUT, RK.START])
        self.publish_and_log_message(new_routing_key, body_json)

        # forward to archive_get - use PREPARE rather than INITIATE for two reasons:
        # 1. We don't want and splitting to take place, as the messages are already
        #    sub-divided on aggregate and we want to pull the whole aggregate back from
        #    tape in a single call. (we don't want to pull the aggregate back multiple
        #    times, which is what would happen if we split the messages here)
        # 2. We might (probably will) need to prepare aggregates on the tape system.
        #    this involves fetching the aggregate from tape and staging it on the small
        #    amount of cache that the tape system has.  This obviously takes time so,
        #    after the prepare call to XrootD, a PREPARE_CHECK message is queued that
        #    will poll the tape system to determine if the aggregate has been staged
        #    yet.  After that, the ARCHIVE_GET.START message is queued.

        queue = RK.ARCHIVE_GET
        new_routing_key = ".".join([RK.ROOT, queue, RK.PREPARE])
        self.log(
            f"Sending  message to {queue} queue with routing key " f"{new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_archive_get_failed(self, body_json: Dict) -> None:
        self.log(
            "Archive retrieval unsuccessful, sending failed files back to "
            "catalog for objectstore-location deletion",
            RK.LOG_INFO,
        )

        queue = f"{RK.CATALOG_REMOVE}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

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
        new_routing_key = ".".join([RK.ROOT, queue, RK.INITIATE])
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
        # forward confirmation to monitor
        self.send_complete(rk_parts, body_json)

    def _process_rk_archive_put_failed(self, body_json: Dict) -> None:
        self.log(
            f"Archive-put unsuccessful, sending back to catalog to mark "
            "aggregation as failed.",
            RK.LOG_INFO,
        )

        queue = f"{RK.CATALOG_REMOVE}"
        new_routing_key = ".".join([RK.ROOT, queue, RK.START])
        self.log(
            f"Sending  message to {queue} queue with routing key {new_routing_key}",
            RK.LOG_INFO,
        )
        self.publish_and_log_message(new_routing_key, body_json)

    def _process_rk_cancel(self, body_json: Dict, properties: Header) -> None:
        """Cancel a transaction"""
        error = None
        try:
            api_method = body_json[MSG.DETAILS][MSG.API_ACTION]
        except KeyError:
            error = f"Message did not contain an appropriate api_action. "
        if api_method != RK.CANCEL:
            error = (
                f"Calling RPC in NLDS consumer with API method: {api_method} is "
                "not supported."
            )
        # return any error using the RPC reply
        if error:
            return_json = body_json
            return_json[MSG.DETAILS][MSG.FAILURE] = error
        else:
            queue = RK.MONITOR_PUT
            new_routing_key = RK.MONITOR_Q
            self.log(
                f"Sending message to {queue} queue with routing key {new_routing_key}",
                RK.LOG_INFO,
            )
            # call the cancel method in the monitor
            function = self.rpc_publisher.call(
                msg_dict=body_json, routing_key=new_routing_key
            )
            response_monitor = asyncio.run(function)
            return_monitor_json = self._deserialize(response_monitor)
            # set the return_json to be that was returned by the monitor.  It might be
            # overwritten by that returned by the catalog.
            return_json = return_monitor_json

            # check if the delete of the transaction succeeded
            # if it did then we also want to delete the holding with that transaction
            # id from the catalog - but only if the api_method was PUT or PUTLIST!
            if not MSG.FAILURE in return_monitor_json[MSG.DETAILS]:
                if MSG.API_ACTION in return_monitor_json[
                    MSG.DETAILS
                ] and return_monitor_json[MSG.DETAILS][MSG.API_ACTION] in [
                    RK.PUT,
                    RK.PUTLIST,
                ]:
                    # Note about the API_ACTION key:
                    # 1. It is needed above to determine whether an attempt is made to
                    #    delete the holding, if the original action was PUT or
                    #    PUTLIST. We don't want to delete holdings if the method is
                    #    ARCHIVE_PUT or GET or GETLIST, etc. So, it is returned as the
                    #    API_ACTION in the return_monitor_json message
                    # 2. The catalog processor needs the API_ACTION to be set back to
                    #    CANCEL so that it can call the method to delete the holding if
                    #    necessary,
                    queue = RK.CATALOG_PUT
                    new_routing_key = RK.CATALOG_Q
                    return_monitor_json[MSG.DETAILS][MSG.API_ACTION] = RK.CANCEL

                    self.log(
                        f"Sending message to {queue} queue with routing key "
                        f"{new_routing_key}",
                        RK.LOG_INFO,
                    )
                    # call the cancel method in the monitor
                    function = self.rpc_publisher.call(
                        msg_dict=return_monitor_json, routing_key=new_routing_key
                    )
                    response_catalog = asyncio.run(function)
                    return_catalog_json = self._deserialize(response_catalog)
                    if not MSG.FAILURE in return_catalog_json[MSG.DETAILS]:
                        return_json = return_catalog_json
            # restore the cancel API ACTION
            return_json[MSG.DETAILS][MSG.API_ACTION] = RK.CANCEL

        # publish the RPC return message
        self.publish_message(
            properties.reply_to,
            msg_dict=return_json,
            exchange={"name": ""},
            correlation_id=properties.correlation_id,
        )

    def callback(
        self,
        ch: Channel,
        method: Method,
        properties: Header,
        body: bytes,
        connection: Connection,
    ) -> None:

        # Convert body from bytes to string for ease of manipulation
        body_json = self._deserialize(body)

        self.log(
            f"Received with routing_key: {method.routing_key}",
            RK.LOG_INFO,
            body_json=body_json,
        )

        # If received system test message, reply to it (this is for system status check)
        if self._is_system_status_check(body_json=body_json, properties=properties):
            return

        # RPC method first - the routing key is just "nlds"
        if method.routing_key == RK.NLDS_Q:
            self._process_rk_cancel(body_json=body_json, properties=properties)
        else:
            rk_parts, body_json = self._process_message(method, body, properties)

            # special cases first
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

                # If transfer_get completed then finish get workflow
                elif rk_parts[1] == f"{RK.TRANSFER_GET}":
                    self._process_rk_transfer_get_complete(
                        rk_parts=rk_parts, body_json=body_json
                    )

                # If transfer_setup completed then start the indexing
                elif rk_parts[1] == f"{RK.TRANSFER_SETUP}":
                    self._process_rk_transfer_setup_complete(body_json)

                # if catalog_get completed then we need to decide whether it was
                # part of a regular get or an archive_put workflow
                elif rk_parts[1] == f"{RK.CATALOG_GET}":
                    self._process_rk_catalog_get_complete(rk_parts, body_json)

                elif rk_parts[1] == f"{RK.CATALOG_PUT}":
                    self._process_rk_catalog_put_complete(rk_parts, body_json)

                # If finished with archive retrieval then pass for catalog-update
                elif rk_parts[1] == f"{RK.ARCHIVE_GET}":
                    self._process_rk_archive_get_complete(rk_parts, body_json)

                # If finished with aggregation of unarchived holding, then send for
                # archive write
                elif rk_parts[1] == f"{RK.CATALOG_ARCHIVE_NEXT}":
                    self._process_rk_catalog_archive_next_complete(rk_parts, body_json)

                # If finished with archive write, then pass checksum info to catalog
                elif rk_parts[1] == f"{RK.ARCHIVE_PUT}":
                    self._process_rk_archive_put_complete(rk_parts, body_json)

                # If finished with catalog setup (create holding) then pass to indexing
                elif rk_parts[1] == f"{RK.CATALOG_SETUP}":
                    self._process_rk_catalog_setup_complete(body_json)

                # If finished with catalog update then pass for transfer get
                elif rk_parts[1] == f"{RK.CATALOG_UPDATE}":
                    self._process_rk_catalog_update_complete(rk_parts, body_json)
                # If finished with catalog delete then mark as completed
                elif rk_parts[1] == f"{RK.CATALOG_DEL}":
                    self._process_rk_catalog_delete_complete(rk_parts, body_json)
                # if finished with catalog archive update then mark ARCHIVE_PUT flow as
                # complete
                elif rk_parts[1] == f"{RK.CATALOG_ARCHIVE_UPDATE}":
                    self._process_rk_catalog_archive_update_complete(
                        rk_parts, body_json
                    )

            # If a archive-restore has happened from the catalog then we need to get from
            # archive before we can do the transfer from object store.
            elif rk_parts[2] == f"{RK.ARCHIVE_RESTORE}":
                self._process_rk_catalog_get_archive_restore(rk_parts, body_json)

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

        self.log(f"Worker callback complete!", RK.LOG_DEBUG)

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
