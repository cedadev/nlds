# encoding: utf-8
"""
keepalive.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from uuid import uuid4
import threading as thr
from typing import List
import time

from pika.connection import Connection


class KeepaliveDaemon:
    """Class for orchestrating a connection keepalive daemon thread."""

    def __init__(self, connection: Connection, heartbeat: int):
        self.name = uuid4()
        self.heartbeat = heartbeat
        self.connection = connection
        self.poll_event = thr.Event()
        self.kill_event = thr.Event()
        self.keepalive = None

    @staticmethod
    def get_thread_names() -> List[str]:
        return list(thr.name for thr in thr.enumerate())

    def start_polling(self):
        self.poll_event.set()

    def stop_polling(self):
        self.poll_event.clear()

    def start(self) -> None:
        # Create a keepalive daemon thread named after the uuid of this object
        if self.name not in self.get_thread_names():
            # Start a deamon thread which processes data events in the
            # background
            self.poll_event.clear()
            self.kill_event.clear()
            self.keepalive = thr.Thread(
                name=self.name,
                target=self.run,
                # args=(self.connection, self.heartbeat,
                #       self.poll_event, self.kill_event),
                daemon=True,
            )
            self.keepalive.start()

    def kill(self):
        if self.name in self.get_thread_names():
            self.kill_event.set()
            # self.keepalive.join()

    def run(self):
        """Simple infinite loop which keeps the connection alive by calling
        process_data_events() during consumption. This is intended to be run in
        the background as a daemon thread, can be exited immediately at main
        thread exit. Needs to be passed the active connection object and the
        required heartbeats.
        """
        # While we have an open connection continue the process
        while self.connection.is_open and not self.kill_event.is_set():
            # If we're actively consuming and the connection is blocked, then
            # periodically call process_data_events to keep the connection open.
            if self.poll_event.is_set():
                self.connection.process_data_events()
            time.sleep(max(self.heartbeat / 2, 1))
