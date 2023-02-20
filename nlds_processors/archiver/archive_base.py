# encoding: utf-8
"""

"""
__author__ = 'Jack Leland and Neil Massey'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from abc import ABC, abstractmethod
import json
import os
from typing import List, NamedTuple, Dict
import pathlib as pth

from nlds.rabbit.consumer import RabbitMQConsumer
from nlds.rabbit.publisher import RabbitMQPublisher as RMQP
from nlds.details import PathDetails

class BaseArchiveConsumer(RabbitMQConsumer, ABC):
    DEFAULT_QUEUE_NAME = "archive_q"
    DEFAULT_ROUTING_KEY = (f"{RMQP.RK_ROOT}.{RMQP.RK_ARCHIVE}.{RMQP.RK_WILD}")
    DEFAULT_REROUTING_INFO = f"->{DEFAULT_QUEUE_NAME.upper()}"

    _TAPE_POOL = 'tape_pool'
    _TAPE_URL = 'tape_url'
    _MAX_RETRIES = 'max_retries'
    _PRINT_TRACEBACKS = 'print_tracebacks_fl'
    DEFAULT_CONSUMER_CONFIG = {
        _TAPE_POOL: None,
        _TAPE_URL: True,
        _PRINT_TRACEBACKS: False,
        _MAX_RETRIES: 5,
        RMQP.RETRY_DELAYS: RMQP.DEFAULT_RETRY_DELAYS,
    }

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.tape_pool = self.load_config_value(
            self._TAPE_POOL)
        self.tape_url = self.load_config_value(
            self._TAPE_URL)
        self.max_retries = self.load_config_value(
            self._MAX_RETRIES)
        self.print_tracebacks_fl = self.load_config_value(
            self._PRINT_TRACEBACKS)
        self.retry_delays = self.load_config_value(
            self.RETRY_DELAYS)

        self.reset()

    def callback(self, ch, method, properties, body, connection):
        self.reset()