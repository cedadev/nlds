# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from ..rabbit.publisher import RabbitMQPublisher as RMQP
from uuid import UUID
from typing import List
import json
from datetime import datetime

# Create a publisher and start its connection
rabbit_publisher = RMQP()
rabbit_publisher.get_connection()

# NRM 26/10/

def rabbit_publish_response(routing_key: str, msg_dict):
    # add the timestamp
    msg_dict[RMQP.MSG_TIMESTAMP] = datetime.now().isoformat(sep='-'),
    msg = json.dumps(msg_dict)
    rabbit_publisher.publish_message(routing_key, msg)