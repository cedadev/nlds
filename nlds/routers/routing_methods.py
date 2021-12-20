# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from ..rabbit.publisher import RabbitMQPublisher
from uuid import UUID

rabbit_publisher = RabbitMQPublisher()

def rabbit_publish_response(routing_key: str, transaction_id: UUID, user: str, 
                            group: str, data: str, target: str = None):
    rabbit_publisher.get_connection()
    msg = rabbit_publisher.create_message(transaction_id, data, user=user, 
                                          group=group, target=target)
    rabbit_publisher.publish_message(routing_key, msg)

