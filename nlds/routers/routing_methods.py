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
from typing import List

# Create a publisher and start its connection
rabbit_publisher = RabbitMQPublisher()
rabbit_publisher.get_connection()

# NOTE: is this even necessary anymore? Has become a big wrapper around
#   create_message. Could we move this to main.py and import the publisher into 
#   routers.files and routers.collections as necessary?

def rabbit_publish_response(routing_key: str, transaction_id: UUID, user: str, 
                            group: str, data: List[str], access_key: str, 
                            secret_key: str, target: str = None,
                            holding_transaction_id: str = None, 
                            tenancy: str = None):
    msg = rabbit_publisher.create_message(
        transaction_id, 
        data, 
        access_key, 
        secret_key, 
        user=user, 
        group=group, 
        target=target, 
        tenancy=tenancy, 
        holding_transaction_id=holding_transaction_id
    )
    rabbit_publisher.publish_message(routing_key, msg)

