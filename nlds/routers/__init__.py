# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from ..rabbit.publisher import RabbitMQPublisher
from ..rabbit.rpc_publisher import RabbitMQRPCPublisher

# Create a publisher and start its connection
rabbit_publisher = RabbitMQPublisher()
rabbit_publisher.get_connection()

# Create a RPC publisher and start its connection
rpc_publisher = RabbitMQRPCPublisher()
rpc_publisher.get_connection()