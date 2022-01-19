# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from fastapi import FastAPI

from .nlds_setup import API_VERSION

from .routers import collections, files, probe
from .routers.routing_methods import rabbit_publisher

nlds = FastAPI()

PREFIX = "/api/" + API_VERSION

nlds.include_router(
    collections.router,
    tags = ["collections",],
    prefix = PREFIX + "/collections"
)
nlds.include_router(
    files.router,
    tags = ["files",],
    prefix = PREFIX + "/files"
)
nlds.include_router(
    probe.router,
    tags=["probe", ],
    prefix = "/probe"
)
