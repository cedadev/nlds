from fastapi import FastAPI

from .nlds_setup import API_VERSION

from .routers import collections, files
from .routers.routing_methods import rabbit_connection

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
