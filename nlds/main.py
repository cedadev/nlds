# encoding: utf-8
"""
main.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from fastapi import FastAPI

from .nlds_setup import API_VERSION

from .routers import list, files, probe, status, find, meta, system, info, init

nlds = FastAPI()

PREFIX = "/api/" + API_VERSION
nlds.include_router(
    meta.router,
    tags=[
        "meta",
    ],
    prefix=PREFIX + "/catalog/meta",
)
nlds.include_router(
    list.router,
    tags=[
        "list",
    ],
    prefix=PREFIX + "/catalog/list",
)
nlds.include_router(
    find.router,
    tags=[
        "find",
    ],
    prefix=PREFIX + "/catalog/find",
)
nlds.include_router(
    status.router,
    tags=[
        "status",
    ],
    prefix=PREFIX + "/status",
)
nlds.include_router(
    files.router,
    tags=[
        "files",
    ],
    prefix=PREFIX + "/files",
)
nlds.include_router(
    probe.router,
    tags=[
        "probe",
    ],
    prefix="/probe",
)
nlds.include_router(
    system.router,
    tags=[
        "system",
    ],
    prefix="/system",
)
nlds.include_router(
    info.router,
    tags=[
        "info",
    ],
    prefix="/info",
)
nlds.include_router(
    init.router,
    tags=[
        "init",
    ],
    prefix=PREFIX + "/init",
)
