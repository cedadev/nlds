# encoding: utf-8
"""
probe.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jan 2022"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import logging

from fastapi import APIRouter, status

router = APIRouter()


@router.get("/healthz", status_code=status.HTTP_200_OK)
async def kubernetes_liveness_probe():
    """
    Liveness probe for kubernetes status service
    """
    return {"status", "healthy"}


# Filter to prevent logs getting filled with probe requests
class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage().find("/healthz") == -1


# Filter out /endpoint
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())
