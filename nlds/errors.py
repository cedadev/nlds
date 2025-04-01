# encoding: utf-8
"""
errors.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from pydantic import BaseModel
from typing import List

"""Error class"""


class ResponseError(BaseModel):
    loc: List[str]
    msg: str
    type: str


class RabbitRetryError(BaseException):

    def __init__(self, *args: object, ampq_exception: Exception = None) -> None:
        super().__init__(*args)
        self.ampq_exception = ampq_exception


class CallbackError(BaseException):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class MessageError(Exception):
    def __init__(self, message, *args):
        super().__init__(args)
        self.message = message

    def __str__(self):
        return self.message