# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from pydantic import BaseModel
from typing import List, Optional

"""Error class"""
class ResponseError(BaseModel):
    loc: List[str]
    msg: str
    type: str

class RabbitRetryError(BaseException):
    
    def __init__(self, *args: object, ampq_exception: Exception = None) -> None:
        super().__init__(*args)
        self.ampq_exception = ampq_exception

class MidCallbackError(BaseException):
    
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
