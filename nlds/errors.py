from pydantic import BaseModel
from typing import List, Optional

"""Error class"""
class ResponseError(BaseModel):
    loc: List[str]
    msg: str
    type: str
