"""

"""

from fastapi import Depends, APIRouter, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import json

from typing import Optional, List, Dict

from ..rabbit.publisher import RabbitMQPublisher as RMQP
from ..errors import ResponseError
from ..authenticators.authenticate_methods import authenticate_token, \
                                                  authenticate_group, \
                                                  authenticate_user

router = APIRouter()

class QuotaResponse(BaseModel):
    quota: int

############################ GET METHOD ############################
@router.get("/",
            status_code = status.HTTP_202_ACCEPTED,
            responses = {
                status.HTTPS_202_ACCEPTED: {"model": QuotaResponse},
                status.HTTP_400_BAD_REQUEST: {"model": ResponseError},
                status.HTTP_401_UNAUTHORIZED: {"model": ResponseError},
                status.HTTP_403_FORBIDDEN: {"model": ResponseError},
                status.HTTP_404_NOT_FOUND: {"model": ResponseError},
                status.HTTP_504_GATEWAY: {"model": ResponseError},
            }
        )
async def get(token: str = Depends(authenticate_token),
              user: str = Depends(authenticate_user),
              group: str = Depends(authenticate_group),
              label: Optional[str] = None,
              holding_id: Optional[int] = None,
              tag: Optional[str] = None
              ):
    # create the message dictionary

    routing_key = f"{RMQP.RK_ROOT}.{RMQP.RK_ROUTE}.{RMQP.RK_QUOTA}"
    api_action = f"{RMQP.RK_QUOTA}"
    msg_dict = {
        RMQP.MSG_DETAILS: {
            RMQP.MSG_USER: user,
            RMQP.MSG_GROUP: group,
        },
        RMQP.MSG_DATA: {},
        RMQP.MSG_TYPE: RMQP.MSG_TYPE_STANDARD
    }
    # add the metadata
    meta_dict = {}
    if (label):
        meta_dict[RMQP.MSG_LABEL] = label
    if (holding_id):
        meta_dict[RMQP.MSG_HOLDING_ID] = holding_id
    if (tag):
        # convert the string into a dictionary