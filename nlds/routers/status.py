# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from typing import Optional, List, Dict, Union
import uuid

from fastapi import Depends, APIRouter, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..rabbit.publisher import RabbitMQPublisher as RMQP
from ..rabbit.consumer import State
from . import rpc_publisher
from ..errors import ResponseError
from ..authenticators.authenticate_methods import authenticate_token, \
                                                  authenticate_group, \
                                                  authenticate_user

router = APIRouter()

class StatusResponse(BaseModel):
    holdings: List[Dict]

############################ GET METHOD ############################
@router.get("/",
            status_code = status.HTTP_202_ACCEPTED,
            responses = {
                status.HTTP_202_ACCEPTED: {"model" : StatusResponse},
                status.HTTP_400_BAD_REQUEST: {"model" : ResponseError},
                status.HTTP_401_UNAUTHORIZED: {"model" : ResponseError},
                status.HTTP_403_FORBIDDEN: {"model" : ResponseError},
                status.HTTP_404_NOT_FOUND: {"model" : ResponseError}
            }
        )
async def get(token: str = Depends(authenticate_token),
              user: str = Depends(authenticate_user),
              group: str = Depends(authenticate_group),
              transaction_id: Optional[str] = None,
              state: Optional[Union[int, str]] = None,
              sub_id: Optional[str] = None,
              retry_count: Optional[int] = None,
              query_user: Optional[str] = None,
              query_group: Optional[str] = None,
              ):
    # create the message dictionary
    routing_key = f"{RMQP.RK_ROOT}.{RMQP.RK_ROUTE}.{RMQP.RK_STAT}"
    api_action = f"{RMQP.RK_STAT}"

    # logic for user/group query verification should go here. Do we want to 
    # prevent the querying of users other than themselves?

    # Validate state at this point.
    if state is not None:
        if State.has_value(state):
            state = State(state).value
        elif State.has_name(state):
            state = State[state].value
        else:
            response_error = ResponseError(
                    loc = ["status", "get"],
                    msg = "given State not valid.",
                    type = "Incomplete request."
                )
            raise HTTPException(
                status_code = status.HTTP_400_BAD_REQUEST,
                detail = response_error.json()
            )
    
    # Validate transaction_id is a valid uuid
    if transaction_id is not None:
        try:
            uuid.UUID(transaction_id)
        except ValueError:
            response_error = ResponseError(
                loc = ["status", "get"],
                msg = "given transaction_id not a valid uuid-4.",
                type = "Incomplete request."
            )
            raise HTTPException(
                status_code = status.HTTP_400_BAD_REQUEST,
                detail = response_error.json()
            )
    # Validate sub_id is a valid uuid
    if sub_id is not None:
        try:
            uuid.UUID(sub_id)
        except ValueError:
            response_error = ResponseError(
                loc = ["status", "get"],
                msg = "given sub_id not a valid uuid-4.",
                type = "Incomplete request."
            )
            raise HTTPException(
                status_code = status.HTTP_400_BAD_REQUEST,
                detail = response_error.json()
            )

    # Assemble message ready for RCP call
    msg_dict = {
        RMQP.MSG_DETAILS: {
            RMQP.MSG_USER: user,
            RMQP.MSG_GROUP: group,        
            RMQP.MSG_API_ACTION: api_action,
            RMQP.MSG_TRANSACT_ID: transaction_id,
            RMQP.MSG_STATE: state,
            RMQP.MSG_SUB_ID: sub_id,
            RMQP.MSG_RETRY: retry_count,
            RMQP.MSG_USER_QUERY: user,
            RMQP.MSG_GROUP_QUERY: group,
        },
        RMQP.MSG_DATA: {},
        RMQP.MSG_TYPE: RMQP.MSG_TYPE_STANDARD
    }

    # call RPC function
    routing_key = "monitor_q"
    response = await rpc_publisher.call(
        msg_dict=msg_dict, routing_key=routing_key
    )
    # convert byte response to str
    response = response.decode()

    return JSONResponse(status_code = status.HTTP_202_ACCEPTED,
                        content = response)

@router.put("/")
async def put():
    return {}

@router.post("/")
async def post():
    return {}

@router.delete("/")
async def delete():
    return {}