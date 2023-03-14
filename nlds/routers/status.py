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
import json

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
                status.HTTP_404_NOT_FOUND: {"model" : ResponseError},
                status.HTTP_504_GATEWAY_TIMEOUT: {"model": ResponseError},
            }
        )
async def get(token: str = Depends(authenticate_token),
              user: str = Depends(authenticate_user),
              group: str = Depends(authenticate_group),
              id: Optional[int] = None,
              transaction_id: Optional[str] = None,
              job_label: Optional[str] = None,
              state: Optional[Union[int, str]] = None,
              sub_id: Optional[str] = None,
              retry_count: Optional[int] = None,
              api_action: Optional[str] = None,
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
        # Attempt to convert to int, if can't then put in upper case for name 
        # comparison
        try: 
            state = int(state)
        except (ValueError, TypeError):
            state = state.upper()

        if State.has_name(state):
            state = State[state].value
        elif State.has_value(state):
            state = State(state).value
        else:
            response_error = ResponseError(
                    loc = ["status", "get"],
                    msg = "Given State not valid.",
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
                msg = "Given transaction_id not a valid uuid-4.",
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
                msg = "Given sub_id not a valid uuid-4.",
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
            RMQP.MSG_ID: id,
            RMQP.MSG_API_ACTION: api_action,
            RMQP.MSG_TRANSACT_ID: transaction_id,
            RMQP.MSG_JOB_LABEL: job_label,
            RMQP.MSG_STATE: state,
            RMQP.MSG_SUB_ID: sub_id,
            RMQP.MSG_RETRY_COUNT_QUERY: retry_count,
            RMQP.MSG_USER_QUERY: user,
            RMQP.MSG_GROUP_QUERY: group,
            RMQP.MSG_API_ACTION: api_action,
        },
        RMQP.MSG_DATA: {},
        RMQP.MSG_TYPE: RMQP.MSG_TYPE_STANDARD
    }

    # call RPC function
    routing_key = "monitor_q"
    response = await rpc_publisher.call(
        msg_dict=msg_dict, routing_key=routing_key
    )
    # Check if response is valid or whether the request timed out
    if response is not None:
        # convert byte response to dict for label fetching
        response_dict = json.loads(response)
        # Attempt to get list of transaction records
        transaction_records = None
        try: 
            transaction_records = response_dict[RMQP.MSG_DATA][RMQP.MSG_RECORD_LIST]
        except KeyError as e:
            print(f"Encountered error when trying to get a record list from the"
                  f" message response ({e})")
        
        transaction_response = None
        # Only continue if the response actually had any transactions in it
        if transaction_records is not None and len(transaction_records) > 0:
            routing_key = "catalog_q"
            transaction_response = await rpc_publisher.call(
                msg_dict=response_dict, routing_key=routing_key
            )

        if transaction_response is not None:
            response = transaction_response

        # convert byte response to str
        response = response.decode()
        return JSONResponse(status_code = status.HTTP_202_ACCEPTED,
                            content = response)
    else:
        response_error = ResponseError(
            loc = ["status", "get"],
            msg = "Monitoring service could not be reached in time.",
            type = "Request timed out."
        )
        raise HTTPException(
            status_code = status.HTTP_504_GATEWAY_TIMEOUT,
            detail = response_error.json()
        )

# @router.put("/")
# async def put():
#     return {}

# @router.post("/")
# async def post():
#     return {}

# @router.delete("/")
# async def delete():
#     return {}
