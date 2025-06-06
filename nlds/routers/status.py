# encoding: utf-8
"""
status.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional, List, Dict
import uuid
import json

from fastapi import Depends, APIRouter, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import nlds.rabbit.message_keys as MSG
import nlds.rabbit.routing_keys as RK
from nlds.rabbit.consumer import State
from nlds.routers import rpc_publisher
from nlds.errors import ResponseError
from nlds.authenticators.authenticate_methods import (
    authenticate_token,
    authenticate_group,
    authenticate_user,
)

router = APIRouter()


class StatusResponse(BaseModel):
    holdings: List[Dict]

class StatBody(BaseModel):
    api_action: Optional[List[str]] = None
    exclude_api_action: Optional[List[str]] = None
    state: Optional[List[str]] = None

############################ GET METHOD ############################
@router.get(
    "/",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": StatusResponse},
        status.HTTP_400_BAD_REQUEST: {"model": ResponseError},
        status.HTTP_401_UNAUTHORIZED: {"model": ResponseError},
        status.HTTP_403_FORBIDDEN: {"model": ResponseError},
        status.HTTP_404_NOT_FOUND: {"model": ResponseError},
        status.HTTP_504_GATEWAY_TIMEOUT: {"model": ResponseError},
    },
)
async def get(
    token: str = Depends(authenticate_token),
    user: str = Depends(authenticate_user),
    group: str = Depends(authenticate_group),
    groupall: Optional[bool] = False,
    id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    job_label: Optional[str] = None,
    sub_id: Optional[str] = None,
    regex: Optional[bool] = None,
    limit: Optional[int] = None,
    descending: Optional[bool] = None,
    stat_options: Optional[StatBody] = None,
):
    # create the message dictionary
    search_api_action = stat_options.api_action
    api_action = f"{RK.STAT}"   # this is overwriting the api_action we want to
                                # filter on, so we have saved it above - we will
                                # put it in the META section of the message

    # Validate state at this point.
    states = []
    if stat_options.state is not None:
        for s in stat_options.state:
            # Attempt to convert to int, if can't then put in upper case for name
            # comparison
            try:
                state = int(s)
            except (ValueError, TypeError):
                state = s.upper()

            if State.has_name(s):
                state = State[s].value
            elif State.has_value(s):
                state = State(s).value
            else:
                response_error = ResponseError(
                    loc=["status", "get"],
                    msg=f"Given State: {s} is not valid.",
                    type="Incomplete request.",
                )
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail=response_error.json()
                )
            states.append(state)

    # Validate transaction_id is a valid uuid
    if transaction_id is not None:
        try:
            uuid.UUID(transaction_id)
        except ValueError:
            response_error = ResponseError(
                loc=["status", "get"],
                msg=f"Given transaction_id: {transaction_id} is not a valid uuid-4.",
                type="Incomplete request.",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=response_error.json()
            )
    # Validate sub_id is a valid uuid
    if sub_id is not None:
        try:
            uuid.UUID(sub_id)
        except ValueError:
            response_error = ResponseError(
                loc=["status", "get"],
                msg=f"Given sub_id: {sub_id} is not a valid uuid-4.",
                type="Incomplete request.",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=response_error.json()
            )
    # Assemble message ready for RCP call
    msg_dict = {
        MSG.DETAILS: {
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.GROUPALL: groupall,
            MSG.ID: id,
            MSG.API_ACTION: api_action,
            MSG.TRANSACT_ID: transaction_id,
            MSG.JOB_LABEL: job_label,
            MSG.SUB_ID: sub_id,
            MSG.API_ACTION: api_action,
        },
        MSG.DATA: {},
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    meta_dict = {}
    if regex:
        meta_dict[MSG.REGEX] = True
    if search_api_action:
        meta_dict[MSG.API_ACTION] = search_api_action
    if stat_options.exclude_api_action:
        meta_dict[MSG.EXCLUDE_API_ACTION] = stat_options.exclude_api_action
    if len(states) > 0:
        meta_dict[MSG.STATE] = states
    if limit:
        meta_dict[MSG.LIMIT] = limit
    if descending:
        meta_dict[MSG.DESCENDING] = descending
    # this should appear last
    if len(meta_dict) > 0:
        msg_dict[MSG.META] = meta_dict

    # call RPC function
    routing_key = "monitor_q"
    response = await rpc_publisher.call(msg_dict=msg_dict, routing_key=routing_key)
    # Check if response is valid or whether the request timed out
    if response is not None:
        # convert byte response to dict for label fetching
        response_dict = json.loads(response)
        # Attempt to get list of transaction records
        transaction_records = None
        try:
            transaction_records = response_dict[MSG.DATA][MSG.RECORD_LIST]
        except KeyError as e:
            print(
                f"Encountered error when trying to get a record list from the"
                f" message response ({e})"
            )

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
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response)
    else:
        response_error = ResponseError(
            loc=["status", "get"],
            msg="Monitoring service could not complete the request in time. "
            "This could be due to high database load. Consider restricting your "
            "request by using the <id>, <limit>, <api_method>, <exclude_api_method>, "
            "or <state> options.",
            type="Request timed out.",
        )
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail=response_error.json()
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
