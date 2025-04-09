""" """

from fastapi import Depends, APIRouter, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import json

from typing import Optional, List, Dict

from ..rabbit.publisher import RabbitMQPublisher as RMQP
from ..routers import rpc_publisher
from ..errors import ResponseError
from ..authenticators.authenticate_methods import (
    authenticate_token,
    authenticate_group,
    authenticate_user,
)

from ..utils.process_tag import process_tag


router = APIRouter()


class QuotaResponse(BaseModel):
    quota: int


############################ GET METHOD ############################
@router.get(
    "/",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": QuotaResponse},
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
    label: Optional[str] = None,
    holding_id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    tag: Optional[str] = None,
):
    # create the message dictionary

    routing_key = f"{RMQP.RK_ROOT}.{RMQP.RK_ROUTE}.{RMQP.RK_QUOTA}"
    api_action = f"{RMQP.RK_QUOTA}"
    msg_dict = {
        RMQP.MSG_DETAILS: {
            RMQP.MSG_USER: user,
            RMQP.MSG_GROUP: group,
            RMQP.MSG_TOKEN: token,
            RMQP.MSG_API_ACTION: api_action,
        },
        RMQP.MSG_DATA: {},
        RMQP.MSG_TYPE: RMQP.MSG_TYPE_STANDARD,
    }
    # add the metadata
    meta_dict = {}
    if label:
        meta_dict[RMQP.MSG_LABEL] = label
    if holding_id:
        meta_dict[RMQP.MSG_HOLDING_ID] = holding_id
    if transaction_id:
        meta_dict[RMQP.MSG_TRANSACT_ID] = transaction_id

    if tag:
        tag_dict = {}
        # convert the string into a dictionary
        try:
            tag_dict = process_tag(tag)
        except ValueError:
            response_error = ResponseError(
                loc=["quota", "get"],
                msg="tag cannot be processed.",
                type="Incomplete request.",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=response_error.json()
            )
        else:
            meta_dict[RMQP.MSG_TAG] = tag_dict

    if len(meta_dict) > 0:
        msg_dict[RMQP.MSG_META] = meta_dict

    # Call RPC function
    routing_key = "catalog_q"
    response = await rpc_publisher.call(msg_dict=msg_dict, routing_key=routing_key)
    # Check if response is valid or whether the request timed out
    if response is not None:
        # convert byte response to str
        response = response.decode()

        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response)
    else:
        response_error = ResponseError(
            loc=["status", "get"],
            msg="Catalog service could not be reached in time.",
            type="Incomplete request.",
        )
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail=response_error.json()
        )
