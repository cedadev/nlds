# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from fastapi import Depends, APIRouter, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import json

from typing import Optional

from ..rabbit.publisher import RabbitMQPublisher as RMQP
from .routing_methods import rabbit_publish_response
from ..errors import ResponseError
from ..rabbit.rpc_publisher import RabbitRPCClient
from ..authenticators.authenticate_methods import authenticate_token, \
                                                  authenticate_group, \
                                                  authenticate_user

router = APIRouter()

rpc_client = RabbitRPCClient()
rpc_client.get_connection()

class HoldingResponse(BaseModel):
    msg: str

############################ GET METHOD ############################
@router.get("/",
            status_code = status.HTTP_202_ACCEPTED,
            responses = {
                status.HTTP_202_ACCEPTED: {"model" : HoldingResponse},
                status.HTTP_400_BAD_REQUEST: {"model" : ResponseError},
                status.HTTP_401_UNAUTHORIZED: {"model" : ResponseError},
                status.HTTP_403_FORBIDDEN: {"model" : ResponseError},
                status.HTTP_404_NOT_FOUND: {"model" : ResponseError}
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
        tag_dict = {}
        # convert the string into a dictionary
        try:
            # strip whitespace and "{" "}" symbolsfirst
            tag_list = (tag.replace(" ","").replace("{", "").replace("}", "")
                       ).split(",")
            for tag_i in tag_list:
                tag_kv = tag_i.split(":")
                tag_dict[tag_kv[0]] = tag_kv[1]
        except: # what exception might be raised here?
            response_error = ResponseError(
                loc = ["holdings", "get"],
                msg = "tag cannot be processed.",
                type = "Incomplete request."
            )
            raise HTTPException(
                status_code = status.HTTP_400_BAD_REQUEST,
                detail = response_error.json()
            )
        else:
            meta_dict[RMQP.MSG_TAG] = tag_dict
    if (len(meta_dict) > 0):
        msg_dict[RMQP.MSG_META] = meta_dict

    response = HoldingResponse(msg="Holding:get accepted")
    # response = await rpc_client.call(msg=json.dumps(msg_dict),
    #                                  queue="catalog_rpc_q")

    rabbit_publish_response(f"{RMQP.RK_ROOT}.{RMQP.RK_ROUTE}.{RMQP.RK_LIST}",
                            msg_dict)

    return JSONResponse(status_code = status.HTTP_202_ACCEPTED,
                        content = response.json())

@router.put("/")
async def put():
    return {}

@router.post("/")
async def post():
    return {}

@router.delete("/")
async def delete():
    return {}
