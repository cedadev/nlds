# encoding: utf-8
"""
meta.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "16 Nov 2022"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from fastapi import Depends, APIRouter, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from typing import Optional, List, Dict

import nlds.rabbit.message_keys as MSG
import nlds.rabbit.routing_keys as RK
from nlds.routers import rpc_publisher
from nlds.errors import ResponseError
from nlds.authenticators.authenticate_methods import (
    authenticate_token,
    authenticate_group,
    authenticate_user,
)
from nlds.utils.process_tag import process_tag

router = APIRouter()


class MetaModel(BaseModel):
    new_label: str = None
    new_tag: Dict[str, str] = None
    del_tag: Dict[str, str] = None


class MetaResponse(BaseModel):
    files: List[Dict]


############################ POST METHOD ############################
@router.post(
    "/",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": MetaResponse},
        status.HTTP_400_BAD_REQUEST: {"model": ResponseError},
        status.HTTP_401_UNAUTHORIZED: {"model": ResponseError},
        status.HTTP_403_FORBIDDEN: {"model": ResponseError},
        status.HTTP_404_NOT_FOUND: {"model": ResponseError},
        status.HTTP_504_GATEWAY_TIMEOUT: {"model": ResponseError},
    },
)
async def post(
    metamodel: MetaModel,
    token: str = Depends(authenticate_token),
    user: str = Depends(authenticate_user),
    group: str = Depends(authenticate_group),
    label: Optional[str] = None,
    holding_id: Optional[int] = None,
    tag: Optional[str] = None,
):
    # create the message dictionary

    routing_key = f"{RK.ROOT}.{RK.ROUTE}.{RK.META}"
    api_action = f"{RK.META}"
    msg_dict = {
        MSG.DETAILS: {MSG.USER: user, MSG.GROUP: group, MSG.API_ACTION: api_action},
        MSG.DATA: {},
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    # add the metadata
    meta_dict = {}
    if label:
        meta_dict[MSG.LABEL] = label
    if holding_id:
        meta_dict[MSG.HOLDING_ID] = holding_id
    if tag:
        # convert the string into a dictionary
        try:
            tag_dict = process_tag(tag)
        except ValueError:
            response_error = ResponseError(
                loc=["meta", "post"],
                msg="tag cannot be processed.",
                type="Incomplete request.",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=response_error.json()
            )
        else:
            meta_dict[MSG.TAG] = tag_dict

    # create the "new_meta" section
    new_meta_dict = {}
    if metamodel:
        if metamodel.new_label:
            new_meta_dict[MSG.LABEL] = metamodel.new_label
        if metamodel.new_tag:
            new_meta_dict[MSG.TAG] = metamodel.new_tag
        if metamodel.del_tag:
            new_meta_dict[MSG.DEL_TAG] = metamodel.del_tag

    # add the "meta" section
    if len(meta_dict) > 0:
        msg_dict[MSG.META] = meta_dict
        # add the "new_meta" section
        if len(new_meta_dict) > 0:
            msg_dict[MSG.META][MSG.NEW_META] = new_meta_dict

    # call RPC function
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
