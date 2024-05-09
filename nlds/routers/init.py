# encoding: utf-8
"""

"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import json
import os
from base64 import b64encode
from cryptography.fernet import Fernet

from fastapi import APIRouter, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from nlds.errors import ResponseError

router = APIRouter()


class InitResponse(BaseModel):
    encrypted_keys: bytes


class TokenRepsonse(BaseModel):
    token: str


############################ GET METHOD ############################
@router.get(
    "/",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": InitResponse},
        status.HTTP_404_NOT_FOUND: {"model": ResponseError},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ResponseError},
        status.HTTP_504_GATEWAY_TIMEOUT: {"model": ResponseError},
    },
)
async def get():
    try:
        oauth_id = os.environ["OAUTH_ID"]
        oauth_secret = os.environ["OAUTH_SECRET"]
        oauth_token_url = os.environ["OAUTH_TOKEN_URL"]
        oauth_scopes = os.environ["OAUTH_SCOPES"]
        enc_key = os.environ["SYM_KEY"].encode()
    except KeyError as e:
        response_error = ResponseError(
            loc=["init", "get"], msg="Unable to get api keys.", type="Failed request."
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=response_error.model_dump_json(),
        )

    api_info_bytes = json.dumps(
        {
            "oauth_client_id": oauth_id,
            "oauth_client_secret": oauth_secret,
            "oauth_token_url": oauth_token_url,
            "oauth_scopes": oauth_scopes,
        }
    ).encode()

    f = Fernet(enc_key)
    encrypted = f.encrypt(api_info_bytes)

    response = InitResponse(encrypted_keys=encrypted)
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.json())


############################ GET METHOD ############################
@router.get(
    "/token",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": InitResponse},
        status.HTTP_404_NOT_FOUND: {"model": ResponseError},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ResponseError},
        status.HTTP_504_GATEWAY_TIMEOUT: {"model": ResponseError},
    },
)
async def get():
    try:
        enc_key = os.environ["SYM_KEY"].encode()
    except KeyError as e:
        response_error = ResponseError(
            loc=["init/token", "get"],
            msg=f"Unable to get token {e}.",
            type="Failed request.",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=response_error.model_dump_json(),
        )

    encoded_token = b64encode(enc_key)

    response = TokenRepsonse(token=encoded_token)
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.json())
