# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

"""Authentication functions for use by the routers."""
from typing import Optional, Dict

from fastapi import Depends, status
from fastapi.security import OAuth2PasswordBearer,OAuth2
from starlette.requests import Request
from .jasmin_authenticator import JasminAuthenticator as Authenticator
from ..errors import ResponseError
from fastapi.exceptions import HTTPException
from fastapi.openapi.models import OAuthFlows

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="", auto_error=False)
authenticator = Authenticator()

class OAuth2MockBearer(OAuth2):
    def __init__(
        self,
        tokenUrl: str,
        scheme_name: Optional[str] = None,
        scopes: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
        auto_error: bool = True,
    ):
        if not scopes:
            scopes = {}
        flows = OAuthFlows(password={"tokenUrl": tokenUrl, "scopes": scopes})
        super().__init__(
            flows=flows,
            scheme_name=scheme_name,
            description=description,
            auto_error=auto_error,
        )

    async def __call__(self, request: Request) -> Optional[str]:
        authorization: str = request.headers.get("Authorization")
        scheme, _, param = authorization.partition(" ")
        print(f"From within oauth_scheme: {scheme}, {param}")
        return request

oauth2_mock = OAuth2MockBearer(tokenUrl="", auto_error=False)
 

async def authenticate_token(
        token: str = Depends(oauth2_scheme), 
        request: Request = Depends(oauth2_mock)
    ):
    """Check the token by calling the authenticator's authenticate_token
    method."""
    print(f"token: {token}")
    print(request.headers.__dict__)
    if token is None:
        response_error = ResponseError(
            loc = ["authenticate_methods", "authenticate_token"],
            msg = f"OAuth token not supplied.",
            type = "Forbidden."
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=response_error.json()
        )
    elif not authenticator.authenticate_token(token):
            response_error = ResponseError(
                loc = ["authenticate_methods", "authenticate_token"],
                msg = f"OAuth token invalid or could not be found.",
                type = "Resource not found."
            )
            raise HTTPException(
                status_code = status.HTTP_401_UNAUTHORIZED,
                detail = response_error.json()
            )
    else:
        return token

# add return type
async def authenticate_user(user: str, token: str = Depends(oauth2_scheme)):
    """Check the user by calling the authenticator's authenticate_user
    method."""
    print(f"user token: {token}")
    if token is None:
        response_error = ResponseError(
            loc = ["authenticate_methods", "authenticate_user"],
            msg = f"OAuth token not supplied.",
            type = "Forbidden."
        )
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN,
            detail = response_error.json()
        )
    elif not authenticator.authenticate_user(token, user):
        response_error = ResponseError(
            loc = ["authenticate_methods", "authenticate_user"],
            msg = f"User {user} could not be found.",
            type = "Forbidden."
        )
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = response_error.json()
        )
    return user


async def authenticate_group(group: str, token: str = Depends(oauth2_scheme)):
    """Check the group by calling the authenticator's authenticate_user
    method."""
    print(f"group token: {token}")
    if token is None:
        response_error = ResponseError(
            loc = ["authenticate_methods", "authenticate_group"],
            msg = "OAuth token not supplied.",
            type = "Forbidden."
        )
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN,
            detail = response_error.json()
        )
    elif not authenticator.authenticate_group(token, group):
        response_error = ResponseError(
            loc = ["authenticate_methods", "authenticate_group"],
            msg = f"User is not a member of the group {group}.",
            type = "Resource not found."
        )
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = response_error.json()
        )
    return group
