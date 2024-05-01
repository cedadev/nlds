# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

"""Authentication functions for use by the routers."""
from fastapi import Depends, status
from fastapi.security import OAuth2PasswordBearer
from .jasmin_authenticator import JasminAuthenticator as Authenticator
from ..errors import ResponseError
from fastapi.exceptions import HTTPException

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="", auto_error=False)
authenticator = Authenticator()

async def authenticate_token(token: str = Depends(oauth2_scheme)):
    """Check the token by calling the authenticator's authenticate_token
    method."""
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
