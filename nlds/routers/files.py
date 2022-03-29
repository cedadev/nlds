# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from fastapi import Depends, APIRouter, status, Query
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from uuid import UUID
from typing import Optional, List

from ..rabbit.publisher import RabbitMQPublisher
from ..errors import ResponseError
from ..authenticators.authenticate_methods import authenticate_token, \
                                                  authenticate_group, \
                                                  authenticate_user
from .routing_methods import rabbit_publish_response

router = APIRouter()

# uuid (for testing)
# 3fa85f64-5717-4562-b3fc-2c963f66afa6

class FileList(BaseModel):
    filelist: List[str]

    class Config:
        schema_extra = {
            "example": {
                "filelist": [
                "/datacentre/archvol5/qb139/archive/spot-9693-piControl/r1i1p1f1/Amon/tasmin/gn/files/d20190628/tasmin_Amon_HadGEM3-GC31-LL_piControl_r1i1p1f1_gn_185001-194912.nc",
                "/datacentre/archvol5/qb139/archive/spot-9693-piControl/r1i1p1f1/Amon/tasmin/gn/files/d20190628/tasmin_Amon_HadGEM3-GC31-LL_piControl_r1i1p1f1_gn_195001-204912.nc",
                "/datacentre/archvol5/qb139/archive/spot-9693-piControl/r1i1p1f1/Amon/tasmin/gn/files/d20190628/tasmin_Amon_HadGEM3-GC31-LL_piControl_r1i1p1f1_gn_205001-214912.nc",
                "/datacentre/archvol5/qb139/archive/spot-9693-piControl/r1i1p1f1/Amon/tasmin/gn/files/d20190628/tasmin_Amon_HadGEM3-GC31-LL_piControl_r1i1p1f1_gn_215001-224912.nc",
                "/datacentre/archvol5/qb139/archive/spot-9693-piControl/r1i1p1f1/Amon/tasmin/gn/files/d20190628/tasmin_Amon_HadGEM3-GC31-LL_piControl_r1i1p1f1_gn_225001-234912.nc"
                ]
            }
        }

    def to_str(self) -> str:
        return "[" + ",".join([f for f in self.filelist]) + "]"
    
    def get_cleaned_list(self) -> List[str]:
        return [f.rstrip('\n') for f in self.filelist]


class FileResponse(BaseModel):
    uuid: UUID
    msg: str


############################ GET METHOD ############################
@router.get("/",
            status_code = status.HTTP_202_ACCEPTED,
            responses = {
                status.HTTP_202_ACCEPTED: {"model" : FileResponse},
                status.HTTP_400_BAD_REQUEST: {"model" : ResponseError},
                status.HTTP_401_UNAUTHORIZED: {"model" : ResponseError},
                status.HTTP_403_FORBIDDEN: {"model" : ResponseError},
                status.HTTP_404_NOT_FOUND: {"model" : ResponseError}
            }
        )
async def get(transaction_id: UUID,
              token: str = Depends(authenticate_token),
              user: str = Depends(authenticate_user),
              group: str = Depends(authenticate_group),
              filepath: str = None,
              tenancy: Optional[str] = None,
              access_key: str = "",
              secret_key: str = ""
              ):

    # validate filepath and filelist - one or the other has to exist
    if not filepath:
        response_error = ResponseError(
                loc = ["files", "get"],
                msg = "filepath not supplied.",
                type = "Incomplete request."
            )
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = response_error.json()
        )
    # return response, transaction id accepted for processing
    response = FileResponse(
        uuid = transaction_id,
        msg = (f"GET transaction with id {transaction_id} accepted for "
                "processing.")
    )
    contents = [filepath, ]
    rabbit_publish_response(
        f"{RabbitMQPublisher.RK_ROOT}.{RabbitMQPublisher.RK_ROUTE}."
        f"{RabbitMQPublisher.RK_GET}", 
        transaction_id, user, group, contents,
        tenancy, access_key, secret_key
    )

    return JSONResponse(status_code = status.HTTP_202_ACCEPTED,
                        content = response.json())


############################ GET LIST METHOD ########################
@router.put("/getlist",
            status_code = status.HTTP_202_ACCEPTED,
            responses = {
                status.HTTP_202_ACCEPTED: {"model" : FileResponse},
                status.HTTP_400_BAD_REQUEST: {"model" : ResponseError},
                status.HTTP_401_UNAUTHORIZED: {"model" : ResponseError},
                status.HTTP_403_FORBIDDEN: {"model" : ResponseError},
                status.HTTP_404_NOT_FOUND: {"model" : ResponseError}
            }
        )
async def put(transaction_id: UUID,
              filelist: FileList,
              token: str = Depends(authenticate_token),
              user: str = Depends(authenticate_user),
              group: str = Depends(authenticate_group),
              tenancy: Optional[str]=None,
              access_key: str="",
              secret_key: str=""
              ):

    # validate filepath and filelist - one or the other has to exist
    if not filelist:
        response_error = ResponseError(
                loc = ["files", "getlist"],
                msg = "filelist not supplied.",
                type = "Incomplete request."
            )
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = response_error.json()
        )
    # return response, transaction id accepted for processing
    response = FileResponse(
        uuid = transaction_id,
        msg = (f"GETLIST transaction with id {transaction_id} accepted for "
                "processing.")
    )
    rabbit_publish_response(
        f"{RabbitMQPublisher.RK_ROOT}.{RabbitMQPublisher.RK_ROUTE}"
        f".{RabbitMQPublisher.RK_GETLIST}", 
        transaction_id, user, group, filelist.get_cleaned_list(),
        tenancy, access_key, secret_key)

    return JSONResponse(status_code = status.HTTP_202_ACCEPTED,
                        content = response.json())


############################ PUT METHOD ############################
@router.put("/",
            status_code = status.HTTP_202_ACCEPTED,
            responses = {
                status.HTTP_202_ACCEPTED: {"model" : FileResponse},
                status.HTTP_400_BAD_REQUEST: {"model" : ResponseError},
                status.HTTP_401_UNAUTHORIZED: {"model" : ResponseError},
                status.HTTP_403_FORBIDDEN: {"model" : ResponseError},
                status.HTTP_404_NOT_FOUND: {"model" : ResponseError}
            }
        )
async def put(transaction_id: UUID,
              token: str=Depends(authenticate_token),
              user: str=Depends(authenticate_user),
              group: str=Depends(authenticate_group),
              filepath: Optional[str]=None,
              filelist: Optional[FileList]=None,
              tenancy: Optional[str]=None,
              access_key: str="",
              secret_key: str=""
            ):

    # validate filepath and filelist - one or the other has to exist
    if not filepath and not filelist:
        response_error = ResponseError(
            loc = ["files", "put"],
            msg = ("filepath and filelist are both empty.  Either (but not "
                   "both) must be supplied."),
            type = "Resources not found."
        )
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = response_error.json()
        )
    # validate filepath and filelist - only one must exist
    if filepath and filelist:
        response_error = ResponseError(
            loc = ["files", "put"],
            msg = ("filepath and filelist both exist.  Only one must be "
                   "supplied"),
            type = "Conflicting resources found."
        )
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = response_error.json()
        )

    # Convert filepath or filelist to lists for JSON serialisation
    contents = [filepath, ] if not filelist else filelist.get_cleaned_list()

    # return response, transaction id accepted for processing
    response = FileResponse(
        uuid = transaction_id,
        msg = (f"PUT transaction with id {transaction_id} accepted for "
                "processing.")
    )
    rabbit_publish_response(
        f"{RabbitMQPublisher.RK_ROOT}.{RabbitMQPublisher.RK_ROUTE}."
        f"{RabbitMQPublisher.RK_PUT}", 
        transaction_id, user, group, contents,
        tenancy, access_key, secret_key
    )
    
    return JSONResponse(status_code = status.HTTP_202_ACCEPTED,
                        content = response.json())


@router.post("/")
async def post():
    return {}


@router.delete("/")
async def delete():
    return {}
