# encoding: utf-8
"""
files.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from fastapi import Depends, APIRouter, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from uuid import UUID, uuid4
from typing import Optional, List, Dict

from nlds.routers import rabbit_publisher
import nlds.rabbit.message_keys as MSG
import nlds.rabbit.routing_keys as RK
from nlds.errors import ResponseError
from nlds.details import PathDetails
from nlds.authenticators.authenticate_methods import (
    authenticate_token,
    authenticate_group,
    authenticate_user,
)

router = APIRouter()

# uuid (for testing)
# 3fa85f64-5717-4562-b3fc-2c963f66afa6


class FileModel(BaseModel):
    filelist: List[str]
    label: str = None
    tag: Dict[str, str] = None
    holding_id: int = None

    class Config:
        json_schema_extra = {
            "example": {
                "filelist": [
                    "/datacentre/archvol5/qb139/archive/spot-9693-piControl/r1i1p1f1/Amon/tasmin/gn/files/d20190628/tasmin_Amon_HadGEM3-GC31-LL_piControl_r1i1p1f1_gn_185001-194912.nc",
                    "/datacentre/archvol5/qb139/archive/spot-9693-piControl/r1i1p1f1/Amon/tasmin/gn/files/d20190628/tasmin_Amon_HadGEM3-GC31-LL_piControl_r1i1p1f1_gn_195001-204912.nc",
                    "/datacentre/archvol5/qb139/archive/spot-9693-piControl/r1i1p1f1/Amon/tasmin/gn/files/d20190628/tasmin_Amon_HadGEM3-GC31-LL_piControl_r1i1p1f1_gn_205001-214912.nc",
                    "/datacentre/archvol5/qb139/archive/spot-9693-piControl/r1i1p1f1/Amon/tasmin/gn/files/d20190628/tasmin_Amon_HadGEM3-GC31-LL_piControl_r1i1p1f1_gn_215001-224912.nc",
                    "/datacentre/archvol5/qb139/archive/spot-9693-piControl/r1i1p1f1/Amon/tasmin/gn/files/d20190628/tasmin_Amon_HadGEM3-GC31-LL_piControl_r1i1p1f1_gn_225001-234912.nc",
                ]
            }
        }

    def to_str(self) -> str:
        return "[" + ",".join([f for f in self.filelist]) + "]"

    def get_cleaned_list(self) -> List[str]:
        # strip new lines and remove empty entries post new-line-strip
        flist = [f.rstrip("\n") for f in self.filelist]
        # important to remove empty entries as they will match with everything via the
        # regexp matcher
        if "" in flist:
            flist.remove("")
        return flist


class FileResponse(BaseModel):
    transaction_id: UUID
    msg: str
    user: str = ""
    group: str = ""
    api_action: str = ""
    job_label: str = ""
    tenancy: str = ""
    label: str = ""
    holding_id: int = -1
    tag: str = ""


############################ GET METHOD ############################
# this is not used by the NLDS client but is left in case another
# program contacts this URL
@router.get(
    "/",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": FileResponse},
        status.HTTP_400_BAD_REQUEST: {"model": ResponseError},
        status.HTTP_401_UNAUTHORIZED: {"model": ResponseError},
        status.HTTP_403_FORBIDDEN: {"model": ResponseError},
        status.HTTP_404_NOT_FOUND: {"model": ResponseError},
    },
)
async def get(
    transaction_id: UUID,
    token: str = Depends(authenticate_token),
    user: str = Depends(authenticate_user),
    group: str = Depends(authenticate_group),
    groupall: bool = False,
    filepath: str = None,
    target: Optional[str] = None,
    job_label: Optional[str] = None,
    tenancy: Optional[str] = None,
    label: Optional[str] = None,
    holding_id: Optional[int] = None,
    tag: Optional[str] = None,
    access_key: str = "",
    secret_key: str = "",
    regex: Optional[bool] = False,
):

    # validate filepath and filelist - one or the other has to exist
    if not filepath:
        response_error = ResponseError(
            loc=["files", "get"],
            msg="filepath not supplied.",
            type="Incomplete request.",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=response_error.json()
        )
    if job_label is None:
        job_label = str(transaction_id)[0:8]
    # return response, job label accepted for processing
    response = FileResponse(
        transaction_id=transaction_id, msg=(f"GET transaction accepted for processing.")
    )
    contents = [
        filepath,
    ]
    # create the message dictionary - do this here now as it's more transparent
    routing_key = f"{RK.ROOT}.{RK.ROUTE}.{RK.GETLIST}"
    api_action = f"{RK.GET}"
    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: str(transaction_id),
            MSG.SUB_ID: str(uuid4()),
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.GROUPALL: groupall,
            MSG.TENANCY: tenancy,
            MSG.TARGET: target,
            MSG.ACCESS_KEY: access_key,
            MSG.SECRET_KEY: secret_key,
            MSG.API_ACTION: api_action,
            MSG.JOB_LABEL: job_label,
        },
        MSG.DATA: {
            # Convert to PathDetails for JSON serialisation
            MSG.FILELIST: [PathDetails(original_path=item) for item in contents],
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    response.user = user
    response.group = group
    response.api_action = api_action
    if job_label:
        response.job_label = job_label
    if tenancy:
        response.tenancy = tenancy

    # add the metadata
    meta_dict = {}
    if label:
        meta_dict[MSG.LABEL] = label
        response.label = label
    if holding_id:
        meta_dict[MSG.HOLDING_ID] = holding_id
        response.holding_id = holding_id
    if tag:
        tag_dict = tag.strip('').split(':')
        meta_dict[MSG.TAG] = {tag_dict[0]:tag_dict[1]}
        response.tag = tag_dict
    if regex:
        meta_dict[MSG.REGEX] = True

    if len(meta_dict) > 0:
        msg_dict[MSG.META] = meta_dict

    rabbit_publisher.publish_message(routing_key, msg_dict)
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.json())


############################ GET LIST METHOD ########################
@router.put(
    "/getlist/",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": FileResponse},
        status.HTTP_400_BAD_REQUEST: {"model": ResponseError},
        status.HTTP_401_UNAUTHORIZED: {"model": ResponseError},
        status.HTTP_403_FORBIDDEN: {"model": ResponseError},
        status.HTTP_404_NOT_FOUND: {"model": ResponseError},
    },
)
async def put(
    transaction_id: UUID,
    token: str = Depends(authenticate_token),
    user: str = Depends(authenticate_user),
    group: str = Depends(authenticate_group),
    groupall: bool = False,
    filemodel: Optional[FileModel] = None,
    tenancy: Optional[str] = None,
    target: Optional[str] = None,
    job_label: Optional[str] = None,
    access_key: str = "",
    secret_key: str = "",
):

    # validate filepath and filelist - one or the other has to exist
    if not filemodel:
        response_error = ResponseError(
            loc=["files", "getlist"],
            msg="filelist not supplied.",
            type="Incomplete request.",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=response_error.json()
        )
    if job_label is None:
        job_label = str(transaction_id)[0:8]
    # return response, transaction id accepted for processing
    response = FileResponse(
        transaction_id=transaction_id,
        msg=(f"GETLIST transaction accepted for processing."),
    )

    # Convert filepath or filelist to lists
    contents = filemodel.get_cleaned_list()

    # create the message dictionary - do this here now as it's more transparent
    routing_key = f"{RK.ROOT}.{RK.ROUTE}.{RK.GETLIST}"
    api_method = f"{RK.GETLIST}"
    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: str(transaction_id),
            MSG.SUB_ID: str(uuid4()),
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.GROUPALL: groupall,
            MSG.TENANCY: tenancy,
            MSG.TARGET: target,
            MSG.JOB_LABEL: job_label,
            MSG.ACCESS_KEY: access_key,
            MSG.SECRET_KEY: secret_key,
            MSG.API_ACTION: api_method,
        },
        MSG.DATA: {
            # Convert to PathDetails for JSON serialisation
            MSG.FILELIST: [PathDetails(original_path=item) for item in contents],
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    response.user = user
    response.group = group
    response.api_action = api_method
    if job_label:
        response.job_label = job_label
    if tenancy:
        response.tenancy = tenancy
    # add the metadata
    meta_dict = {}
    if filemodel.label:
        meta_dict[MSG.LABEL] = filemodel.label
        response.label = filemodel.label
    if filemodel.holding_id:
        meta_dict[MSG.HOLDING_ID] = filemodel.holding_id
        response.holding_id = filemodel.holding_id
    if filemodel.tag:
        tag_dict = filemodel.tag
        meta_dict[MSG.TAG] = tag_dict
        response.tag = tag_dict

    if len(meta_dict) > 0:
        msg_dict[MSG.META] = meta_dict
    rabbit_publisher.publish_message(routing_key, msg_dict)

    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.json())


############################ PUT METHOD ############################
@router.put(
    "/",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": FileResponse},
        status.HTTP_400_BAD_REQUEST: {"model": ResponseError},
        status.HTTP_401_UNAUTHORIZED: {"model": ResponseError},
        status.HTTP_403_FORBIDDEN: {"model": ResponseError},
        status.HTTP_404_NOT_FOUND: {"model": ResponseError},
    },
)
async def put(
    transaction_id: UUID,
    token: str = Depends(authenticate_token),
    user: str = Depends(authenticate_user),
    group: str = Depends(authenticate_group),
    filemodel: Optional[FileModel] = None,
    tenancy: Optional[str] = None,
    job_label: Optional[str] = None,
    access_key: str = "",
    secret_key: str = "",
):

    # validate FileModel, it has to exist
    if not filemodel:
        response_error = ResponseError(
            loc=["files", "put"],
            msg=("body data is incomplete"),
            type="Resources not found.",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=response_error.json()
        )

    # Convert filepath or filelist to lists
    contents = filemodel.get_cleaned_list()

    if job_label is None:
        job_label = str(transaction_id)[0:8]

    # if one file then then method is PUT, if more
    # than one it is PUTLIST
    if len(contents) == 1:
        api_method = f"{RK.PUT}"
    else:
        api_method = f"{RK.PUTLIST}" 
    routing_key = f"{RK.ROOT}.{RK.ROUTE}.{api_method}"

    # return response, transaction id accepted for processing
    response = FileResponse(
        transaction_id=transaction_id, 
        msg=(f"{api_method} transaction accepted for processing.")
    )

    # create the message dictionary
    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: str(transaction_id),
            MSG.SUB_ID: str(uuid4()),
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.TENANCY: tenancy,
            MSG.JOB_LABEL: job_label,
            MSG.ACCESS_KEY: access_key,
            MSG.SECRET_KEY: secret_key,
            MSG.API_ACTION: api_method,
        },
        MSG.DATA: {
            # Convert to PathDetails for JSON serialisation
            MSG.FILELIST: [PathDetails(original_path=item) for item in contents],
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    response.user = user
    response.group = group
    response.api_action = api_method
    if job_label:
        response.job_label = job_label
    if tenancy:
        response.tenancy = tenancy
    # add the metadata
    meta_dict = {}
    if filemodel.label:
        meta_dict[MSG.LABEL] = filemodel.label
        response.label = filemodel.label
    if filemodel.holding_id:
        meta_dict[MSG.HOLDING_ID] = filemodel.holding_id
        response.holding_id = filemodel.holding_id
    if filemodel.tag:
        tag_dict = filemodel.tag
        meta_dict[MSG.TAG] = tag_dict
        response.tag = tag_dict

    if len(meta_dict) > 0:
        msg_dict[MSG.META] = meta_dict

    rabbit_publisher.publish_message(routing_key, msg_dict)

    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.json())


@router.put(
    "/archive/",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {"model": FileResponse},
        status.HTTP_400_BAD_REQUEST: {"model": ResponseError},
        status.HTTP_401_UNAUTHORIZED: {"model": ResponseError},
        status.HTTP_403_FORBIDDEN: {"model": ResponseError},
        status.HTTP_404_NOT_FOUND: {"model": ResponseError},
    },
)
async def put(
    transaction_id: UUID,
    token: str = Depends(authenticate_token),
    user: str = Depends(authenticate_user),
    group: str = Depends(authenticate_group),
    filemodel: Optional[FileModel] = None,
    tenancy: Optional[str] = None,
    job_label: Optional[str] = None,
    access_key: str = "",
    secret_key: str = "",
):

    # validate FileModel, it has to exist
    if not filemodel:
        response_error = ResponseError(
            loc=["files", "archive"],
            msg=("body data is incomplete"),
            type="Resources not found.",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=response_error.json()
        )

    if user not in ("nlds"):
        response_error = ResponseError(
            loc=["files", "archive"],
            msg=("archive action is admin-only"),
            type="Action Forbidden.",
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail=response_error.json()
        )

    # Convert filepath or filelist to lists
    contents = filemodel.get_cleaned_list()

    if job_label is None:
        job_label = str(transaction_id)[0:8]

    # return response, transaction id accepted for processing
    response = FileResponse(
        transaction_id=transaction_id,
        msg=(f"ARCHIVE transaction accepted for processing."),
    )

    # create the message dictionary - do this here now as it's more transparent
    routing_key = f"{RK.ROOT}.{RK.ROUTE}.{RK.ARCHIVE_PUT}"
    api_method = f"{RK.ARCHIVE_PUT}"
    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: str(transaction_id),
            MSG.SUB_ID: str(uuid4()),
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.TENANCY: tenancy,
            MSG.JOB_LABEL: job_label,
            MSG.ACCESS_KEY: access_key,
            MSG.SECRET_KEY: secret_key,
            MSG.API_ACTION: api_method,
        },
        MSG.DATA: {
            # Convert to PathDetails for JSON serialisation
            MSG.FILELIST: [PathDetails(original_path=item) for item in contents],
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    response.user = user
    response.group = group
    response.api_action = api_method
    if job_label:
        response.job_label = job_label
    if tenancy:
        response.tenancy = tenancy
    # add the metadata
    meta_dict = {}
    if filemodel.label:
        meta_dict[MSG.LABEL] = filemodel.label
        response.label = filemodel.label
    if filemodel.holding_id:
        meta_dict[MSG.HOLDING_ID] = filemodel.holding_id
        response.holding_id = filemodel.holding_id
    if filemodel.tag:
        tag_dict = filemodel.tag
        meta_dict[MSG.TAG] = tag_dict
        response.tag = tag_dict

    if len(meta_dict) > 0:
        msg_dict[MSG.META] = meta_dict
    rabbit_publisher.publish_message(routing_key, msg_dict)

    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.json())


# @router.post("/")
# async def post():
#     return {}


# @router.delete("/")
# async def delete():
#     return {}
