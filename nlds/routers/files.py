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

from uuid import UUID, uuid4
from typing import Optional, List, Dict
from copy import deepcopy

from ..routers import rabbit_publisher
from ..rabbit.publisher import RabbitMQPublisher as RMQP
from ..errors import ResponseError
from ..details import PathDetails, Retries
from ..authenticators.authenticate_methods import authenticate_token, \
                                                  authenticate_group, \
                                                  authenticate_user

router = APIRouter()

# uuid (for testing)
# 3fa85f64-5717-4562-b3fc-2c963f66afa6

class FileModel(BaseModel):
    filelist: List[str]
    label: str = None
    tag: Dict[str, str] = None
    holding_id: int = None

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
              target: Optional[str] = None,
              job_label: Optional[str] = None,
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
    if job_label is None:
        job_label = transaction_id[0:8]
    # return response, job label accepted for processing
    response = FileResponse(
        transaction_id = transaction_id,
        msg = (f"GET transaction accepted for processing.")
    )
    contents = [filepath, ]
    # create the message dictionary - do this here now as it's more transparent
    routing_key = f"{RMQP.RK_ROOT}.{RMQP.RK_ROUTE}.{RMQP.RK_GETLIST}"
    api_action = f"{RMQP.RK_GETLIST}"
    msg_dict = {
        RMQP.MSG_DETAILS: {
            RMQP.MSG_TRANSACT_ID: str(transaction_id),
            RMQP.MSG_SUB_ID: str(uuid4()),
            RMQP.MSG_USER: user,
            RMQP.MSG_GROUP: group,
            RMQP.MSG_TENANCY: tenancy,
            RMQP.MSG_TARGET: target,
            RMQP.MSG_ACCESS_KEY: access_key,
            RMQP.MSG_SECRET_KEY: secret_key,
            RMQP.MSG_API_ACTION: api_action,
            RMQP.MSG_JOB_LABEL: job_label
        }, 
        RMQP.MSG_DATA: {
            # Convert to PathDetails for JSON serialisation
            RMQP.MSG_FILELIST: [PathDetails(original_path=item) for item in contents],
        }, 
        **Retries().to_dict(),
        RMQP.MSG_TYPE: RMQP.MSG_TYPE_STANDARD,
    }
    response.user = user
    response.group = group
    response.api_action = api_action
    if job_label:
        response.job_label = job_label
    if tenancy:
        response.tenancy = tenancy

    rabbit_publisher.publish_message(routing_key, msg_dict)
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
              token: str = Depends(authenticate_token),
              user: str = Depends(authenticate_user),
              group: str = Depends(authenticate_group),
              filemodel: Optional[FileModel]=None,
              tenancy: Optional[str]=None,
              target: Optional[str]=None,
              job_label: Optional[str] = None,
              access_key: str="",
              secret_key: str=""
              ):

    # validate filepath and filelist - one or the other has to exist
    if not filemodel:
        response_error = ResponseError(
                loc = ["files", "getlist"],
                msg = "filelist not supplied.",
                type = "Incomplete request."
            )
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = response_error.json()
        )
    if job_label is None:
        job_label = transaction_id[0:8]
    # return response, transaction id accepted for processing
    response = FileResponse(
        transaction_id = transaction_id,
        msg = (f"GETLIST transaction accepted for processing.")
    )

    # Convert filepath or filelist to lists
    contents = filemodel.get_cleaned_list()

    # create the message dictionary - do this here now as it's more transparent
    routing_key = f"{RMQP.RK_ROOT}.{RMQP.RK_ROUTE}.{RMQP.RK_GETLIST}"
    api_method = f"{RMQP.RK_GETLIST}"
    msg_dict = {
        RMQP.MSG_DETAILS: {
            RMQP.MSG_TRANSACT_ID: str(transaction_id),
            RMQP.MSG_SUB_ID: str(uuid4()),
            RMQP.MSG_USER: user,
            RMQP.MSG_GROUP: group,
            RMQP.MSG_TENANCY: tenancy,
            RMQP.MSG_TARGET: target,
            RMQP.MSG_JOB_LABEL: job_label,
            RMQP.MSG_ACCESS_KEY: access_key,
            RMQP.MSG_SECRET_KEY: secret_key,
            RMQP.MSG_API_ACTION: api_method
        }, 
        RMQP.MSG_DATA: {
            # Convert to PathDetails for JSON serialisation
            RMQP.MSG_FILELIST: [PathDetails(original_path=item) for item in contents],
        },
        **Retries().to_dict(),
        RMQP.MSG_TYPE: RMQP.MSG_TYPE_STANDARD,
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
    if (filemodel.label):
        meta_dict[RMQP.MSG_LABEL] = filemodel.label
        response.label = filemodel.label
    if (filemodel.holding_id):
        meta_dict[RMQP.MSG_HOLDING_ID] = filemodel.holding_id
        response.holding_id = filemodel.holding_id
    if (filemodel.tag):
        tag_dict = filemodel.tag
        meta_dict[RMQP.MSG_TAG] = tag_dict
        response.tag = tag_dict

    if (len(meta_dict) > 0):
        msg_dict[RMQP.MSG_META] = meta_dict
    rabbit_publisher.publish_message(routing_key, msg_dict
    )

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
              filemodel: Optional[FileModel]=None,
              tenancy: Optional[str]=None,
              job_label: Optional[str] = None,
              access_key: str="",
              secret_key: str=""
            ):

    # validate FileModel, it has to exist
    if not filemodel:
        response_error = ResponseError(
            loc = ["files", "put"],
            msg = ("body data is incomplete"),
            type = "Resources not found."
        )
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = response_error.json()
        )

    # Convert filepath or filelist to lists
    contents = filemodel.get_cleaned_list()

    if job_label is None:
        job_label = transaction_id[0:8]
        
    # return response, transaction id accepted for processing
    response = FileResponse(
        transaction_id = transaction_id,
        msg = (f"PUT transaction accepted for processing.")
    )
    # create the message dictionary - do this here now as it's more transparent
    routing_key = f"{RMQP.RK_ROOT}.{RMQP.RK_ROUTE}.{RMQP.RK_PUT}"
    api_method = f"{RMQP.RK_PUT}"
    msg_dict = {
        RMQP.MSG_DETAILS: {
            RMQP.MSG_TRANSACT_ID: str(transaction_id),
            RMQP.MSG_SUB_ID: str(uuid4()),
            RMQP.MSG_USER: user,
            RMQP.MSG_GROUP: group,
            RMQP.MSG_TENANCY: tenancy,
            RMQP.MSG_JOB_LABEL: job_label,
            RMQP.MSG_ACCESS_KEY: access_key,
            RMQP.MSG_SECRET_KEY: secret_key,
            RMQP.MSG_API_ACTION: api_method
        }, 
        RMQP.MSG_DATA: {
            # Convert to PathDetails for JSON serialisation
            RMQP.MSG_FILELIST: [PathDetails(original_path=item) for item in contents],
        },
        **Retries().to_dict(),
        RMQP.MSG_TYPE: RMQP.MSG_TYPE_STANDARD,
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
    if (filemodel.label):
        meta_dict[RMQP.MSG_LABEL] = filemodel.label
        response.label = filemodel.label
    if (filemodel.holding_id):
        meta_dict[RMQP.MSG_HOLDING_ID] = filemodel.holding_id
        response.holding_id = filemodel.holding_id
    if (filemodel.tag):
        tag_dict = filemodel.tag
        meta_dict[RMQP.MSG_TAG] = tag_dict
        response.tag = tag_dict

    if (len(meta_dict) > 0):
        msg_dict[RMQP.MSG_META] = meta_dict
        
    rabbit_publisher.publish_message(routing_key, msg_dict)
    
    return JSONResponse(status_code = status.HTTP_202_ACCEPTED,
                        content = response.json())


@router.put("/archive",
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
              filemodel: Optional[FileModel]=None,
              tenancy: Optional[str]=None,
              job_label: Optional[str] = None,
              access_key: str="",
              secret_key: str=""
            ):

    # validate FileModel, it has to exist
    if not filemodel:
        response_error = ResponseError(
            loc = ["files", "archive"],
            msg = ("body data is incomplete"),
            type = "Resources not found."
        )
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = response_error.json()
        )
    
    if user not in ('jleland', 'nrmassey', 'nlds'):
        response_error = ResponseError(
            loc = ["files", "archive"],
            msg = ("archive action is admin-only"),
            type = "Action Forbidden."
        )
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN,
            detail = response_error.json()
        )

    # Convert filepath or filelist to lists
    contents = filemodel.get_cleaned_list()

    if job_label is None:
        job_label = str(transaction_id)[0:8]
        
    # return response, transaction id accepted for processing
    response = FileResponse(
        transaction_id = transaction_id,
        msg = (f"ARCHIVE transaction accepted for processing.")
    )

    # create the message dictionary - do this here now as it's more transparent
    routing_key = f"{RMQP.RK_ROOT}.{RMQP.RK_ROUTE}.{RMQP.RK_ARCHIVE_PUT}"
    api_method = f"{RMQP.RK_ARCHIVE_PUT}"
    msg_dict = {
        RMQP.MSG_DETAILS: {
            RMQP.MSG_TRANSACT_ID: str(transaction_id),
            RMQP.MSG_SUB_ID: str(uuid4()),
            RMQP.MSG_USER: user,
            RMQP.MSG_GROUP: group,
            RMQP.MSG_TENANCY: tenancy,
            RMQP.MSG_JOB_LABEL: job_label,
            RMQP.MSG_ACCESS_KEY: access_key,
            RMQP.MSG_SECRET_KEY: secret_key,
            RMQP.MSG_API_ACTION: api_method
        }, 
        RMQP.MSG_DATA: {
            # Convert to PathDetails for JSON serialisation
            RMQP.MSG_FILELIST: [PathDetails(original_path=item) for item in contents],
        },
        **Retries().to_dict(),
        RMQP.MSG_TYPE: RMQP.MSG_TYPE_STANDARD,
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
    if (filemodel.label):
        meta_dict[RMQP.MSG_LABEL] = filemodel.label
        response.label = filemodel.label
    if (filemodel.holding_id):
        meta_dict[RMQP.MSG_HOLDING_ID] = filemodel.holding_id
        response.holding_id = filemodel.holding_id
    if (filemodel.tag):
        tag_dict = filemodel.tag
        meta_dict[RMQP.MSG_TAG] = tag_dict
        response.tag = tag_dict

    if (len(meta_dict) > 0):
        msg_dict[RMQP.MSG_META] = meta_dict
    rabbit_publisher.publish_message(routing_key, msg_dict)

    return JSONResponse(status_code = status.HTTP_202_ACCEPTED,
                        content = response.json())

# @router.post("/")
# async def post():
#     return {}


# @router.delete("/")
# async def delete():
#     return {}
