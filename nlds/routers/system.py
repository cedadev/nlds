from fastapi import Depends, APIRouter, status, Query, FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import FileResponse

import asyncio
import os

from . import rpc_publisher
from ..errors import ResponseError


router = APIRouter()


static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../static/"))

print(static_dir)

router.mount("/static", StaticFiles(directory=static_dir), name="static")


template_dir = os.path.join(os.path.dirname(__file__), "../templates/")

templates = Jinja2Templates(directory=template_dir)




class SystemResponse(BaseModel):
    status: str = ""




@router.get("/stats/",
            status_code = status.HTTP_202_ACCEPTED,
            responses = {
                status.HTTP_202_ACCEPTED: {"model" : SystemResponse},
                status.HTTP_400_BAD_REQUEST: {"model" : ResponseError},
                status.HTTP_401_UNAUTHORIZED: {"model" : ResponseError},
                status.HTTP_403_FORBIDDEN: {"model" : ResponseError},
                status.HTTP_404_NOT_FOUND: {"model" : ResponseError}
            },
            response_class=HTMLResponse
        )
async def get(request: Request):
    
    
    msg_dict = {"details": {"api_action": "system_stat", "target_consumer": ""}}
    
    monitor_key = "monitor_q"
    catalog_key = "catalog_q"
    nlds_worker_key = "nlds_q"
    index_key = "index_q"
    get_transfer_key = "transfer_get_q"
    put_transfer_key = "transfer_put_q"
    logger_key = "logging_q"
    
    
    
    
    msg_dict["details"]["target_consumer"] = "monitor"
    
    try:
        monitor = await asyncio.wait_for(rpc_publisher.call(msg_dict=msg_dict, routing_key=monitor_key), timeout=5)
        if monitor:
            monitor = {"val": "Online", "colour": "GREEN"}
        else:
            monitor = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        monitor = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "catalog"
    
    try:
        catalog = await asyncio.wait_for(rpc_publisher.call(msg_dict=msg_dict, routing_key=catalog_key), timeout=5)
        if catalog:
            catalog = {"val": "Online", "colour": "GREEN"}
        else:
            catalog = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        catalog = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "nlds_worker"
    
    try:
        nlds_worker = await asyncio.wait_for(rpc_publisher.call(msg_dict=msg_dict, routing_key=nlds_worker_key), timeout=5)
        if nlds_worker:
            nlds_worker = {"val": "Online", "colour": "GREEN"}
        else:
            nlds_worker = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        nlds_worker = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "index"
    
    try:
        index = await asyncio.wait_for(rpc_publisher.call(msg_dict=msg_dict, routing_key=index_key), timeout=5)
        if index:
            index = {"val": "Online", "colour": "GREEN"}
        else:
            index = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        index = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "get_transfer"
    
    try:
        get_transfer = await asyncio.wait_for(rpc_publisher.call(msg_dict=msg_dict, routing_key=get_transfer_key), timeout=5)
        if get_transfer:
            get_transfer = {"val": "Online", "colour": "GREEN"}
        else:
            get_transfer = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        get_transfer = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "put_transfer"
    
    try:
        put_transfer = await asyncio.wait_for(rpc_publisher.call(msg_dict=msg_dict, routing_key=put_transfer_key), timeout=5)
        if put_transfer:
            put_transfer = {"val": "Online", "colour": "GREEN"}
        else:
            put_transfer = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        put_transfer = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "logger"
    
    try:
        logger = await asyncio.wait_for(rpc_publisher.call(msg_dict=msg_dict, routing_key=logger_key), timeout=5)
        if logger:
            logger = {"val": "Online", "colour": "GREEN"}
        else:
            logger = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        logger = {"val": "Offline", "colour": "RED"}
    
    
    response = {
        "monitor": monitor,
        "catalog": catalog,
        "nlds_worker": nlds_worker,
        "index": index,
        "get_transfer": get_transfer,
        "put_transfer": put_transfer,
        "logger": logger
    }
    
    #RED
    #GREEN
    
    #return response
    
    
    return templates.TemplateResponse("item.html", context={"request": request, "stats": response})