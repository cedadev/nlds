from fastapi import Depends, APIRouter, status, Query, FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import FileResponse
from requests.auth import HTTPBasicAuth

import asyncio
import requests
import os
import json

from . import rpc_publisher
from ..errors import ResponseError


router = APIRouter()


static_dir = (os.path.join(os.path.dirname(__file__), "../static"))
#print(static_dir)
router.mount("/static", StaticFiles(directory=static_dir), name="static")

template_dir = os.path.join(os.path.dirname(__file__), "../templates/")

templates = Jinja2Templates(directory=template_dir)




class SystemResponse(BaseModel):
    status: str = ""


def get_consumer_number(host_ip, api_port, queue_name, login, password, vhost):
    api_queues = 'http://' + host_ip + ':' + api_port + '/api/queues/' + vhost + '/'+queue_name
    res = requests.get(api_queues, auth=HTTPBasicAuth(login, password))
    res_json = res.json()
    # Number of consumers
    consumers = (res_json['consumer_details'])
    consumer_tags = []
    for consumer in consumers:
        consumer_tags.append(consumer["consumer_tag"])
    
    return(consumer_tags)


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
    
    
    time_limit = 1
    
    
    
    #print(json.dumps(json_info, indent=4))
    
    
    
    #make into function
    consumer_tags = (get_consumer_number(rpc_publisher.config["server"], "15672", monitor_key, rpc_publisher.config["user"], rpc_publisher.config["password"], rpc_publisher.config["vhost"]))
    
    msg_dict["details"]["target_consumer"] = "monitor"
    
    monitor_fail = []
    consumer_count = len(consumer_tags)
    monitor_count = 0
    
    for consumer_tag in consumer_tags:
        try:
            monitor = await rpc_publisher.call(msg_dict=msg_dict, routing_key=monitor_key, time_limit=time_limit, correlation_id=consumer_tag)
            if monitor:
                monitor_count += 1
            else:
                monitor_fail.append(consumer_tag)
        except asyncio.TimeoutError:
            monitor_fail.append(consumer_tag)
    
    if monitor_count == 0:
        if consumer_count == 0:
            monitor = {"val": "All Consumers Offline (None running)", "colour": "RED"}
        else:
            monitor = {"val": "All Consumers Offline (0/"+ str(consumer_count) +")" , "colour": "RED", "failed": monitor_fail}
        
    elif monitor_count == consumer_count:
        monitor = {"val": "All Consumers Online ("+ str(consumer_count) +"/"+ str(consumer_count) +")", "colour": "GREEN"}
            
    else:
        monitor = {"val": ""+ str(monitor_count) +"/"+ str(consumer_count) +" Online", "colour": "ORANGE", "failed": monitor_fail}
    
    
    
    
    
    msg_dict["details"]["target_consumer"] = "catalog"
    
    try:
        catalog = await rpc_publisher.call(msg_dict=msg_dict, routing_key=catalog_key, time_limit=time_limit)
        if catalog:
            catalog = {"val": "Online", "colour": "GREEN"}
        else:
            catalog = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        catalog = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "nlds_worker"
    
    try:
        nlds_worker = await rpc_publisher.call(msg_dict=msg_dict, routing_key=nlds_worker_key, time_limit=time_limit)
        if nlds_worker:
            nlds_worker = {"val": "Online", "colour": "GREEN"}
        else:
            nlds_worker = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        nlds_worker = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "index"
    
    try:
        index = await rpc_publisher.call(msg_dict=msg_dict, routing_key=index_key, time_limit=time_limit)
        if index:
            index = {"val": "Online", "colour": "GREEN"}
        else:
            index = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        index = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "get_transfer"
    
    try:
        get_transfer = await rpc_publisher.call(msg_dict=msg_dict, routing_key=get_transfer_key, time_limit=time_limit)
        if get_transfer:
            get_transfer = {"val": "Online", "colour": "GREEN"}
        else:
            get_transfer = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        get_transfer = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "put_transfer"
    
    try:
        put_transfer = await rpc_publisher.call(msg_dict=msg_dict, routing_key=put_transfer_key, time_limit=time_limit)
        if put_transfer:
            put_transfer = {"val": "Online", "colour": "GREEN"}
        else:
            put_transfer = {"val": "Offline", "colour": "RED"}
    except asyncio.TimeoutError:
        put_transfer = {"val": "Offline", "colour": "RED"}
    
    
    msg_dict["details"]["target_consumer"] = "logger"
    
    try:
        logger = await rpc_publisher.call(msg_dict=msg_dict, routing_key=logger_key, time_limit=time_limit)
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
    
    
    return templates.TemplateResponse("index.html", context={"request": request, "stats": response})