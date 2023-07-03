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


def get_consumer_info(host_ip, api_port, queue_name, login, password, vhost):                       #what happenes if the rabbit monitoring api is unavailable (make more defensive)
    api_queues = 'http://' + host_ip + ':' + api_port + '/api/queues/' + vhost + '/'+queue_name
    res = requests.get(api_queues, auth=HTTPBasicAuth(login, password))
    res_json = res.json()
    # Number of consumers
    consumers = (res_json['consumer_details'])
    consumer_tags = []
    for consumer in consumers:
        consumer_tags.append(consumer["consumer_tag"])
    
    return(consumer_tags)


async def get_consumer_status(key, target, msg_dict, time_limit, skip_num=0):
    consumer_tags = (get_consumer_info(rpc_publisher.config["server"], rpc_publisher.config["port"], key, rpc_publisher.config["user"], rpc_publisher.config["password"], rpc_publisher.config["vhost"]))
    
    msg_dict["details"]["target_consumer"] = target
    
    
    #change monitor to consumer
    
    
    monitor_fail = []
    consumer_count = len(consumer_tags)
    monitor_count = 0
    
    if len(consumer_tags) < skip_num:
        skip_num = len(consumer_tags)
    
    for consumer_tag in consumer_tags:
        if skip_num > 0:
            msg_dict["details"]["ignore_message"] = True
            skip_num = skip_num - 1
            
        try:
            monitor = await rpc_publisher.call(msg_dict=msg_dict, routing_key=key, time_limit=time_limit, correlation_id=consumer_tag)
            if monitor:
                monitor_count += 1
            else:
                monitor_fail.append(consumer_tag)
        except asyncio.TimeoutError:
            monitor_fail.append(consumer_tag)
        msg_dict["details"]["ignore_message"] = False
    
    if monitor_count == 0:
        if consumer_count == 0:
            monitor = {"val": "All Consumers Offline (None running)", "colour": "RED"}
        else:
            monitor = {"val": "All Consumers Offline (0/"+ str(consumer_count) +")" , "colour": "RED", "failed": monitor_fail}
        
    elif monitor_count == consumer_count:
        monitor = {"val": "All Consumers Online ("+ str(consumer_count) +"/"+ str(consumer_count) +")", "colour": "GREEN"}
            
    else:
        monitor = {"val": "Consumers Online ("+ str(monitor_count) +"/"+ str(consumer_count) +")", "colour": "ORANGE", "failed": monitor_fail}

    return monitor


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
    
    
    msg_dict = {"details": {"api_action": "system_stat", "target_consumer": "", "ignore_message": False}}
    
    
    time_limit = 5
    
    
    
    #the numbers indicate how many consumers in a queue to skip
    monitor = await get_consumer_status("monitor_q", "monitor", msg_dict, time_limit, 1)
    catalog = await get_consumer_status("catalog_q", "catalog", msg_dict, time_limit, 0)
    nlds_worker = await get_consumer_status("nlds_q", "nlds_worker", msg_dict, time_limit, 0)
    index = await get_consumer_status("index_q", "index", msg_dict, time_limit, 1110)
    get_transfer = await get_consumer_status("transfer_get_q", "get_transfer", msg_dict, time_limit, 0)
    put_transfer = await get_consumer_status("transfer_put_q", "put_transfer", msg_dict, time_limit, 0)
    logger = await get_consumer_status("logging_q", "logger", msg_dict, time_limit, 0)
    
    
    response = {
        "monitor": monitor,
        "catalog": catalog,
        "nlds_worker": nlds_worker,
        "index": index,
        "get_transfer": get_transfer,
        "put_transfer": put_transfer,
        "logger": logger
    }
    
    
    return templates.TemplateResponse("index.html", context={"request": request, "stats": response})