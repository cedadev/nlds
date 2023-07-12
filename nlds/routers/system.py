import asyncio
import requests
import os
import json

from fastapi import Depends, APIRouter, status, Query, FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import FileResponse
from requests.auth import HTTPBasicAuth

from . import rpc_publisher
from ..errors import ResponseError


router = APIRouter()


static_dir = (os.path.join(os.path.dirname(__file__), "../static"))
router.mount("/static", StaticFiles(directory=static_dir), name="static")

template_dir = os.path.join(os.path.dirname(__file__), "../templates/")

templates = Jinja2Templates(directory=template_dir)




class SystemResponse(BaseModel):
    
    status: str = ""


def convert_json(val):
    
    return(val.json())


def get_consumer_info(host_ip, api_port, queue_name, login, password, vhost):
    
    api_queues = (f'http://{host_ip}:{api_port}/api/queues/{vhost}/{queue_name}')
    try:
        res = requests.get(api_queues, auth=HTTPBasicAuth(login, password))
    except:
        return("request error")
    
    res_json = convert_json(res)
    if res_json == {'error': 'Object Not Found', 'reason': 'Not Found'}:
        return "RabbitError"
    # Number of consumers
    consumers = (res_json['consumer_details'])
    consumer_tags = []
    for consumer in consumers:
        consumer_tags.append(consumer["consumer_tag"])
    
    return(consumer_tags)


async def get_consumer_status(key, target, msg_dict, time_limit, skip_num=0):
    
    try:
        consumer_tags = (get_consumer_info(rpc_publisher.config["server"], 
                                       rpc_publisher.config["port"], key, 
                                       rpc_publisher.config["user"], 
                                       rpc_publisher.config["password"], 
                                       rpc_publisher.config["vhost"]))
    except requests.exceptions.RequestException as e:
        print("Something went wrong, returning a 403... ")
        return{
            "val": ("403 error"), 
            "colour": "PURPLE"
            }
    
    if consumer_tags == "RabbitError":
        print("The rabbit server may be offline... ")
        print("Please try restart the consumers starting with logging_q ")
        return{
            "val": ("Rabbit error"), 
            "colour": "PURPLE"
            }
    
    # if consumer_tags == "request error":
    #     return{
    #         "val": (f"Request Error"), 
    #         "colour": "PURPLE"
    #         }
    
    msg_dict["details"]["target_consumer"] = target
    
    
    consumers_fail = []
    consumer_count = len(consumer_tags)
    consumers_count = 0
    
    if len(consumer_tags) < skip_num:
        skip_num = len(consumer_tags)
    
    for consumer_tag in consumer_tags:
        if skip_num > 0:
            msg_dict["details"]["ignore_message"] = True
            skip_num = skip_num - 1
            
        try:
            consumers = await rpc_publisher.call(msg_dict=msg_dict, 
                                                 routing_key=key, 
                                                 time_limit=time_limit, 
                                                 correlation_id=consumer_tag)
            if consumers:
                consumers_count += 1
            else:
                consumers_fail.append(consumer_tag)
        except asyncio.TimeoutError:
            consumers_fail.append(consumer_tag)
        msg_dict["details"]["ignore_message"] = False
    
    if consumers_count == 0:
        if consumer_count == 0:
            consumers = {
                "val": "All Consumers Offline (None running)", 
                "colour": "RED"
                }
        else:
            consumers = {
                "val": (f"All Consumers Offline (0/{consumer_count})"), 
                "colour": "RED", "failed": consumers_fail
                }
        
    elif consumers_count == consumer_count:
        consumers = {
            "val": (f"All Consumers Online ({consumer_count}/{consumer_count})"), 
            "colour": "GREEN"
            }
            
    else:
        consumers = {
            "val": (f"Consumers Online ({consumers_count}/{consumer_count})"), 
            "colour": "ORANGE", 
            "failed": consumers_fail
            }

    return consumers


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
    
    
    msg_dict = {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
            }
        }
    
    
    # x seconds for each broken consumer meaning if there are 
    # a lot you might want to decrease this number
    time_limit = 5
    
    
    
    # The numbers indicate how many consumers in a queue to skip
    monitor = await get_consumer_status("monitor_q", "monitor", 
                                        msg_dict, time_limit, 0)
    
    catalog = await get_consumer_status("catalog_q", "catalog", 
                                        msg_dict, time_limit, 0)
    
    nlds_worker = await get_consumer_status("nlds_q", "nlds_worker", 
                                            msg_dict, time_limit, 0)
    
    index = await get_consumer_status("index_q", "index", 
                                      msg_dict, time_limit, 0)
    
    get_transfer = await get_consumer_status("transfer_get_q", "get_transfer", 
                                             msg_dict, time_limit, 0)
    
    put_transfer = await get_consumer_status("transfer_put_q", "put_transfer", 
                                             msg_dict, time_limit, 0)
    
    logger = await get_consumer_status("logging_q", "logger", 
                                       msg_dict, time_limit, 0)
    
    consumer_list = [monitor, catalog, nlds_worker, index, 
                     get_transfer, put_transfer, logger]
    
    num = 0
    offline_num = 0
    service = "Online"
    
    for consumer in consumer_list:
        try:
            count = len(consumer["failed"])
            num = num + count
        except:
            if consumer["colour"] == "RED":
                offline_num += 1
            else:
                pass
    
    if offline_num == 7:
        service = "Offline"
    
    if num > 0:
        colour = "alert-danger"
    else:
        colour = "alert-success"
    if service == "Offline":
        colour = "alert-info"
    
    failed_info = {
        "failed_num": num,
        "failed_colour": colour
    }
    
    response = {
        "monitor": monitor,
        "catalog": catalog,
        "nlds_worker": nlds_worker,
        "index": index,
        "get_transfer": get_transfer,
        "put_transfer": put_transfer,
        "logger": logger,
        "failed": failed_info
    }
    
    
    return templates.TemplateResponse("index.html", 
                                      context={
                                          "request": request, 
                                          "stats": response
                                          }
                                      )