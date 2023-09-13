import asyncio
import requests
import socket
import os
import json
from typing import Annotated

from fastapi import Depends, APIRouter, status, Query, FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
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


class RabbitError(Exception):
    "Rabbits may not be working"
    pass


class RequestError(Exception):
    "Requests may not be working"
    pass


class LoginError(Exception):
    "Your login information is incorrect or you are not authorised"
    pass


class Error(Exception):
    def __init__(self, json):
        self.json = json


class SystemResponse(BaseModel):
    
    status: str = ""


def convert_json(val):
    
    return(val.json())


def get_consumer_info(host_ip, api_port, queue_name, login, password, vhost):
    
    api_queues = (f'http://{host_ip}:{api_port}/api/queues/{vhost}/{queue_name}')
    try:
        res = requests.get(api_queues, auth=HTTPBasicAuth(login, password))
    except:
        raise RequestError
    
    res_json = convert_json(res)
    if res_json == {'error': 'Object Not Found', 'reason': 'Not Found'}:
        raise RabbitError
    
    if res_json == {'error': 'not_authorized', 'reason': 'Login failed'}:
        raise LoginError
    
    try:
        res_json["error"]
        raise Error(res_json)
    except KeyError:
        pass
    
    # Number of consumers
    consumers = (res_json['consumer_details'])
    consumer_tags = []
    for consumer in consumers:
        consumer_tags.append(consumer["consumer_tag"])
    
    return(consumer_tags)


async def get_consumer_status(key, target, msg_dict, time_limit, skip_num=0):
    try:
        consumer_tags = (get_consumer_info(rpc_publisher.config["server"], 
                                       rpc_publisher.config["admin_port"], key, 
                                       rpc_publisher.config["user"], 
                                       rpc_publisher.config["password"], 
                                       rpc_publisher.config["vhost"]))
    except requests.exceptions.RequestException as e:
        print("Something went wrong, returning a 404... ")
        return{
            "val": ("404 error"), 
            "colour": "PURPLE"
            }, 0, 0
    except RequestError as e:
        print("Something went wrong, returning a 404... ")
        return{
            "val": ("404 error"), 
            "colour": "PURPLE"
            }, 0, 0
    except RabbitError as e:
        print("The rabbit server may be offline... ")
        print("Please try restart the consumers starting with logging_q ")
        return{
            "val": ("Rabbit error"), 
            "colour": "PURPLE"
            }, 0, 0
    except LoginError as e:
        print("Your RabbitMQ login information is incorrect or not authorised ")
        print("Please enter valid login information in the JASMIN .server_config file ")
        return{
            "val": ("Login error"), 
            "colour": "PURPLE"
            }, 0, 0
    except Error as e:
        print("an unexpected error occurred: ")
        print(e.json)
        return{
            "val": e.json, 
            "colour": "PURPLE"
            }, 0, 0
    
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

    return consumers, consumer_count, consumers_count


@router.get("/status/",
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

async def get(request: Request, time_limit: str = Query("5", alias='time-limit'), 
              consumer: list[str] = Query(["all"], alias='consumer')):


    # http://127.0.0.1:8000/system/status/?consumer={consumer name here}&time_limit={time limit here}
    # max time limit is 30 seconds because the uvicorn server doesn't actually do
    # anything until its finished with that number even if the page was closed
    
    
    default = False
    services = ["monitor", "catalog", "nlds worker", "index", "get transfer", 
                "put transfer", "logger"]
    gathered = []
    
    try:
        time = int(time_limit)
        if time > 15 or time < 1:
            time = 5
    except:
        time = 5


    msg_dict = {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
            }
        }
    
    
    # x seconds for each broken consumer meaning if there are 
    # a lot you might want to decrease this number
    time_limit = time


    services_dict = {
        "monitor": ["monitor_q", "monitor"],
        "catalog": ["catalog_q", "catalog"],
        "nlds worker": ["nlds_q", "nlds_worker"],
        "index": ["index_q", "index"],
        "get transfer": ["transfer_get_q", "get_transfer"],
        "put transfer": ["transfer_put_q", "put_transfer"],
        "logger": ["logging_q", "logger"],
    }
    
    partial_services = []
    structure_dict = {
        "service": "",
        "info": ""
    }

    consumers = consumer
    if consumers:
        if consumers[0] != "all":
            for consumer in consumers:
                consumer = consumer.lower()
                if consumer in services:
                    gathered.append(consumer)
            gathered = list(dict.fromkeys(gathered))
        
            for service in gathered:
                
                name = services_dict[service][1]
                key = services_dict[service][0]
            
                value = await get_consumer_status(key, name, 
                                                    msg_dict, time_limit, 0)
                value = value[0]
                
                structure_dict2 = structure_dict.copy()
                
                structure_dict2["service"] = name
                structure_dict2["info"] = value
                
                partial_services.append(structure_dict2)
            if len(partial_services) != 0:
                return templates.TemplateResponse("selection.html", 
                                                context={
                                                    "request": request, 
                                                    "status": partial_services
                                                    }
                                                )
    
    

    
    
    # The numbers indicate how many consumers in a queue to skip
    monitor = await get_consumer_status("monitor_q", "monitor", 
                                        msg_dict, time_limit, 0)
    monitor = monitor[0]
    
    catalog = await get_consumer_status("catalog_q", "catalog", 
                                        msg_dict, time_limit, 0)
    catalog = catalog[0]
    
    nlds_worker = await get_consumer_status("nlds_q", "nlds_worker", 
                                            msg_dict, time_limit, 0)
    nlds_worker = nlds_worker[0]
    
    index = await get_consumer_status("index_q", "index", 
                                      msg_dict, time_limit, 0)
    index = index[0]
    
    get_transfer = await get_consumer_status("transfer_get_q", "get_transfer", 
                                             msg_dict, time_limit, 0)
    get_transfer = get_transfer[0]
    
    put_transfer = await get_consumer_status("transfer_put_q", "put_transfer", 
                                             msg_dict, time_limit, 0)
    put_transfer = put_transfer[0]
    
    logger = await get_consumer_status("logging_q", "logger", 
                                       msg_dict, time_limit, 0)
    logger = logger[0]
    
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
                                          "status": response
                                          }
                                      )
    
@router.get("/status/{service}",
            status_code = status.HTTP_202_ACCEPTED,
            responses = {
                status.HTTP_202_ACCEPTED: {"model" : SystemResponse},
                status.HTTP_400_BAD_REQUEST: {"model" : ResponseError},
                status.HTTP_401_UNAUTHORIZED: {"model" : ResponseError},
                status.HTTP_403_FORBIDDEN: {"model" : ResponseError},
                status.HTTP_404_NOT_FOUND: {"model" : ResponseError}
            }
        )
async def get_service_json(request: Request, service, 
                           time_limit: str = Query("5", alias='time-limit')):
    
    
    try:
        time = int(time_limit)
        if time > 15 or time < 1:
            time = 5
    except ValueError:
        time = 5
            
    
    services_dict = {
        "monitor": "monitor_q",
        "catalog": "catalog_q",
        "nlds_worker": "nlds_q",
        "index": "index_q",
        "get_transfer": "transfer_get_q",
        "put_transfer": "transfer_put_q",
        "logger": "logging_q",
    }
    
    services = ["monitor", "catalog", "nlds_worker", "index", "get_transfer", 
                "put_transfer", "logger"]
    if service not in services:
        target_route_url = "/system/status/"
        if time_limit:
            target_route_url += f"?time-limit={time}&consumer=all"
        return RedirectResponse(url=target_route_url)
    else:
        
        
        time_limit = time

        msg_dict = {
            "details": {
                "api_action": "system_stat", 
                "target_consumer": "", 
                "ignore_message": False
                }
            }
        
        info_list = await get_consumer_status(services_dict[service], service, 
                                             msg_dict, time_limit, 0)
        info = info_list[0]
        
        final_dict = {
            "microservice_name": service,
            "total_num": 0,
            "num_failed": 0,
            "num_success": 0,
            "failed_list": [],
            "pid": os.getpid(),
            "hostname": socket.gethostname()
        }
        
        if info["val"] != "All Consumers Offline (None running)":
            try:
                failed_list = info["failed"]
            except KeyError:
                failed_list = []
            
            total_consumers = info_list[1]
            success_consumers = info_list[2]
            failed_consumers = total_consumers - success_consumers
            
            final_dict["total_num"] = total_consumers
            final_dict["num_success"] = success_consumers
            final_dict["num_failed"] = failed_consumers
            final_dict["failed_list"] = failed_list
        
        else:
            pass
        return(final_dict)
