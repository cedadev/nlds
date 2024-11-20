import os
from datetime import datetime, timedelta

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

from fastapi.templating import Jinja2Templates

try:
    from nlds.nlds_utils import nlds_monitor
except ImportError:
    # The pytest requires an alternative import option
    from nlds_utils import nlds_monitor

router = APIRouter()

template_dir = os.path.join(os.path.dirname(__file__), "../templates/")

templates = Jinja2Templates(directory=template_dir)


class FilterRequest(BaseModel):
    user: Optional[str] = None
    group: Optional[str] = None
    state: Optional[str] = None
    recordState: Optional[str] = None
    recordId: Optional[int] = None
    transactionId: Optional[str] = None
    startTime: Optional[str] = None
    endTime: Optional[str] = None
    order: str


def get_records(
    user=None,
    group=None,
    state=None,
    recordState=None,
    recordId=None,
    transactionId=None,
    startTime=None,
    endTime=None,
    order=None,
):
    if not startTime:
        startTime = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    try:
        state, recordState = nlds_monitor.validate_inputs(
            startTime, endTime, state, recordState
        )
    except SystemExit as e:
        raise HTTPException(status_code=400, detail=str(e))

    session = nlds_monitor.connect_to_monitor()

    record_list = nlds_monitor.query_monitor_db(
        session,
        user=user,
        group=group,
        state=state,
        record_state=recordState,
        id=recordId,
        transaction_id=transactionId,
        start_time=startTime,
        end_time=endTime,
        order=order,
    )
    result = nlds_monitor.construct_record_dict(record_list)

    for record in result:
        record["creation_time"] = str(record["creation_time"])
        for sub_record in record["sub_records"]:
            sub_record["last_updated"] = str(sub_record["last_updated"])

    stat_string = nlds_monitor.construct_stat_string(
        recordId,
        transactionId,
        user,
        group,
        state,
        recordState,
        startTime,
        endTime,
        order,
    )

    return result, stat_string


@router.get("/")
def get(
    request: Request,
):
    result, _ = get_records()

    return templates.TemplateResponse(
        "info.html", context={"request": request, "info": result}
    )


@router.post("/")
def get_filtered_records(filter_request: FilterRequest):
    user = filter_request.user
    group = filter_request.group
    state = filter_request.state
    recordState = filter_request.recordState
    recordId = filter_request.recordId
    transactionId = filter_request.transactionId
    startTime = filter_request.startTime
    endTime = filter_request.endTime
    order = filter_request.order

    result, stat_string = get_records(
        user,
        group,
        state,
        recordState,
        recordId,
        transactionId,
        startTime,
        endTime,
        order,
    )

    return JSONResponse(content={"records": result, "message": stat_string})


# no authentication required for the moment

# nlds-up nlds-api
# http://127.0.0.1:8000/info/