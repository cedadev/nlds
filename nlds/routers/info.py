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
    subRecordState: Optional[str] = None
    recordId: Optional[int] = None
    transactionId: Optional[str] = None
    startTime: Optional[str] = None
    endTime: Optional[str] = None
    all: bool = False
    order: str


def get_records(
    user=None,
    group=None,
    state=None,
    subRecordState=None,
    recordId=None,
    transactionId=None,
    startTime=None,
    endTime=None,
    all=False,
    order=None,
):
    if not startTime:
        startTime = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    try:
        state, subRecordState = nlds_monitor.validate_inputs(
            startTime, endTime, state, subRecordState
        )
    except SystemExit as e:
        raise HTTPException(status_code=400, detail=str(e))

    session = nlds_monitor.connect_to_monitor()

    record_list = nlds_monitor.query_monitor_db(
        session,
        user=user,
        group=group,
        state=state,
        sub_record_state=subRecordState,
        id=recordId,
        transaction_id=transactionId,
        start_time=startTime,
        end_time=endTime,
        all=all,
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
        subRecordState,
        startTime,
        endTime,
        all,
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


@router.get("/{input_id}")
def get_from_id(
    request: Request,
    input_id: int,
):
    result, _ = get_records(recordId=input_id)

    return templates.TemplateResponse(
        "info.html", context={"request": request, "info": result}
    )


@router.post("/")
def get_filtered_records(filter_request: FilterRequest):
    user = filter_request.user
    group = filter_request.group
    state = filter_request.state
    subRecordState = filter_request.subRecordState
    recordId = filter_request.recordId
    transactionId = filter_request.transactionId
    startTime = filter_request.startTime
    endTime = filter_request.endTime
    all = filter_request.all
    order = filter_request.order

    result, stat_string = get_records(
        user,
        group,
        state,
        subRecordState,
        recordId,
        transactionId,
        startTime,
        endTime,
        all,
        order,
    )

    return JSONResponse(content={"records": result, "message": stat_string})


def create_rss_information(record_list):
    for pos, record in enumerate(record_list):
        title = f"NLDS Job Status id: {record['id']}"

        description_parts = []
        if "label" in record and record["label"]:
            description_parts.append(f"<p>label: {record['label']}</p>")
        if "job_label" in record and record["job_label"]:
            description_parts.append(f"<p>job label: {record['job_label']}</p>")

        if record["state"] == "COMPLETE":
            description_parts.append(
                f'<p>state:  <span style="color: green;">{record["state"]}</span></p>'
            )
        else:
            description_parts.append(
                f'<p>state:  <span style="color: red;">{record["state"]}</span></p>'
            )

        description = f"<![CDATA[{''.join([part for part in description_parts])}"

        transformed_item = {
            "record": record,
            "title": title,
            "description": description,
        }
        record_list[pos] = transformed_item
    return record_list


@router.get("/rss/user/{username}")
def user_rss(request: Request, username: str):
    """Generate RSS feed for a specific user based on their entries."""

    record_list, _ = get_records(user=username, state="end_states", all=True)

    if not record_list:
        raise HTTPException(
            status_code=404, detail=f"No entries available for {username}"
        )

    # RSS channel pub date is the most recent entry
    rss_channel_pub_date = record_list[0]["creation_time"]

    record_information = create_rss_information(record_list)

    return templates.TemplateResponse(
        "rss_feed.html",
        context={
            "request": request,
            "type": "user",
            "user_input": username,
            "entries": record_information,
            "rss_channel_pub_date": rss_channel_pub_date,
        },
        media_type="application/rss+xml",
    )


@router.get("/rss/group/{group}")
def group_rss(request: Request, group: str):
    """Generate RSS feed for a specific group work space based on their entries."""

    record_list, _ = get_records(group=group, state="end_states", all=True)

    if not record_list:
        raise HTTPException(status_code=404, detail=f"No entries available for {group}")

    # RSS channel pub date is the most recent entry
    rss_channel_pub_date = record_list[0]["creation_time"]

    record_information = create_rss_information(record_list)

    return templates.TemplateResponse(
        "rss_feed.html",
        context={
            "request": request,
            "type": "group",
            "user_input": group,
            "entries": record_information,
            "rss_channel_pub_date": rss_channel_pub_date,
        },
        media_type="application/rss+xml",
    )


# authentication required (needs to be figured out)

# nlds-up nlds-api
# http://127.0.0.1:8000/info/
