import jinja2.environment
from fastapi import Request, HTTPException

from datetime import datetime, timedelta
import json
import pytest

from common import sample_record, mock_monitor
from nlds_utils import nlds_monitor
from nlds.routers import info


def make_record_list(record_list):
    record_dict_list = nlds_monitor.construct_record_dict(record_list)
    for record in record_dict_list:
        record["creation_time"] = str(record["creation_time"])
        for sub_record in record["sub_records"]:
            sub_record["last_updated"] = str(sub_record["last_updated"])
    return record_dict_list


def clean_output(result, record_list):
    result = result.__dict__
    result.pop("background")
    result.pop("body")
    result.pop("raw_headers")

    record_dict_list = make_record_list(record_list)

    return result, record_dict_list


def test_get_request_success(monkeypatch):
    monitor = mock_monitor()
    record1 = sample_record()
    monitor.session.add(record1)
    monitor.session.commit()

    monkeypatch.setattr(nlds_monitor, "connect_to_monitor", lambda: monitor)

    result = info.get(Request)
    result_dict, record_dict_list = clean_output(result, [record1])

    assert result_dict["status_code"] == 200

    assert isinstance(result_dict["template"], jinja2.environment.Template)

    assert result_dict["template"].name == "info.html"

    assert result_dict["context"]["info"] == record_dict_list


def test_post_recordID_filter(monkeypatch):
    monitor = mock_monitor()
    record1 = sample_record(user="new_user")
    record2 = sample_record(id=2)
    record3 = sample_record(id=3)
    monitor.session.add(record1)
    monitor.session.add(record2)
    monitor.session.add(record3)
    monitor.session.commit()

    monkeypatch.setattr(nlds_monitor, "connect_to_monitor", lambda: monitor)

    filter_request_data = {"user": "new_user", "recordId": 2, "order": "ascending"}
    filter_request = info.FilterRequest(**filter_request_data)

    result = info.get_filtered_records(filter_request)
    result_content = json.loads(result.body.decode("utf-8"))

    assert result.status_code == 200

    assert (
        result_content["message"] == "State of transactions for id: 2, order: ascending"
    )

    assert result_content["records"] == make_record_list([record2])


def test_post_transactionId_filter(monkeypatch):
    monitor = mock_monitor()
    record1 = sample_record(user="new_user")
    record2 = sample_record(id=2, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfca")
    record3 = sample_record(id=3, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfcd")
    monitor.session.add(record1)
    monitor.session.add(record2)
    monitor.session.add(record3)
    monitor.session.commit()

    monkeypatch.setattr(nlds_monitor, "connect_to_monitor", lambda: monitor)

    filter_request_data = {
        "user": "new_user",
        "transactionId": "17e96f14-5f7b-49de-a309-b61e39a2cfcb",
        "order": "ascending",
    }
    filter_request = info.FilterRequest(**filter_request_data)

    result = info.get_filtered_records(filter_request)
    result_content = json.loads(result.body.decode("utf-8"))

    assert result.status_code == 200

    assert (
        result_content["message"]
        == "State of transactions for transaction id: "
        "17e96f14-5f7b-49de-a309-b61e39a2cfcb, order: ascending"
    )

    assert result_content["records"] == make_record_list([record1])


def test_post_filter_success(monkeypatch):
    monitor = mock_monitor()
    record1 = sample_record(group="new_group", user="new_user")
    record2 = sample_record(
        group="new_group", id=2, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfca"
    )
    record3 = sample_record(
        user="new_user", id=3, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfcd"
    )
    monitor.session.add(record1)
    monitor.session.add(record2)
    monitor.session.add(record3)
    monitor.session.commit()

    monkeypatch.setattr(nlds_monitor, "connect_to_monitor", lambda: monitor)

    filter_request_data = {
        "user": "new_user",
        "group": "new_group",
        "order": "ascending",
    }
    filter_request = info.FilterRequest(**filter_request_data)

    result = info.get_filtered_records(filter_request)
    result_content = json.loads(result.body.decode("utf-8"))

    assert result.status_code == 200

    start_time = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    assert (
        result_content["message"]
        == f"State of transactions for user: new_user, group: new_group, from: "
        f"{start_time}, order: ascending"
    )

    assert result_content["records"] == make_record_list([record1])


def test_post_time_fail(monkeypatch):
    monitor = mock_monitor()
    record1 = sample_record(group="new_group", user="new_user")
    record2 = sample_record(
        group="new_group", id=2, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfca"
    )
    record3 = sample_record(
        user="new_user", id=3, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfcd"
    )
    monitor.session.add(record1)
    monitor.session.add(record2)
    monitor.session.add(record3)
    monitor.session.commit()

    monkeypatch.setattr(nlds_monitor, "connect_to_monitor", lambda: monitor)

    filter_request_data = {
        "startTime": str(datetime.now()),
        "endTime": str(datetime.now() - timedelta(days=5)),
        "order": "ascending",
    }
    filter_request = info.FilterRequest(**filter_request_data)

    with pytest.raises(HTTPException) as excinfo:
        result = info.get_filtered_records(filter_request)
    exception_instance = excinfo.value
    assert exception_instance.status_code == 400
    assert exception_instance.detail == "Error: Start time must be before end time."


def test_post_state_incorrect(monkeypatch):
    monitor = mock_monitor()
    record1 = sample_record(group="new_group", user="new_user")
    record2 = sample_record(
        group="new_group", id=2, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfca"
    )
    record3 = sample_record(
        user="new_user", id=3, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfcd"
    )
    monitor.session.add(record1)
    monitor.session.add(record2)
    monitor.session.add(record3)
    monitor.session.commit()

    monkeypatch.setattr(nlds_monitor, "connect_to_monitor", lambda: monitor)

    filter_request_data = {"state": "state", "order": "ascending"}
    filter_request = info.FilterRequest(**filter_request_data)

    with pytest.raises(HTTPException) as excinfo:
        result = info.get_filtered_records(filter_request)
    exception_instance = excinfo.value
    assert exception_instance.status_code == 400
    assert exception_instance.detail == "Error: Invalid state: state"


def test_post_recordState_incorrect(monkeypatch):
    monitor = mock_monitor()
    record1 = sample_record(group="new_group", user="new_user")
    record2 = sample_record(
        group="new_group", id=2, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfca"
    )
    record3 = sample_record(
        user="new_user", id=3, transaction_id="17e96f14-5f7b-49de-a309-b61e39a2cfcd"
    )
    monitor.session.add(record1)
    monitor.session.add(record2)
    monitor.session.add(record3)
    monitor.session.commit()

    monkeypatch.setattr(nlds_monitor, "connect_to_monitor", lambda: monitor)

    filter_request_data = {"recordState": "state", "order": "ascending"}
    filter_request = info.FilterRequest(**filter_request_data)

    with pytest.raises(HTTPException) as excinfo:
        result = info.get_filtered_records(filter_request)
    exception_instance = excinfo.value
    assert exception_instance.status_code == 400
    assert exception_instance.detail == "Error: Invalid state: state"


# TODO:
"""
is there actually a way to test html css and js? (search on GitHub?)


maybe test the HTML, CSS and JS
"""

# cd nlds/tests/nlds_info
# pytest test_info_router.py -s -v -vv
