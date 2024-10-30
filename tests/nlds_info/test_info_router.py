import jinja2.environment
from fastapi import Request

from common import sample_record, mock_monitor
from nlds_utils import nlds_monitor

from nlds.routers import info

def clean_output(result):
    attrs = result.__dict__
    attrs.pop("background")
    attrs.pop("body")
    attrs.pop("raw_headers")
    return(attrs)

def test_get_request_success(monkeypatch):
    monitor = mock_monitor()
    record1 = sample_record()
    monitor.session.add(record1)
    monitor.session.commit()
    
    monkeypatch.setattr(nlds_monitor, "connect_to_monitor", lambda: monitor)
    
    result = info.get(Request)
    result_dict = clean_output(result)
    print("")
    print("")
    
    for key, value in result.__dict__.items():
        print(key)
        print(value)
        print("")
    
    assert result_dict["status_code"] == 200
    
    assert isinstance(result_dict["template"], jinja2.environment.Template)
    
    assert result_dict["template"].name == "info.html"


# TODO:

"""
is there actually a way to test html css and js? (search on GitHub?)
"""