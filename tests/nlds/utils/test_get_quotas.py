import pytest
import requests
import re
from nlds.utils.get_quotas import Quotas


@pytest.fixture
def quotas():
    return Quotas()

user_services_url= "https://example.com/services"
url = f"{user_services_url}?name=test_service"

def test_get_projects_services_success(monkeypatch, quotas):
    def mock_load_config():
        return {
            'authentication': {
                'authenticator':{
                    'user_services_url': user_services_url
                }
            }
        }
    monkeypatch.setattr('nlds.utils.get_quotas.load_config', mock_load_config)
    
    def mock_construct_url(*args, **kwargs):
        return f'https://example.com/services?name=test_service'
    monkeypatch.setattr('nlds.utils.get_quotas.construct_url', mock_construct_url)

    class MockResponse:
        status_code = 200
        text = '{"key": "value"}'

        def json(self):
            return {"key": "value"}
        
    def mock_get(*args, **kwargs):
        return MockResponse()
    monkeypatch.setattr(requests, 'get', mock_get)

    result = quotas.get_projects_services('dummy_oauth_token', 'test_service')

    assert result == {"key": "value"}


def test_get_projects_services_connection_error(monkeypatch, quotas):
    def mock_load_config():
        return {
            'authentication': {
                'authenticator': {
                    'user_services_url': user_services_url
                }
            }
        }
    monkeypatch.setattr('nlds.utils.get_quotas.load_config', mock_load_config)
    
    def mock_construct_url(*args, **kwargs):
        return f'https://example.com/services?name=test_service'
    monkeypatch.setattr('nlds.utils.get_quotas.construct_url', mock_construct_url)

    def mock_get(*args, **kwargs):
        raise requests.exceptions.ConnectionError
    monkeypatch.setattr(requests, 'get', mock_get)

    with pytest.raises(RuntimeError, match=re.escape(f"User services url {url} could not be reached.")):
        quotas.get_projects_services('dummy_oauth_token', 'test_service')


def test_extract_tape_quota_success(monkeypatch, quotas):
    def mock_get_projects_services(*args, **kwargs):
        return[{
        "category": 1,
        "requirements": [
            {
                "status": 50,
                "resource": {"short_name": "tape"},
                "amount": 100
            }
        ]
        }]
    monkeypatch.setattr('nlds.utils.get_quotas.Quotas.get_projects_services', mock_get_projects_services)

    result = quotas.extract_tape_quota('dummy_oauth_token', 'test_service')

    assert result == 100


def test_extract_tape_quota_no_requirements(monkeypatch, quotas):
    def mock_get_projects_services(*args, **kwargs):
        return[{
            "category": 1,
            "requirements": []
        }]
    monkeypatch.setattr('nlds.utils.get_quotas.Quotas.get_projects_services', mock_get_projects_services)

    service_name = 'test_service'

    with pytest.raises(ValueError, match=f"Cannot find any requirements for {service_name}"):
        quotas.extract_tape_quota('dummy_oauth_token', service_name)

def test_extract_tape_quota_no_tape_resource(monkeypatch, quotas):
    def mock_get_projects_services(*args, **kwargs):
        return [{
            "category": 1,
            "requirements": [
                {
                    "status": 50,
                    "resource": {"short_name": "other"},
                    "amount": 100
                }
            ]
        }]
    monkeypatch.setattr(Quotas, 'get_projects_services', mock_get_projects_services)

    with pytest.raises(ValueError):
        quotas.extract_tape_quota('dummy_oauth_token', 'test_service')