import pytest
import requests
import re
from nlds.utils.get_quotas import Quotas


# Create an instance of Quotas
@pytest.fixture
def quotas():
    return Quotas()


# Consts needed in the tests
user_services_url= "https://example.com/services"
url = f"{user_services_url}?name=test_service"


def test_get_projects_services_success(monkeypatch, quotas):
    """Test a successful instance of get_projects_services."""

    def mock_load_config():
        """Mock the load_config function to make it return the test config."""
        return {
            'authentication': {
                'authenticator':{
                    'user_services_url': user_services_url
                }
            }
        }
    monkeypatch.setattr('nlds.utils.get_quotas.load_config', mock_load_config)
    
    def mock_construct_url(*args, **kwargs):
        """Mock the construct_url function to make it return the test url."""
        return f'https://example.com/services?name=test_service'
    monkeypatch.setattr('nlds.utils.get_quotas.construct_url', mock_construct_url)

    class MockResponse:
        """Mock the response to return a 200 status code and the test text."""
        status_code = 200
        text = '{"key": "value"}'

        def json(self):
            return {"key": "value"}
        
    def mock_get(*args, **kwargs):
        """Mock the get function to give the MockResponse."""
        return MockResponse()
    monkeypatch.setattr(requests, 'get', mock_get)

    #  Call the get_projects_services function with the mocked functions
    result = quotas.get_projects_services('dummy_oauth_token', 'test_service')

    # It should succeed and give the {"key":"value"} dict.
    assert result == {"key": "value"}


def test_get_projects_services_connection_error(monkeypatch, quotas):
    """Test an unsuccessful instance of get_projects_services"""

    def mock_load_config():
        """Mock the load_config function to make it return the test config."""
        return {
            'authentication': {
                'authenticator': {
                    'user_services_url': user_services_url
                }
            }
        }
    monkeypatch.setattr('nlds.utils.get_quotas.load_config', mock_load_config)
    
    def mock_construct_url(*args, **kwargs):
        """Mock the construct_url function to make it return the test url."""
        return f'https://example.com/services?name=test_service'
    monkeypatch.setattr('nlds.utils.get_quotas.construct_url', mock_construct_url)

    def mock_get(*args, **kwargs):
        """Mock the get function to give the MockResponse."""
        raise requests.exceptions.ConnectionError
    monkeypatch.setattr(requests, 'get', mock_get)

    # Check that the ConnectionError in the get triggers a RuntimeError with the right text.
    with pytest.raises(RuntimeError, match=re.escape(f"User services url {url} could not be reached.")):
        quotas.get_projects_services('dummy_oauth_token', 'test_service')


def test_extract_tape_quota_success(monkeypatch, quotas):
    """Test a succesful instance of extract_tape_quota"""

    def mock_get_projects_services(*args, **kwargs):
        """Mock the response from get_projects_services to give the response for
        a GWS with a provisioned tape requirement."""
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

    # extract_tape_quota should return the quota value of 100
    result = quotas.extract_tape_quota('dummy_oauth_token', 'test_service')
    assert result == 100


def test_extract_tape_quota_no_requirements(monkeypatch, quotas):
    """Test an unsuccesful instance of extract_tape_quota due to no requirements."""

    def mock_get_projects_services(*args, **kwargs):
        """Mock the response from get_projects_services to give the response for
        a GWS with no requirements."""
        return[{
            "category": 1,
            "requirements": []
        }]
    monkeypatch.setattr('nlds.utils.get_quotas.Quotas.get_projects_services', mock_get_projects_services)

    # A ValueError should be raised saying there's no requirements found.
    with pytest.raises(ValueError, match="Cannot find any requirements for test_service"):
        quotas.extract_tape_quota('dummy_oauth_token', 'test_service')


def test_extract_tape_quota_no_tape_resource(monkeypatch, quotas):
    """Test an unsuccessful instance of extract_tape_quota due to no tape resources."""

    def mock_get_projects_services(*args, **kwargs):
        """Mock the response from get_projects_services to give the response for
        a GWS with a requirement that isn't tape."""
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

    # A ValueError should be raised saying there's no tape resources.
    with pytest.raises(ValueError, match="No tape resources could be found for test_service"):
        quotas.extract_tape_quota('dummy_oauth_token', 'test_service')