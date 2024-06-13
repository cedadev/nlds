import requests
import pytest
import json

from nlds.authenticators.jasmin_authenticator import JasminAuthenticator

@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    """Remove requests.sessions.Session.request for all tests."""
    monkeypatch.delattr("requests.sessions.Session.request")

class MockResponse:
    """Custom class to mock the requests.Response object."""
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code
        self.text = json.dumps(json_data)

    def json(self):
        return self.json_data
    

def test_authenticate_user(monkeypatch):
    """Check whether the user is a valid user."""
    def mock_get(*args, **kwargs):
        return MockResponse({"username": "test_user"}, 200)
    
    # Apply the monkeypatch for requests.get to mock_get
    monkeypatch.setattr(requests, "get", mock_get)

    # Create an instance of the JASMIN Authenticator, replacing the config with our test config
    test_config = {
        'authentication' : {
            'authentication_backend': 'jasmin_authenticator',
            'jasmin_authenticator': {
                'user_profile_url': 'https://mock.url/api/profile'
            }
        }
    }
    monkeypatch.setattr('nlds.authenticators.jasmin_authenticator.load_config', lambda: test_config)
    auth = JasminAuthenticator()

    # The authenticate_user method will use the monkeypatch
    is_user = auth.authenticate_user("mock_oauth_token", "test_user")
    assert is_user == True

# def test_authenticate_group():
#     """Check whether the user is part of the group."""

# def test_authenticate_user_group_role():
#     """Check the user's role in the group."""

    # If the user belongs to the GWS and has deputy role, it should be True
    # If the user belongs to the GWS and has manager role, it should be True
    # If the user belongs to the GWS and has the user role, it should be False
    # If the user doesn't belong to the GWS, it should be False
    # If the user doesn't belong to the GWS but has deputy role in different GWS, it should be False
    # If the user doesn't belong to the GWS but has manager role in different GWS, it should be False