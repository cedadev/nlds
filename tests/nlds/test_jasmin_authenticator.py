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


@pytest.fixture
def mock_load_config(monkeypatch):
    """Mock the load_config function to return a test-specific configuration."""
    test_config = {
        "authentication": {
            "authentication_backend": "jasmin_authenticator",
            "jasmin_authenticator": {
                "user_profile_url": "https://mock.url/api/profile",
                "user_services_url": "https://mock.url/api/services",
                "user_grants_url": "https://mock.url/api/v1/users/grants",
            },
        }
    }
    monkeypatch.setattr(
        "nlds.authenticators.jasmin_authenticator.load_config", lambda: test_config
    )


@pytest.fixture
def mock_requests_get(monkeypatch):
    """Mock the requests.get method to return different responses based on the URL."""
    responses = {}

    def mock_get(url, *args, **kwargs):
        return responses[url]

    monkeypatch.setattr(requests, "get", mock_get)
    return responses


@pytest.fixture
def oauth_token():
    """Fixture for the oauth token."""
    return "mock_oauth_token"


class TestAuthenticateUser:
    """Check whether the user is a valid user."""

    @pytest.mark.parametrize(
        "user, mock_response, expected_result",
        [
            ("test_user", MockResponse({"username": "test_user"}, 200), True),
            ("test_user", MockResponse({"username": "another_user"}, 200), False),
            ("test_user", MockResponse({"error": "Unauthorized"}, 401), False),
            ("test_user", MockResponse(None, 500), False),
        ],
    )
    def test_authenticate_user(
        self,
        mock_load_config,
        mock_requests_get,
        oauth_token,
        user,
        mock_response,
        expected_result,
    ):
        """Check whether the user is a valid user."""
        mock_requests_get["https://mock.url/api/profile"] = mock_response

        # Create an instance of the JASMIN Authenticator
        auth = JasminAuthenticator()

        # The authenticate_user method will use the monkeypatch
        is_user = auth.authenticate_user(oauth_token, "test_user")
        assert is_user == expected_result


class TestAuthenticateGroup:
    """Check whether the user is part of the group."""

    @pytest.mark.parametrize(
        "group, mock_response, expected_result, raises_exception",
        [
            (
                "test_group",
                MockResponse({"group_workspaces": ["test_group"]}, 200),
                True,
                False,
            ),
            (
                "test_group",
                MockResponse({"group_workspaces": ["another_group"]}, 200),
                False,
                False,
            ),
            ("test_group", MockResponse({"error": "Unauthorized"}, 401), False, False),
            ("test_group", MockResponse({}, 200), False, True),  # Key error scenario
        ],
    )
    def test_authenticate_group(
        self,
        mock_load_config,
        mock_requests_get,
        oauth_token,
        group,
        mock_response,
        expected_result,
        raises_exception,
    ):
        """Check whether the user is part of the group."""
        mock_requests_get["https://mock.url/api/services"] = mock_response

        # Create an instance of the JASMIN Authenticator
        auth = JasminAuthenticator()

        if raises_exception:
            with pytest.raises(RuntimeError):
                auth.authenticate_group(oauth_token, group)
        else:
            # The authenticate_group method will use the monkeypatch
            is_member = auth.authenticate_group(oauth_token, group)
            assert is_member == expected_result


class TestAuthenticateUserGroupRole:
    """Check the user's role in the group."""

    @pytest.mark.parametrize(
        "user, group, mock_response, expected_result, raises_exception",
        [
            (
                "test_user",
                "test_group",
                MockResponse({"group_workspaces": ["MANAGER"]}, 200),
                True,
                False,
            ),
            (
                "test_user",
                "test_group",
                MockResponse({"group_workspaces": ["DEPUTY"]}, 200),
                True,
                False,
            ),
            (
                "test_user",
                "test_group",
                MockResponse({"group_workspaces": ["USER"]}, 200),
                False,
                False,
            ),
            (
                "test_user",
                "test_group",
                MockResponse({"error": "Unauthorized"}, 401),
                False,
                False,
            ),
            ("test_user", "test_group", MockResponse(None, 500), False, False),
            (
                "test_user",
                "test_group",
                MockResponse({}, 200),
                None,
                True,
            ),  # Key error scenario
        ],
    )
    def test_authenticate_user_group_role(
        self,
        mock_load_config,
        mock_requests_get,
        oauth_token,
        user,
        group,
        mock_response,
        expected_result,
        raises_exception,
    ):
        """Check whether the user has a manager/deputy role within the specified group."""
        mock_requests_get["https://mock.url/api/v1/users/grants"] = mock_response

        # Create an instance of JASMIN Authenticator
        auth = JasminAuthenticator()

        if raises_exception:
            with pytest.raises(RuntimeError):
                auth.authenticate_user_group_role(oauth_token, user, group)
        else:
            # The authenticate_user_group_role method will use the monkeypatch
            has_role = auth.authenticate_user_group_role(oauth_token, user, group)
            assert has_role == expected_result
