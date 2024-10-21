import requests
import pytest
import json
import urllib
import re

from nlds.authenticators.jasmin_authenticator import JasminAuthenticator
from nlds_processors.catalog.catalog_models import Holding
from nlds.utils.construct_url import construct_url


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
                "user_profile_url": "https://mock.url/api/profile/",
                "user_services_url": "https://mock.url/api/services/",
                "user_grants_url": "https://mock.url/api/v1/users/",
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

    # Run the tests for different responses.
    @pytest.mark.parametrize(
        "user, mock_response, expected_result",
        [
            # There is a valid response for test_user so the expected result is True.
            ("test_user", MockResponse({"username": "test_user"}, 200), True),
            # There is a valid response but it's for another user so the expected result is False.
            ("test_user", MockResponse({"username": "another_user"}, 200), False),
            # The reponse is showing a 401 Unauthorized error so the expected result is False.
            ("test_user", MockResponse({"error": "Unauthorized"}, 401), False),
            # There is a 500 Internal Server Error so the expected result is False.
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
        # Get the user information from the profile URL
        mock_requests_get["https://mock.url/api/profile/"] = mock_response

        # Create an instance of the JASMIN Authenticator
        auth = JasminAuthenticator()

        # The authenticate_user method will use the monkeypatch
        is_user = auth.authenticate_user(oauth_token, "test_user")
        assert is_user == expected_result


class TestAuthenticateGroup:
    """Check whether the user is part of the group."""

    # Run the test for different responses.
    @pytest.mark.parametrize(
        "group, mock_response, expected_result, raises_exception",
        [
            # There is a valid response for test_group so the result should be True with no exception
            (
                "test_group",
                MockResponse({"group_workspaces": ["test_group"]}, 200),
                True,
                False,
            ),
            # There is a valid response but for the wrong group so the result should be False with no exception
            (
                "test_group",
                MockResponse({"group_workspaces": ["another_group"]}, 200),
                False,
                False,
            ),
            # There is a 401 unauthorized error so the result should be False with no exception
            ("test_group", MockResponse({"error": "Unauthorized"}, 401), False, False),
            # The response is valid but empty so the response should be False with an exception (key error scenario)
            ("test_group", MockResponse({}, 200), False, True),
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
        mock_requests_get["https://mock.url/api/services/"] = mock_response

        # Create an instance of the JASMIN Authenticator
        auth = JasminAuthenticator()

        # If raises_exception is true, the test is expecting the authenticate_group method to raise a RuntimeError exception.
        if raises_exception:
            with pytest.raises(RuntimeError):
                auth.authenticate_group(oauth_token, group)
        # Otherwise, the test should return the expected result.
        else:
            # The authenticate_group method will use the monkeypatch
            is_member = auth.authenticate_group(oauth_token, group)
            assert is_member == expected_result


class TestAuthenticateUserGroupRole:
    """Check the user's role in the group."""

    # Run the test for different responses
    @pytest.mark.parametrize(
        "user, group, mock_response, expected_result, raises_exception",
        [
            # The user is a manager of that group so the result should be true with no exception
            (
                "test_user",
                "test_group",
                MockResponse({"group_workspaces": ["MANAGER"]}, 200),
                True,
                False,
            ),
            #  The user is a deputy of that group so the result should be true with no exception
            (
                "test_user",
                "test_group",
                MockResponse({"group_workspaces": ["DEPUTY"]}, 200),
                True,
                False,
            ),
            # The user is just a user of that group so the result should be true with no exception
            (
                "test_user",
                "test_group",
                MockResponse({"group_workspaces": ["USER"]}, 200),
                False,
                False,
            ),
            # The user isn't authorized so the result should be false with no exception
            (
                "test_user",
                "test_group",
                MockResponse({"error": "Unauthorized"}, 401),
                False,
                False,
            ),
            # The user is a manager AND a user of the group but the result should still be true with no exception.
            (
                "test_user",
                "test_group",
                MockResponse({"group_workspaces": ["MANAGER", "USER"]}, 200),
                True,
                False,
            ),
            # There response is an internal server error so the result should be false with no exception.
            ("test_user", "test_group", MockResponse(None, 500), False, False),
            #  There is a valid response but it's empty so there should be an exception (key error scenario)
            (
                "test_user",
                "test_group",
                MockResponse({}, 200),
                None,
                True,
            ),
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
        #  Create the URL
        url = construct_url(
            ["https://mock.url/api/v1/users", user, "grants"],
            {"category": "GWS", "service": group},
        )
        # Get the response from the full_url
        mock_requests_get[url] = mock_response

        # Create an instance of JASMIN Authenticator
        auth = JasminAuthenticator()

        # If raises_exception is true, the test is expecting the authenticate_user_group_role method to raise a RuntimeError exception.
        if raises_exception:
            with pytest.raises(RuntimeError):
                auth.authenticate_user_group_role(oauth_token, user, group)
        # Otherwise, the test should return the expected result.
        else:
            # The authenticate_user_group_role method will use the monkeypatch
            has_role = auth.authenticate_user_group_role(oauth_token, user, group)
            assert has_role == expected_result

class TestUserPermissions:
    """Test the functions that assign permissions to get holdings, get files and to delete from holding."""

    @pytest.fixture()
    def mock_holding():
        return Holding(    
            label='test-label',
            user='test-user',
            group='test-group',
        )

    def test_user_has_get_holding_permission(self):
        # Leaving this for now until it's a bit more fleshed out
        pass

    def test_user_has_get_file_permission(self):
        # Leaving this for now until it's a bit more fleshed out
        pass

    @pytest.mark.parametrize("user, group, mock_is_admin, expected", [
        ("test-user", "test-group", False, True), # User owns the holding
        ("user2", "test-group", False, False), # User does not own holding and is not admin
        ("user2", "test-group", True, True), # User is admin of the group
        ("test-user", "group2", False, False), # User is owner of different holding
    ])
    def test_user_has_delete_from_holiding_permission(self, monkeypatch, user, group, mock_is_admin, expected, mock_holding):
        # Mock the authenticate_user_group_role method
        def mock_authenticate_user_group_role(user, group):
            return mock_is_admin
        
        monkeypatch.setattr(JasminAuthenticator, "authenticate_user_group_role", mock_authenticate_user_group_role)
        result = JasminAuthenticator.user_has_delete_from_holding_permission(user, group, mock_holding)
        assert result == expected



class TestGetProjectsServices:
    """Get the projects for a service from the JASMIN Projects Portal."""
    user_services_url = "https://example.com/services"
    url = f"{user_services_url}?name=test_service"

    @pytest.fixture()
    def mock_construct_url(self, *args, **kwargs):
        """Mock the construct_url function to make it return the test url."""
        return self.url

    def test_get_projects_services_success(self,monkeypatch):
        """Test a successful instance of get_projects_services."""
        
        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.load_config", mock_load_config)
        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.construct_url", self.mock_construct_url)

        class MockResponse:
            """Mock the response to return a 200 status code and the test text."""

            status_code = 200
            text = '{"key": "value"}'

            def json(self):
                return {"key": "value"}
            
        def mock_get(*args, **kwargs):
            """Mock the get function to give MockResponse."""
            return MockResponse()
        
        monkeypatch.setattr(requests, "get", mock_get)

        # Call the get_projects_services function with the mocked functions
        result = JasminAuthenticator.get_projects_services("dummy_oauth_token", "test_service")

        # It should succeed and give the {"key":"value"} dict.
        assert result == {"key":"value"}

    def test_get_projects_services_connection_error(self,monkeypatch, quotas):
        """Test an unsuccessful instance of get_projects_services due to connection error."""

        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.load_config", mock_load_config)
        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.construct_url", self.mock_construct_url)

        def mock_get(*args, **kwargs):
            """Mock the get function to give a ConnectionError."""
            raise requests.exceptions.ConnectionError
        
        monkeypatch.setattr(requests, "get", mock_get)

        # Check that the ConnectionError in the 'get' triggers a RuntimeError with the right text.
        with pytest.raises(
            RuntimeError, match=re.escape(f"User services url {self.url} could not be reached.")
        ):
            JasminAuthenticator.get_projects_services("dummy_oauth_token", "test_service")

    def test_get_projects_services_key_error(self,monkeypatch):
        """Test an unsuccessful instance of get_projects_services due to a key error."""

        def mock_load_config_key_error():
            """Mock the load_config function to make it return the test config with no user_services_key"""
            return {"authentication": {"jasmin_authenticator": {"other_url": "test.com"}}}
        
        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.load_config", mock_load_config_key_error)
        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.construct_url", self.mock_construct_url)

        def mock_get(*args, **kwargs):
            """Mock the get function to give the KeyError."""
            raise KeyError
        
        monkeypatch.setattr(requests, "get", mock_get)

        # Check that the KeyError in the 'get' triggers a RuntimeError with the right text.
        with pytest.raises(
            RuntimeError,
            match=f"Could not find 'user_services_url' key in the jasmin_authenticator section of the .server_config file.",
        ):
            JasminAuthenticator.get_projects_services("dummy_oauth_token", "test_service")

    def test_get_projects_services_json_error(self, monkeypatch):
        """Test an unsuccessful instance of get_projects_services due to a JSON error."""

        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.load_config", mock_load_config)
        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.construct_url", self.mock_construct_url)

        class MockInvalidJSONResponse:
            """Mock the response to return a 200 status code and the JSON decode error."""
            status_code = 200
            text = "invalid json"

            def json(self):
                raise json.JSONDecodeError("Expecting value", "invalid json", 0)
            
        def mock_get(*args, **kwargs):
            """Mock the 'get' function to give the JSON error."""
            return MockInvalidJSONResponse()
        
        monkeypatch.setattr(requests, "get", mock_get)

        # Check that the JSONDecodeError triggers a RuntimeError with the right text.
        with pytest.raises(
            RuntimeError,
            match=re.escape(f"Invalid JSON returned from the user services url: {self.url}"),
        ):
            JasminAuthenticator.get_projects_services("dummy_oauth_token", "test_service")

    def test_get_projects_services_404_error(self,monkeypatch):
        """Test an unsuccessful instance of get_projects_services due to a 404 error."""
        
        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.load_config", mock_load_config)
        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.construct_url", self.mock_construct_url)

        class MockResponse:
            """Mock the response to return a 401 status code and the relevant text."""
            status_code = 401
            text = "Unauthorized"

            def json(self):
                return "Unauthorized"
            
        def mock_get(*args, **kwargs):
            """Mock the get function to give the 401 error."""
            return MockResponse()
        
        monkeypatch.setattr(requests, "get", mock_get)

        # Check that the 401 error triggers a RuntimeError with the right text.
        with pytest.raises(RuntimeError, match=f"Error getting data for test_service"):
            JasminAuthenticator.get_projects_services("dummy_oauth_token", "test_service")


class TestGetTapeQuota:
    """Get the tape quota from the list of projects services."""

    def test_get_tape_quota_success(monkeypatch):
        """Test a successful instance of get_tape_quota"""

        def mock_get_projects_services(*args, **kwargs):
            """Mock the response from get_projects_services to gvie the response for
            a GWS with a provisioned tape requirement."""
            return [
                {
                    "category": 1,
                    "requirements": [
                        {"status": 50, "resource": {"short_name": "tape"}, "amount": 100}
                    ],
                }
            ]
        
        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.get_projects_services", mock_get_projects_services)

        # get_tape_quota should return the quota value of 100
        result = JasminAuthenticator.get_tape_quota("dummy_oauth_token", "test_service")
        assert result == 100

    def test_get_tape_quota_no_requirements(monkeypatch, quotas):
        """Test an unsuccessful instance of get_tape_quota due to no requirements."""

        def mock_get_projects_services(*args, **kwargs):
            """Mock the response from get_projects_services to give the response for
            a GWS with no requirements."""
            return [{"category": 1, "requirements": []}]
        
        monkeypatch.setattr("jasmin_authenticator.JasminAuthenticator.get_projects_setvices", mock_get_projects_services)

        # A ValueError should be raised saying there's no requirements found.
        with pytest.raises(ValueError, match="Cannot find any requirements for test_service"):
            JasminAuthenticator.get_tape_quota("dummy_oauth_token", "test_service")

    def test_get_tape_quota_no_tape_resource(monkeypatch):
        """Test an unsuccessful instance of get_tape_quota due to no tape resources."""

        def mock_get_projects_services(*args, **kwargs):
            """Mock the response from get_projects_services to give the response for
            a GWS with a requirement that isn't tape."""
            return [
                {
                    "category": 1,
                    "requirements": [
                        {"status": 50, "resource": {"short_name": "other"}, "amount": 100}
                    ],
                }
            ]
        
        monkeypatch.setattr(JasminAuthenticator, "get_projects_services", mock_get_projects_services)

        # A ValueError should be raised saying there's no tape resources.
        with pytest.raises(
            ValueError, match="No tape resources could be found for test_service"
        ):
            JasminAuthenticator.get_tape_quota("dummy_oauth_token", "test_service")

    def test_get_tape_quota_services_runtime_error(monkeypatch):
        """Test an unsuccessful instance of get_tape_quota due to a runtime error when
        getting services from the projects portal."""

        def mock_get_projects_services(*args, **kwargs):
            """Mock the response from get_projects_services to give a RuntimeError."""
            raise RuntimeError("Runtime error occurred.")
        
        monkeypatch.setattr(JasminAuthenticator, "get_projects_services", mock_get_projects_services)

        # A RuntimeError should be raised saying a runtime error occurred.
        with pytest.raises(
            RuntimeError,
            match="Error getting information for test_service: Runtime error occurred",
        ):
            JasminAuthenticator.get_tape_quota("dummy_oauth_token", "test_service")

    def test_get_tape_quota_services_value_error(monkeypatch):
        """Test an unsuccessful instance of get_tape_quota due to a value error
        getting services from the projects portal."""

        def mock_get_projects_services(*args, **kwargs):
            """Mock the response from get_projects_services to give a ValueError."""
            raise ValueError("Value error occurred")
        
        monkeypatch.setattr(JasminAuthenticator, "get_projects_services", mock_get_projects_services)

        # A ValueError should be raised saying a value error occurred.
        with pytest.raises(
            ValueError,
            match="Error getting information for test_service: Value error occurred",
        ):
            JasminAuthenticator.get_tape_quota("dummy_oauth_token", "test_service")

    def test_get_tape_quota_no_gws(monkeypatch):
        """Test an unsuccessful instance of get_tape_quota due to the given service
        not being a GWS."""

        def mock_get_projects_services(*args, **kwargs):
            """Mock the response from get_projects_services to give results with the wrong category (a GWS is 1)."""
            return [
                {"category": 2, "requirements": []},
                {"category": 3, "requirements": []},
            ]
        
        monkeypatch.setattr(JasminAuthenticator, "get_projects_services", mock_get_projects_services)

        # A ValueError should be raised saying it cannot find a GWS and to check the category.
        with pytest.raises(
            ValueError,
            match="Cannot find a Group Workspace with the name test_service. Check the category.",
        ):
            JasminAuthenticator.get_tape_quota("dummy_oauth_token", "test_service")

    def get_quota_zero_quota(monkeypatch):
        """Test an unsuccessful instance of get_tape_quota due to the quota being zero."""

        def mock_get_projects_services(*args, **kwargs):
            """Mock the response from get_projects_services to give a quota of 0."""
            return [
                {
                    "category": 1,
                    "requirements": [
                        {
                            "status": 50,
                            "resource": {"short_name": "tape"},
                            "amount": 0,
                        }
                    ],
                }
            ]
        
        monkeypatch.setattr(JasminAuthenticator, "get_projects_services", mock_get_projects_services)

        # A ValueError should be raised saying there was an issue getting tape quota as it was zero.
        with pytest.raises(
            ValueError, match="Issue getting tape quota for test_service. Quota is zero."
        ):
            JasminAuthenticator.get_tape_quota("dummy_oauth_token", "test_service")

    def test_get_tape_quota_no_quota(monkeypatch):
        """Test an unsuccessful instance of get_tape_quota due to there being no quota field."""

        def mock_get_projects_services(*args, **kwargs):
            """Mock the response from get_projects_services to give no 'amount field."""
            return [
                {
                    "category": 1,
                    "requirements": [
                        {
                            "status": 50,
                            "resource": {"short_name": "tape"},
                        }
                    ],
                }
            ]
        
        monkeypatch.setattr(JasminAuthenticator, "get_projects_services", mock_get_projects_services)

        # A key error should be raised saying there was an issue getting tape quota as no value field exists.
        with pytest.raises(
            KeyError,
            match="Issue getting tape quota for test_service. No 'value' field exists.",
        ):
            JasminAuthenticator.get_tape_quota("dummy_oauth_token", "test_service")

    def test_get_tape_quota_no_provisioned_resources(monkeypatch):
        """Test an unsuccessful instance of get_tape_quota due to there being no provisioned resources."""

        def mock_get_projects_services(*args, **kwargs):
            """Mock the response from get_projects_services to give no provisioned resources (status 50)."""
            return [
                {
                    "category": 1,
                    "requirements": [
                        {
                            "status": 1,
                            "resource": {"short_name": "tape"},
                        }
                    ],
                }
            ]
        
        monkeypatch,setattr(JasminAuthenticator, "get_projects_services", mock_get_projects_services)

        # A value error should be raised saying there were no provisioned requirements found and to check the status of requested resources.
        with pytest.raises(
            ValueError,
            match="No provisioned requirements found for test_service. Check the status of your requested resources.",
        ):
            JasminAuthenticator.get_tape_quota("dummy_oauth_token", "test_service")
