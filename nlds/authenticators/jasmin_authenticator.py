# encoding: utf-8
"""

"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2021 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from .base_authenticator import BaseAuthenticator
from ..server_config import load_config
from retry import retry
import requests
import json
import urllib.parse


class JasminAuthenticator(BaseAuthenticator):

    _timeout = 10.0

    def __init__(self):
        self.config = load_config()
        self.name = "jasmin_authenticator"
        self.auth_name = "authentication"

    @retry(requests.ConnectTimeout, tries=5, delay=1, backoff=2)
    def authenticate_token(self, oauth_token: str):
        """Make a call to the JASMIN token introspection to determine whether
        the token is a true token and whether it is valid."""
        config = self.config[self.auth_name][self.name]
        token_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "cache-control": "no-cache",
            "Authorization": f"Bearer {oauth_token}",
        }
        token_data = {
            "token": oauth_token,
        }
        # contact the oauth_token_introspect_url to check the token
        # we expect a 200 status code to be returned

        ### we need a better way of indicating server errors (such as these)
        ### and user errors
        try:
            response = requests.post(
                config["oauth_token_introspect_url"],
                data=token_data,
                headers=token_headers,
                timeout=JasminAuthenticator._timeout,
            )
        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                "Token introspection url "
                f"{config['oauth_token_introspect_url']} could not "
                "be reached."
            )
        except KeyError:
            raise RuntimeError(
                f"Could not find 'oauth_token_introspect_url' key in the "
                f"[{self.name}] section of the .server_config file."
            )

        if response.status_code == requests.codes.ok:  # status code 200
            try:
                response_json = json.loads(response.text)
                return response_json["active"]
            except KeyError:
                raise RuntimeError(
                    "The 'active' key was not found in the response from "
                    "the token introspection url: "
                    f"{config['oauth_token_introspect_url']}"
                )
            except json.JSONDecodeError:
                raise RuntimeError(
                    "Invalid JSON returned from the token introspection url: "
                    f"{config['oauth_token_introspect_url']}"
                )

        return False

    @retry(requests.ConnectTimeout, tries=5, delay=1, backoff=2)
    def authenticate_user(self, oauth_token: str, user: str):
        """Make a call to the JASMIN services for a user to determine whether
        the user with the token is a valid user."""
        config = self.config[self.auth_name][self.name]
        token_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "cache-control": "no-cache",
            "Authorization": f"Bearer {oauth_token}",
        }
        # contact the user_profile_url to check the token and check that the
        # user in the profile matches the user in the parameter
        # it really should, as the token should be tied to the user, but the
        # user could change the user name in the config or on the command line
        # we expect a 200 status code to be returned
        try:
            response = requests.get(
                config["user_profile_url"],
                headers=token_headers,
                timeout=JasminAuthenticator._timeout,
            )
        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                "User profile url "
                f"{config['user_profile_url']} could not "
                "be reached."
            )
        except KeyError:
            raise RuntimeError(
                f"Could not find 'user_profile_url' key in the "
                f"[{self.name}] section of the .server_config file."
            )
        if response.status_code == requests.codes.ok:  # status code 200
            try:
                response_json = json.loads(response.text)
                return response_json["username"] == user
            except KeyError:
                raise RuntimeError(
                    "The 'username' key was not found in the response from "
                    "the user profile url: "
                    f"{config['user_profile_url']}"
                )
            except json.JSONDecodeError:
                raise RuntimeError(
                    "Invalid JSON returned from the user profile url: "
                    f"{config['user_profile_url']}"
                )
        else:
            return False
        return user

    @retry(requests.ConnectTimeout, tries=5, delay=1, backoff=2)
    def authenticate_group(self, oauth_token: str, group: str):
        """Make a call to the JASMIN services for a user to determine whether
        the user with the token is part of the requested group."""
        config = self.config[self.auth_name][self.name]
        token_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "cache-control": "no-cache",
            "Authorization": f"Bearer {oauth_token}",
        }
        # contact the user_services_url to check the token and check that one
        # of the groups listed in the services matches the group in the
        # parameter. This means that the user is a member of the group.
        # We've done all this without using LDAP and in a completely secure
        # manner.  Hurrah!
        try:
            response = requests.get(
                config["user_services_url"],
                headers=token_headers,
                timeout=JasminAuthenticator._timeout,
            )
        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                "User services url "
                f"{config['user_services_url']} could not "
                "be reached."
            )
        except KeyError:
            raise RuntimeError(
                f"Could not find 'user_services_url' key in the "
                f"[{self.name}] section of the .server_config file."
            )
        if response.status_code == requests.codes.ok:  # status code 200
            try:
                response_json = json.loads(response.text)
                user_gws = response_json["group_workspaces"]
                return group in user_gws
            except KeyError:
                raise RuntimeError(
                    "The 'group_workspaces' key was not found in the response "
                    "from the user services url: "
                    f"{config['user_services_url']}"
                )
            except json.JSONDecodeError:
                raise RuntimeError(
                    "Invalid JSON returned from the user services url: "
                    f"{config['user_services_url']}"
                )
        else:
            return False

    @retry(requests.ConnectTimeout, tries=5, delay=1, backoff=2)
    def authenticate_user_group_role(self, oauth_token: str, user: str, group: str):
        """Make a call to the JASMIN services to determine whether the user with the
        token has a manager/deputy role within the requested group."""
        config = self.config[self.auth_name][self.name]
        token_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "cache-control": "no-cache",
            "Authorization": f"Bearer {oauth_token}",
        }
        # Parameters needed in the URL to get service information
        encoded_query_params = urllib.parse.urlencode(
            {"category": "GWS", "service": group}
        )
        relative_url = f"{user}/grants/{encoded_query_params}"
        full_url = urllib.parse.urljoin(config["user_grants_url"], relative_url)
        # Contact the user_grants_url to check the role of the user in the group given.
        # This checks whether the user is a manger, deputy or user and returns True if
        # if they are either a manager or deputy, otherwise it returns False.
        try:
            response = requests.get(
                url=full_url,
                headers=token_headers,
                timeout=JasminAuthenticator._timeout,
            )
        except requests.exceptions.ConnectionError:
            raise RuntimeError("User grants url ", full_url, "could not be reached.")
        except KeyError:
            raise RuntimeError(
                f"Could not find 'user_grants_url' key in the "
                f"[{self.name}] section of the .server_config file.",
                config,
            )
        if response.status_code == requests.codes.ok:  # status code 200
            try:
                response_json = json.loads(response.text)
                user_role = response_json["group_workspaces"]
                # is_manager is False by default and only changes if user has a manager or deputy role.
                is_manager = False
                for role in user_role:
                    if role in ["MANAGER", "DEPUTY"]:
                        is_manager = True
                return is_manager
            except KeyError:
                raise RuntimeError(
                    "The user's role was not found in the response ",
                    "from the user grants url: ",
                    full_url,
                )
            except json.JSONDecodeError:
                raise RuntimeError(
                    "Invalid JSON returned from the user grants url: ", full_url
                )
        else:
            return False
