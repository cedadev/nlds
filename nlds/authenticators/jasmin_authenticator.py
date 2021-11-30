# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from .base_authenticator import BaseAuthenticator
from ..server_config import load_config
from ..errors import ResponseError
from fastapi import status
from fastapi.exceptions import HTTPException
import requests
import json

class JasminAuthenticator(BaseAuthenticator):

    def __init__(self):
        self.config = load_config()
        self.name = "jasmin_authenticator"
        self.auth_name = "authentication"


    def authenticate_token(self, oauth_token: str):
        """Make a call to the JASMIN token introspection to determine whether
        the token is a true token and whether it is valid."""
        config = self.config[self.auth_name][self.name]
        token_headers = {
            "Content-Type" : "application/x-www-form-urlencoded",
            "cache-control": "no-cache",
            "Authorization" : f"Bearer {oauth_token}"
        }
        token_data = {
            "token" : oauth_token,
        }
        # contact the oauth_token_introspect_url to check the token
        # we expect a 200 status code to be returned

        ### we need a better way of indicating server errors (such as these)
        ### and user errors
        try:
            response = requests.post(
                config['oauth_token_introspect_url'],
                data = token_data,
                headers = token_headers
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
                return response_json['active']
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


    def authenticate_user(self, oauth_token: str, user: str):
        """Make a call to the JASMIN services for a user to determine whether
        the user with the token is a valid user."""
        config = self.config[self.auth_name][self.name]
        token_headers = {
            "Content-Type" : "application/x-www-form-urlencoded",
            "cache-control": "no-cache",
            "Authorization" : f"Bearer {oauth_token}"
        }
        # contact the user_profile_url to check the token and check that the
        # user in the profile matches the user in the parameter
        # it really should, as the token should be tied to the user, but the
        # user could change the user name in the config or on the command line
        # we expect a 200 status code to be returned
        try:
            response = requests.get(
                config['user_profile_url'],
                headers = token_headers
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
                return response_json['username'] == user
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


    def authenticate_group(self, oauth_token: str, group: str):
        """Make a call to the JASMIN services for a user to determine whether
        the user with the token is part of the requested group."""
        config = self.config[self.auth_name][self.name]
        token_headers = {
            "Content-Type" : "application/x-www-form-urlencoded",
            "cache-control": "no-cache",
            "Authorization" : f"Bearer {oauth_token}"
        }
        # contact the user_services_url to check the token and check that one
        # of the groups listed in the services matches the group in the
        # parameter. This means that the user is a member of the group.
        # We've done all this without using LDAP and in a completely secure
        # manner.  Hurrah!
        try:
            response = requests.get(
                config['user_services_url'],
                headers = token_headers
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
                user_gws = response_json['group_workspaces']
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
        return group


    def authenticate_collection(self, oauth_token: str, collection: str):
        return NotImplementedError
