# encoding: utf-8
"""

"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2021 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from nlds.authenticators.base_authenticator import BaseAuthenticator
from nlds.server_config import load_config
from nlds.utils.format_url import format_url
from nlds_processors.catalog.catalog_models import File, Holding, Transaction
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
        self.default_quota = 0

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
        # Construct the URL
        url = format_url(
            [config["user_grants_url"], user, "grants"],
            {"category": "GWS", "service": group},
        )
        # Contact the user_grants_url to check the role of the user in the group given.
        # This checks whether the user is a manger, deputy or user and returns True if
        # if they are either a manager or deputy, otherwise it returns False.
        try:
            response = requests.get(
                url=url,
                headers=token_headers,
                timeout=JasminAuthenticator._timeout,
            )
        except requests.exceptions.ConnectionError:
            raise RuntimeError("User grants url ", url, "could not be reached.")
        except KeyError:
            raise RuntimeError(
                f"Could not find 'user_grants_url' key in the "
                f"[{self.name}] section of the .server_config file.",
                config,
            )
        if response.status_code == requests.codes.ok:  # status code 200
            try:
                response_json = json.loads(response.text)
                # is_manager is False by default and only changes if user has a manager or deputy role.
                is_manager = False
                for role in response_json:
                    if role["role"]["name"] in ["MANAGER", "DEPUTY"]:
                        is_manager = True
                return is_manager
            except KeyError:
                raise RuntimeError(
                    "The user's role was not found in the response ",
                    "from the user grants url: ",
                    url,
                )
            except json.JSONDecodeError:
                raise RuntimeError(
                    "Invalid JSON returned from the user grants url: ", url
                )
        else:
            return False
        

    @staticmethod
    def user_has_get_holding_permission(user: str, 
                                         group: str,
                                         holding: Holding) -> bool:
        """Check whether a user has permission to view this holding.
        When we implement ROLES this will be more complicated."""
        permitted = True
        #Users can view / get all holdings in their group
        #permitted &= holding.user == user
        permitted &= holding.group == group
        return permitted
    

    def user_has_get_file_permission(session, 
                                      user: str, 
                                      group: str,
                                      file: File) -> bool:
        """Check whether a user has permission to access a file.
        Later, when we implement the ROLES this function will be a lot more
        complicated!"""
        assert(session != None)
        holding = session.query(Holding).filter(
            Transaction.id == file.transaction_id,
            Holding.id == Transaction.holding_id
        ).all()
        permitted = True
        for h in holding:
            # users have get file permission if in group
            # permitted &= h.user == user
            permitted &= h.group == group

        return permitted
    
    @staticmethod
    def user_has_delete_from_holding_permission(self, user: str, 
                                                 group: str,
                                                 holding: Holding) -> bool:
        """Check whether a user has permission to delete files from this holding.
        When we implement ROLES this will be more complicated."""
        # is_admin == whether the user is an administrator of the group
        # i.e. a DEPUTY or MANAGER
        # this gives them delete permissions for all files in the group
        is_admin = self.authenticate_user_group_role(user, group)
        permitted = True
        # Currently, only users can delete files from their owned holdings
        permitted &= (holding.user == user or is_admin)
        permitted &= holding.group == group
        return permitted


    @retry(requests.ConnectTimeout, tries=5, delay=1, backoff=2)
    def get_service_information(self, service_name: str):
        """Make a call to the JASMIN Projects Portal to get the service information."""
        config = self.config[self.auth_name][self.name]
        token_headers = {
            "Content-Type": "application/x-ww-form-urlencoded",
            "cache-control": "no-cache",
            # WORK THIS OUT
            "Authorization": f"Bearer {config["client_token"]}",
        }
        # Contact the user_services_url to get the information about the services
        url = format_url([config["project_services_url"]], {"name": service_name})
        try:
            response = requests.get(
                url,
                headers=token_headers,
                timeout=JasminAuthenticator._timeout,
            )
        except requests.exceptions.ConnectionError:
            raise RuntimeError(f"User services url {url} could not be reached.")
        except KeyError:
            raise RuntimeError(f"Could not find 'user_services_url' key in the {self.name} section of the .server_config file.")
        if response.status_code == requests.codes.ok: # status code 200
            try:
                response_json = json.loads(response.text)
                return response_json
            except json.JSONDecodeError:
                raise RuntimeError(f"Invalid JSON returned from the user services url: {url}")
        else:
            raise RuntimeError(f"Error getting data for {service_name}")
        
        
    def get_tape_quota(self, service_name: str):
        """Get the service information then process it to extract the quota for the service."""
        try:
            result = self.get_service_information(service_name)
        except (RuntimeError, ValueError) as e:
            raise type(e)(f"Error getting information for {service_name}: {e}")
        
        try:
            # Filter for Group Workspace category
            group_workspace = next(
                service for service in result if service.get("category") == 1
            )
        except StopIteration:
            raise ValueError(f"Cannot find a Group workspace with the name {service_name}. Check the category.")
        
        requirements = group_workspace.get("requirements")
        if not requirements:
            raise ValueError(f"Cannot find any requirements for {service_name}.")
        
        tape_quota = next(
            (
                req.get("amount")
                for req in requirements
                if req.get("status") == 50 and req.get("resource", {}).get("short_name") == "tape"
            ),
            None,
        )

        if tape_quota is not None:
            return tape_quota
        else:
            return self.default_quota