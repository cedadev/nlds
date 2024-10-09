# encoding: utf-8
"""

"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2021 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

"""Base class used to authenticate / authorise the users, groups, collections,
   etc.
"""
from nlds_processors.catalog.catalog_models import File, Holding
from abc import ABC


class BaseAuthenticator(ABC):

    def authenticate_token(self, oauth_token: str):
        """Validate an oauth token."""
        raise NotImplementedError

    def authenticate_user(self, oauth_token: str, user: str):
        """Validate whether the Bearer of the token is a valid user."""
        raise NotImplementedError

    def authenticate_group(self, oauth_token: str, group: str):
        """Validate whether the Bearer of the token belongs to the group."""
        return NotImplementedError

    def authenticate_user_group_role(self, oauth_token: str, user: str, group: str):
        """Validate whether the user has manager/deputy permissions in the group."""
        return NotImplementedError
    
    def user_has_get_holding_permission(self, user: str, group: str, holding: Holding) -> bool:
        """Check whether a user has permission to view this holding."""
        return NotImplementedError
    
    def user_has_get_file_permission(self, session, user: str, group: str, file: File) -> bool:
        """Check whether a user has permission to access a file."""
        return NotImplementedError
    
    def user_has_delete_from_holding_permission(self, user: str, group: str, holding: Holding) ->  bool:
        """Check whether a user has permission to delete files from this holding."""
        return NotImplementedError
    
    def get_service_information(self, oauth_token: str, service_name: str):
        """Get the information about the given service."""
        return NotImplementedError
    
    def extract_tape_quota(self, oauth_token: str, service_name: str):
        """Process the service inforrmation to return the tape quota value."""
        return NotImplementedError
