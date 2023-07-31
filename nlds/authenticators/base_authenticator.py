# encoding: utf-8
"""

"""
__author__ = 'Neil Massey and Jack Leland'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

"""Base class used to authenticate / authorise the users, groups, collections,
   etc.
"""
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

    def authenticate_collection(self, oauth_token: str, collection: str):
        return NotImplementedError
