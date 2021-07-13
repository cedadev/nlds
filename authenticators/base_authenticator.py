"""Base class used to authenticate / authorise the users, groups, collections,
   etc.
"""

class BaseAuthenticator:

    def authenticate_token(oauth_token: str):
        """Validate an oauth token."""
        raise NotImplementedError

    def authenticate_user(oauth_token: str, user: str):
        """Validate whether the Bearer of the token is a valid user."""
        raise NotImplementedError

    def authenticate_group(oauth_token: str, group: str):
        """Validate whether the Bearer of the token belongs to the group."""
        return NotImplementedError

    def authenticate_collection(oauth_token: str, collection: str):
        return NotImplementedError
